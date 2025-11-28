# price_history_service.py - Persistent 30 Traded Days Price History

import sqlite3
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class PriceHistoryService:
    """
    Manage persistent 30 traded days stock price history stored in Railway mounted volume.
    Each day's closing prices are stored, maintaining exactly 30 rows per symbol.
    """
    
    MAX_TRADED_DAYS = 30  # Keep exactly 30 traded days of data
    
    def __init__(self, db_service):
        """Initialize with database service"""
        self.db_service = db_service
        self.history_db_path = self._get_history_db_path()
        self._init_history_tables()
    
    def _get_history_db_path(self):
        """
        Get path to persistent price history database.
        Uses Railway mounted volume if available, otherwise local.
        """
        volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
        
        if volume_path:
            history_path = os.path.join(volume_path, 'price_history.db')
            logger.info(f"Using Railway persistent volume for price history: {history_path}")
        else:
            history_path = os.environ.get('PRICE_HISTORY_DB_PATH', 'price_history.db')
            logger.info(f"Using local price history database: {history_path}")
        
        # Ensure directory exists
        history_dir = os.path.dirname(history_path)
        if history_dir and not os.path.exists(history_dir):
            os.makedirs(history_dir, exist_ok=True)
            logger.info(f"Created price history directory: {history_dir}")
        
        return history_path
    
    def _get_history_connection(self):
        """Get connection to persistent history database"""
        conn = sqlite3.connect(self.history_db_path)
        conn.execute('PRAGMA foreign_keys = ON')
        # Disable WAL mode to ensure immediate writes
        conn.execute('PRAGMA journal_mode = DELETE')
        # Enable synchronous mode for data safety
        conn.execute('PRAGMA synchronous = FULL')
        return conn
    
    def _init_history_tables(self):
        """Initialize price history tables in persistent database"""
        try:
            conn = self._get_history_connection()
            cursor = conn.cursor()
            
            # Main price history table (30 traded days per symbol)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume INTEGER,
                    turnover REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                )
            ''')
            
            # Metadata table to track history state per symbol
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_history_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE,
                    last_updated DATE,
                    record_count INTEGER DEFAULT 0,
                    first_date DATE,
                    last_date DATE,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_symbol_date ON stock_price_history(symbol, date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_symbol ON stock_price_history(symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_date ON stock_price_history(date)')
            
            conn.commit()
            conn.close()
            
            logger.info(f"Price history tables initialized in persistent database (keeping {self.MAX_TRADED_DAYS} traded days)")
            
        except Exception as e:
            logger.error(f"Failed to initialize price history tables: {e}")
            raise
    
    def save_daily_prices(self, stocks_data):
        """
        Save today's stock prices as closing prices for the day.
        Maintains exactly 30 traded days of history per symbol.
        
        Args:
            stocks_data: List of stock data from PriceService.get_all_stocks()
        
        Returns:
            dict with save results
        """
        if not stocks_data:
            logger.warning("No stock data provided for daily price save")
            return {'success': False, 'saved': 0, 'error': 'No data provided'}
        
        try:
            conn = self._get_history_connection()
            cursor = conn.cursor()
            
            today = datetime.now().date()
            saved_count = 0
            skipped_count = 0
            rotated_count = 0
            
            for stock in stocks_data:
                try:
                    symbol = stock.get('symbol', '').strip().upper()
                    
                    if not symbol:
                        skipped_count += 1
                        continue
                    
                    # Use latest price data as closing price
                    close_price = stock.get('ltp', 0)
                    
                    if not close_price or close_price <= 0:
                        logger.debug(f"Skipping {symbol} - invalid price")
                        skipped_count += 1
                        continue
                    
                    # Check if data already exists for today
                    cursor.execute(
                        'SELECT id FROM stock_price_history WHERE symbol = ? AND date = ?',
                        (symbol, today)
                    )
                    
                    if cursor.fetchone():
                        logger.debug(f"Price data for {symbol} already exists for {today}")
                        skipped_count += 1
                        continue
                    
                    # Insert new price record
                    cursor.execute('''
                        INSERT INTO stock_price_history 
                        (symbol, date, open_price, high_price, low_price, close_price, volume, turnover)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        symbol,
                        today,
                        stock.get('open_price', close_price),
                        stock.get('high', close_price),
                        stock.get('low', close_price),
                        close_price,
                        stock.get('qty', 0),
                        stock.get('turnover', 0)
                    ))
                    
                    saved_count += 1
                    
                    # Rotate data for this symbol (keep only 30 traded days)
                    rotated = self._rotate_symbol_data(cursor, symbol)
                    rotated_count += rotated
                    
                except Exception as e:
                    logger.debug(f"Error saving price for {symbol}: {e}")
                    skipped_count += 1
                    continue
            
            # Update metadata for all modified symbols
            self._update_metadata(cursor, today)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Daily prices saved: {saved_count} stocks, {skipped_count} skipped, {rotated_count} old records removed")
            
            return {
                'success': True,
                'saved': saved_count,
                'skipped': skipped_count,
                'rotated': rotated_count,
                'date': today.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to save daily prices: {e}")
            return {'success': False, 'saved': 0, 'error': str(e)}
    
    def _rotate_symbol_data(self, cursor, symbol):
        """
        Rotate data for a specific symbol: keep only the 30 most recent traded days.
        
        Args:
            cursor: Database cursor
            symbol: Stock symbol
        
        Returns:
            Number of old records deleted
        """
        try:
            # Count current records for this symbol
            cursor.execute(
                'SELECT COUNT(*) FROM stock_price_history WHERE symbol = ?',
                (symbol,)
            )
            record_count = cursor.fetchone()[0]
            
            # If we have more than MAX_TRADED_DAYS, delete oldest records
            if record_count > self.MAX_TRADED_DAYS:
                excess_count = record_count - self.MAX_TRADED_DAYS
                
                # Delete the oldest records
                cursor.execute(f'''
                    DELETE FROM stock_price_history 
                    WHERE symbol = ? AND id IN (
                        SELECT id FROM stock_price_history 
                        WHERE symbol = ? 
                        ORDER BY date ASC 
                        LIMIT ?
                    )
                ''', (symbol, symbol, excess_count))
                
                deleted = cursor.rowcount
                logger.debug(f"Rotated {deleted} old records for {symbol} (kept {self.MAX_TRADED_DAYS} traded days)")
                return deleted
            
            return 0
        
        except Exception as e:
            logger.error(f"Error rotating data for {symbol}: {e}")
            return 0
    
    def _update_metadata(self, cursor, today):
        """
        Update metadata for all symbols with new data.
        
        Args:
            cursor: Database cursor
            today: Today's date
        """
        try:
            # Get all symbols with data
            cursor.execute('SELECT DISTINCT symbol FROM stock_price_history')
            symbols = [row[0] for row in cursor.fetchall()]
            
            for symbol in symbols:
                # Count records for this symbol
                cursor.execute(
                    'SELECT COUNT(*) FROM stock_price_history WHERE symbol = ?',
                    (symbol,)
                )
                record_count = cursor.fetchone()[0]
                
                # Get date range
                cursor.execute(
                    'SELECT MIN(date), MAX(date) FROM stock_price_history WHERE symbol = ?',
                    (symbol,)
                )
                min_date, max_date = cursor.fetchone()
                
                # Update or insert metadata
                cursor.execute(
                    'SELECT id FROM price_history_metadata WHERE symbol = ?',
                    (symbol,)
                )
                
                if cursor.fetchone():
                    cursor.execute('''
                        UPDATE price_history_metadata 
                        SET last_updated = ?, record_count = ?, first_date = ?, last_date = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE symbol = ?
                    ''', (today, record_count, min_date, max_date, symbol))
                else:
                    cursor.execute('''
                        INSERT INTO price_history_metadata 
                        (symbol, last_updated, record_count, first_date, last_date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (symbol, today, record_count, min_date, max_date))
        
        except Exception as e:
            logger.error(f"Error updating metadata: {e}")
    
    def get_price_history(self, symbol, days=30):
        """
        Get price history for a symbol (up to 30 traded days).
        
        Args:
            symbol: Stock symbol
            days: Number of days to retrieve (default 30)
        
        Returns:
            List of price history records, ordered by date ascending
        """
        try:
            conn = self._get_history_connection()
            cursor = conn.cursor()
            
            # Ensure days doesn't exceed MAX_TRADED_DAYS
            if days > self.MAX_TRADED_DAYS:
                days = self.MAX_TRADED_DAYS
            
            cursor.execute('''
                SELECT date, open_price, high_price, low_price, close_price, volume, turnover
                FROM stock_price_history
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT ?
            ''', (symbol.upper(), days))
            
            history = []
            rows = cursor.fetchall()
            
            # Reverse to get ascending date order
            for row in reversed(rows):
                history.append({
                    'date': row[0],
                    'open': row[1],
                    'high': row[2],
                    'low': row[3],
                    'close': row[4],
                    'volume': row[5],
                    'turnover': row[6]
                })
            
            conn.close()
            return history
            
        except Exception as e:
            logger.error(f"Failed to get price history for {symbol}: {e}")
            return []
    
    def get_price_history_stats(self, symbol):
        """
        Get statistics about price history for a symbol.
        
        Returns:
            dict with record count, date range, etc.
        """
        try:
            conn = self._get_history_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT record_count, first_date, last_date, last_updated FROM price_history_metadata WHERE symbol = ?',
                (symbol.upper(),)
            )
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'symbol': symbol.upper(),
                    'records': row[0],
                    'first_date': row[1],
                    'last_date': row[2],
                    'last_updated': row[3],
                    'max_records': self.MAX_TRADED_DAYS
                }
            else:
                return {
                    'symbol': symbol.upper(),
                    'records': 0,
                    'message': 'No history data available',
                    'max_records': self.MAX_TRADED_DAYS
                }
        
        except Exception as e:
            logger.error(f"Failed to get history stats for {symbol}: {e}")
            return {'error': str(e)}
    
    def get_all_symbols_stats(self):
        """
        Get statistics about all symbols in price history.
        
        Returns:
            List of stats for all symbols with history data
        """
        try:
            conn = self._get_history_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT symbol, record_count, first_date, last_date, last_updated
                FROM price_history_metadata
                ORDER BY last_updated DESC
            ''')
            
            stats = []
            for row in cursor.fetchall():
                stats.append({
                    'symbol': row[0],
                    'records': row[1],
                    'first_date': row[2],
                    'last_date': row[3],
                    'last_updated': row[4]
                })
            
            conn.close()
            return stats
        
        except Exception as e:
            logger.error(f"Failed to get all symbols stats: {e}")
            return []
    
    def get_history_database_info(self):
        """Get information about the price history database"""
        try:
            db_exists = os.path.exists(self.history_db_path)
            db_size = os.path.getsize(self.history_db_path) if db_exists else 0
            
            if db_exists:
                conn = self._get_history_connection()
                cursor = conn.cursor()
                
                cursor.execute('SELECT COUNT(*) FROM stock_price_history')
                total_records = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM price_history_metadata')
                total_symbols = cursor.fetchone()[0]
                
                conn.close()
            else:
                total_records = 0
                total_symbols = 0
            
            volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
            
            return {
                'path': self.history_db_path,
                'exists': db_exists,
                'size_mb': round(db_size / (1024 * 1024), 2),
                'persistent': bool(volume_path),
                'volume_mount': volume_path or 'None (local)',
                'total_records': total_records,
                'total_symbols': total_symbols,
                'max_records_per_symbol': self.MAX_TRADED_DAYS,
                'description': f'Persistent {self.MAX_TRADED_DAYS} traded days rolling price history'
            }
        
        except Exception as e:
            logger.error(f"Failed to get history database info: {e}")
            return {'error': str(e)}
    
    def cleanup_invalid_records(self):
        """
        Clean up any invalid or duplicate records.
        Should be run periodically for maintenance.
        """
        try:
            conn = self._get_history_connection()
            cursor = conn.cursor()
            
            # Remove records with invalid prices
            cursor.execute('DELETE FROM stock_price_history WHERE close_price <= 0 OR close_price > 10000')
            invalid_deleted = cursor.rowcount
            
            # Remove duplicate dates for same symbol (keep latest)
            cursor.execute('''
                DELETE FROM stock_price_history 
                WHERE id NOT IN (
                    SELECT MAX(id) FROM stock_price_history GROUP BY symbol, date
                )
            ''')
            duplicate_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Cleanup completed: {invalid_deleted} invalid records, {duplicate_deleted} duplicates removed")
            
            return {
                'invalid_deleted': invalid_deleted,
                'duplicates_deleted': duplicate_deleted
            }
        
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return {'error': str(e)}
    
    def get_traded_days_count(self):
        """Get the maximum number of traded days stored per symbol"""
        return self.MAX_TRADED_DAYS