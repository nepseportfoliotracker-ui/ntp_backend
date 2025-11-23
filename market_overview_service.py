# market_overview_service.py - Calculate and store market overview snapshots

import logging
from datetime import datetime, timedelta
import json
from decimal import Decimal

logger = logging.getLogger(__name__)


class MarketOverviewService:
    """Handle market overview calculations and time-series storage"""
    
    def __init__(self, db_service, price_service):
        self.db_service = db_service
        self.price_service = price_service
        self._init_overview_tables()
    
    def _init_overview_tables(self):
        """Initialize tables for market overview storage"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # Market overview snapshots - stores top gainers, losers, etc. at each interval
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_overview_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_time DATETIME NOT NULL,
                    snapshot_date DATE NOT NULL,
                    overview_data TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(snapshot_date, snapshot_time)
                )
            ''')
            
            # Top gainers - fast lookup for latest
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS top_gainers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    rank INTEGER,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    change_val REAL,
                    change_percent REAL,
                    snapshot_time DATETIME,
                    FOREIGN KEY(snapshot_id) REFERENCES market_overview_snapshots(id)
                )
            ''')
            
            # Top losers
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS top_losers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    rank INTEGER,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    change_val REAL,
                    change_percent REAL,
                    snapshot_time DATETIME,
                    FOREIGN KEY(snapshot_id) REFERENCES market_overview_snapshots(id)
                )
            ''')
            
            # Most active by quantity
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS top_active_quantity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    rank INTEGER,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    qty INTEGER,
                    turnover REAL,
                    snapshot_time DATETIME,
                    FOREIGN KEY(snapshot_id) REFERENCES market_overview_snapshots(id)
                )
            ''')
            
            # Most active by turnover
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS top_active_turnover (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    rank INTEGER,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    turnover REAL,
                    qty INTEGER,
                    snapshot_time DATETIME,
                    FOREIGN KEY(snapshot_id) REFERENCES market_overview_snapshots(id)
                )
            ''')
            
            # Daily overview summary (one per day)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_overview_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL UNIQUE,
                    summary_data TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_overview_snapshots_time ON market_overview_snapshots(snapshot_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_overview_snapshots_date ON market_overview_snapshots(snapshot_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_top_gainers_snapshot ON top_gainers(snapshot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_top_losers_snapshot ON top_losers(snapshot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_top_active_qty_snapshot ON top_active_quantity(snapshot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_top_active_turnover_snapshot ON top_active_turnover(snapshot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_overview_summary(date)')
            
            conn.commit()
            conn.close()
            logger.info("Market overview tables initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize overview tables: {e}")
    
    def calculate_market_overview(self, limit=10):
        """
        Calculate market overview from current stock data
        
        Args:
            limit: Number of top items to include in each category
            
        Returns:
            dict with top gainers, losers, active stocks
        """
        try:
            stocks = self.price_service.get_all_stocks()
            
            if not stocks:
                logger.warning("No stock data available for overview calculation")
                return None
            
            # Filter out zero changes and invalid data
            valid_stocks = [
                s for s in stocks 
                if s.get('ltp', 0) > 0 and s.get('change', 0) != 0
            ]
            
            # Calculate categories
            overview = {
                'timestamp': datetime.now().isoformat(),
                'total_stocks': len(stocks),
                'active_stocks': len(valid_stocks),
                
                'top_gainers': self._get_top_gainers(stocks, limit),
                'top_losers': self._get_top_losers(stocks, limit),
                'top_active_quantity': self._get_top_quantity(stocks, limit),
                'top_active_turnover': self._get_top_turnover(stocks, limit),
                
                'market_stats': {
                    'advancing': sum(1 for s in stocks if s.get('change', 0) > 0),
                    'declining': sum(1 for s in stocks if s.get('change', 0) < 0),
                    'unchanged': sum(1 for s in stocks if s.get('change', 0) == 0),
                    'total_turnover': sum(s.get('turnover', 0) for s in stocks),
                    'total_volume': sum(s.get('qty', 0) for s in stocks)
                }
            }
            
            return overview
            
        except Exception as e:
            logger.error(f"Error calculating market overview: {e}")
            return None
    
    def _get_top_gainers(self, stocks, limit=10):
        """Get top gaining stocks"""
        gainers = [s for s in stocks if s.get('change', 0) > 0]
        gainers.sort(key=lambda x: float(x.get('change_percent', 0)), reverse=True)
        
        return [
            {
                'rank': i + 1,
                'symbol': s['symbol'],
                'company_name': s.get('company_name', s['symbol']),
                'ltp': float(s.get('ltp', 0)),
                'change': float(s.get('change', 0)),
                'change_percent': float(s.get('change_percent', 0))
            }
            for i, s in enumerate(gainers[:limit])
        ]
    
    def _get_top_losers(self, stocks, limit=10):
        """Get top losing stocks"""
        losers = [s for s in stocks if s.get('change', 0) < 0]
        losers.sort(key=lambda x: float(x.get('change_percent', 0)))
        
        return [
            {
                'rank': i + 1,
                'symbol': s['symbol'],
                'company_name': s.get('company_name', s['symbol']),
                'ltp': float(s.get('ltp', 0)),
                'change': float(s.get('change', 0)),
                'change_percent': float(s.get('change_percent', 0))
            }
            for i, s in enumerate(losers[:limit])
        ]
    
    def _get_top_quantity(self, stocks, limit=10):
        """Get most active stocks by quantity"""
        active = sorted(
            stocks,
            key=lambda x: int(x.get('qty', 0)),
            reverse=True
        )
        
        return [
            {
                'rank': i + 1,
                'symbol': s['symbol'],
                'company_name': s.get('company_name', s['symbol']),
                'ltp': float(s.get('ltp', 0)),
                'qty': int(s.get('qty', 0)),
                'turnover': float(s.get('turnover', 0))
            }
            for i, s in enumerate(active[:limit])
        ]
    
    def _get_top_turnover(self, stocks, limit=10):
        """Get most active stocks by turnover"""
        active = sorted(
            stocks,
            key=lambda x: float(x.get('turnover', 0)),
            reverse=True
        )
        
        return [
            {
                'rank': i + 1,
                'symbol': s['symbol'],
                'company_name': s.get('company_name', s['symbol']),
                'ltp': float(s.get('ltp', 0)),
                'turnover': float(s.get('turnover', 0)),
                'qty': int(s.get('qty', 0))
            }
            for i, s in enumerate(active[:limit])
        ]
    
    def save_overview_snapshot(self, overview_data=None, limit=10):
        """
        Save market overview snapshot to database
        
        Args:
            overview_data: Pre-calculated overview dict (if None, will calculate)
            limit: Number of top items per category
        """
        try:
            if overview_data is None:
                overview_data = self.calculate_market_overview(limit)
            
            if not overview_data:
                logger.warning("No overview data to save")
                return None
            
            now = datetime.now()
            today = now.date()
            
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # Save main snapshot
            cursor.execute('''
                INSERT INTO market_overview_snapshots 
                (snapshot_time, snapshot_date, overview_data)
                VALUES (?, ?, ?)
            ''', (now.isoformat(), today.isoformat(), json.dumps(overview_data)))
            
            snapshot_id = cursor.lastrowid
            
            # Save individual categories
            self._save_top_gainers(cursor, snapshot_id, overview_data['top_gainers'], now)
            self._save_top_losers(cursor, snapshot_id, overview_data['top_losers'], now)
            self._save_top_quantity(cursor, snapshot_id, overview_data['top_active_quantity'], now)
            self._save_top_turnover(cursor, snapshot_id, overview_data['top_active_turnover'], now)
            
            conn.commit()
            
            # Update daily summary
            self._update_daily_summary(today, overview_data)
            
            conn.close()
            
            logger.info(f"Saved market overview snapshot at {now}")
            return snapshot_id
            
        except Exception as e:
            logger.error(f"Error saving overview snapshot: {e}")
            return None
    
    def _save_top_gainers(self, cursor, snapshot_id, gainers, snapshot_time):
        """Save top gainers to database"""
        for item in gainers:
            cursor.execute('''
                INSERT INTO top_gainers 
                (snapshot_id, rank, symbol, company_name, ltp, change_val, change_percent, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_id, item['rank'], item['symbol'], item['company_name'],
                item['ltp'], item['change'], item['change_percent'], snapshot_time.isoformat()
            ))
    
    def _save_top_losers(self, cursor, snapshot_id, losers, snapshot_time):
        """Save top losers to database"""
        for item in losers:
            cursor.execute('''
                INSERT INTO top_losers 
                (snapshot_id, rank, symbol, company_name, ltp, change_val, change_percent, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_id, item['rank'], item['symbol'], item['company_name'],
                item['ltp'], item['change'], item['change_percent'], snapshot_time.isoformat()
            ))
    
    def _save_top_quantity(self, cursor, snapshot_id, active, snapshot_time):
        """Save top by quantity to database"""
        for item in active:
            cursor.execute('''
                INSERT INTO top_active_quantity 
                (snapshot_id, rank, symbol, company_name, ltp, qty, turnover, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_id, item['rank'], item['symbol'], item['company_name'],
                item['ltp'], item['qty'], item['turnover'], snapshot_time.isoformat()
            ))
    
    def _save_top_turnover(self, cursor, snapshot_id, active, snapshot_time):
        """Save top by turnover to database"""
        for item in active:
            cursor.execute('''
                INSERT INTO top_active_turnover 
                (snapshot_id, rank, symbol, company_name, ltp, turnover, qty, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_id, item['rank'], item['symbol'], item['company_name'],
                item['ltp'], item['turnover'], item['qty'], snapshot_time.isoformat()
            ))
    
    def _update_daily_summary(self, date, overview_data):
        """Update or create daily summary"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            daily_summary = {
                'date': date.isoformat(),
                'market_stats': overview_data['market_stats'],
                'top_gainers': overview_data['top_gainers'][:3],
                'top_losers': overview_data['top_losers'][:3],
                'top_turnover': overview_data['top_active_turnover'][:3]
            }
            
            cursor.execute('''
                INSERT OR REPLACE INTO daily_overview_summary 
                (date, summary_data, updated_at)
                VALUES (?, ?, ?)
            ''', (date.isoformat(), json.dumps(daily_summary), datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating daily summary: {e}")
    
    def get_latest_overview(self):
        """Get the latest market overview snapshot"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, snapshot_time, overview_data 
                FROM market_overview_snapshots 
                ORDER BY snapshot_time DESC 
                LIMIT 1
            ''')
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'snapshot_id': row[0],
                    'timestamp': row[1],
                    'data': json.loads(row[2])
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching latest overview: {e}")
            return None
    
    def get_overview_by_time(self, snapshot_time):
        """Get overview snapshot at specific time"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, snapshot_time, overview_data 
                FROM market_overview_snapshots 
                WHERE snapshot_time = ?
            ''', (snapshot_time,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'snapshot_id': row[0],
                    'timestamp': row[1],
                    'data': json.loads(row[2])
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching overview by time: {e}")
            return None
    
    def get_overview_history(self, start_time=None, end_time=None, limit=50):
        """
        Get overview history within time range
        
        Args:
            start_time: Start datetime (default: last 24 hours)
            end_time: End datetime (default: now)
            limit: Max number of snapshots
        """
        try:
            if end_time is None:
                end_time = datetime.now()
            if start_time is None:
                start_time = end_time - timedelta(hours=24)
            
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, snapshot_time, overview_data 
                FROM market_overview_snapshots 
                WHERE snapshot_time BETWEEN ? AND ?
                ORDER BY snapshot_time DESC
                LIMIT ?
            ''', (start_time.isoformat(), end_time.isoformat(), limit))
            
            snapshots = []
            for row in cursor.fetchall():
                snapshots.append({
                    'snapshot_id': row[0],
                    'timestamp': row[1],
                    'data': json.loads(row[2])
                })
            
            conn.close()
            return snapshots
            
        except Exception as e:
            logger.error(f"Error fetching overview history: {e}")
            return []
    
    def get_daily_summary(self, date=None):
        """Get daily overview summary"""
        try:
            if date is None:
                date = datetime.now().date()
            
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT summary_data, created_at, updated_at
                FROM daily_overview_summary 
                WHERE date = ?
            ''', (date.isoformat(),))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'date': date.isoformat(),
                    'data': json.loads(row[0]),
                    'created_at': row[1],
                    'updated_at': row[2]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching daily summary: {e}")
            return None
    
    def cleanup_old_snapshots(self, keep_days=7):
        """Remove snapshots older than specified days"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=keep_days)).date()
            
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # Get snapshot IDs to delete
            cursor.execute('''
                SELECT id FROM market_overview_snapshots 
                WHERE snapshot_date < ?
            ''', (cutoff_date.isoformat(),))
            
            snapshot_ids = [row[0] for row in cursor.fetchall()]
            
            if snapshot_ids:
                placeholders = ','.join('?' * len(snapshot_ids))
                
                # Delete related records
                cursor.execute(f'DELETE FROM top_gainers WHERE snapshot_id IN ({placeholders})', snapshot_ids)
                cursor.execute(f'DELETE FROM top_losers WHERE snapshot_id IN ({placeholders})', snapshot_ids)
                cursor.execute(f'DELETE FROM top_active_quantity WHERE snapshot_id IN ({placeholders})', snapshot_ids)
                cursor.execute(f'DELETE FROM top_active_turnover WHERE snapshot_id IN ({placeholders})', snapshot_ids)
                
                # Delete snapshots
                cursor.execute(f'DELETE FROM market_overview_snapshots WHERE snapshot_date < ?', (cutoff_date.isoformat(),))
                
                conn.commit()
                logger.info(f"Cleaned up {len(snapshot_ids)} old overview snapshots")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error cleaning up old snapshots: {e}")