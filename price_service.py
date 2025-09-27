# price_service.py - Fixed version with MySQL/SQLite support

import sqlite3
import logging
from datetime import datetime, timedelta, time, timezone

logger = logging.getLogger(__name__)

class PriceService:
    """Handle all stock price data operations and market information"""
    
    def __init__(self, db_service):
        """Initialize with DatabaseService instead of just db_path"""
        self.db_service = db_service
        self.db_type = db_service.db_type
        self.market_hours = MarketHours()
        self._init_price_tables()
    
    def _get_connection(self):
        """Get database connection using the database service"""
        return self.db_service.get_connection()
    
    def _init_price_tables(self):
        """Initialize price-related database tables"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if self.db_type == 'sqlite':
            # SQLite version
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    ltp REAL,
                    change_val REAL,
                    change_percent REAL,
                    high REAL,
                    low REAL,
                    open_price REAL,
                    prev_close REAL,
                    qty INTEGER,
                    turnover REAL,
                    trades INTEGER DEFAULT 0,
                    source TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_latest BOOLEAN DEFAULT TRUE
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    total_turnover REAL,
                    total_trades INTEGER,
                    total_scrips INTEGER,
                    advancing INTEGER DEFAULT 0,
                    declining INTEGER DEFAULT 0,
                    unchanged INTEGER DEFAULT 0,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_latest BOOLEAN DEFAULT TRUE
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date DATE,
                    open_price REAL,
                    high REAL,
                    low REAL,
                    close_price REAL,
                    volume INTEGER,
                    turnover REAL,
                    UNIQUE(symbol, date)
                )
            ''')
            
            # Create indexes for SQLite
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_symbol_latest ON stocks(symbol, is_latest)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_timestamp ON stocks(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_symbol_date ON price_history(symbol, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_market_summary_latest ON market_summary(is_latest)')
            
        else:  # MySQL
            # MySQL version
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    company_name VARCHAR(200),
                    ltp DECIMAL(10,2),
                    change_val DECIMAL(10,2),
                    change_percent DECIMAL(8,4),
                    high DECIMAL(10,2),
                    low DECIMAL(10,2),
                    open_price DECIMAL(10,2),
                    prev_close DECIMAL(10,2),
                    qty INT,
                    turnover DECIMAL(15,2),
                    trades INT DEFAULT 0,
                    source VARCHAR(100),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_latest BOOLEAN DEFAULT TRUE,
                    INDEX idx_symbol_latest (symbol, is_latest),
                    INDEX idx_timestamp (timestamp)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    total_turnover DECIMAL(18,2),
                    total_trades INT,
                    total_scrips INT,
                    advancing INT DEFAULT 0,
                    declining INT DEFAULT 0,
                    unchanged INT DEFAULT 0,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_latest BOOLEAN DEFAULT TRUE,
                    INDEX idx_latest (is_latest)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE,
                    open_price DECIMAL(10,2),
                    high DECIMAL(10,2),
                    low DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume INT,
                    turnover DECIMAL(15,2),
                    UNIQUE KEY unique_symbol_date (symbol, date),
                    INDEX idx_symbol_date (symbol, date)
                )
            ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Price tables initialized ({self.db_type})")
    
    def save_stock_prices(self, stock_data_list, source_name):
        """Save stock price data to database"""
        if not stock_data_list:
            return 0
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Mark existing records as not latest
            cursor.execute('UPDATE stocks SET is_latest = FALSE')
            cursor.execute('UPDATE market_summary SET is_latest = FALSE')
            
            saved_count = 0
            advancing = declining = unchanged = 0
            total_turnover = total_trades = 0
            
            for stock_data in stock_data_list:
                try:
                    if not self._validate_stock_data(stock_data):
                        continue
                    
                    # Save stock data with database-specific queries
                    if self.db_type == 'sqlite':
                        cursor.execute('''
                            INSERT INTO stocks 
                            (symbol, company_name, ltp, change_val, change_percent, high, low, 
                             open_price, prev_close, qty, turnover, trades, source, timestamp, is_latest)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                        ''', (
                            stock_data['symbol'][:10],
                            stock_data.get('company_name', stock_data['symbol'])[:100],
                            stock_data['ltp'],
                            stock_data.get('change', 0),
                            stock_data.get('change_percent', 0),
                            stock_data.get('high', stock_data['ltp']),
                            stock_data.get('low', stock_data['ltp']),
                            stock_data.get('open_price', stock_data['ltp']),
                            stock_data.get('prev_close', stock_data['ltp']),
                            stock_data.get('qty', 0),
                            stock_data.get('turnover', 0),
                            stock_data.get('trades', 0),
                            source_name,
                            datetime.now()
                        ))
                    else:  # MySQL
                        cursor.execute('''
                            INSERT INTO stocks 
                            (symbol, company_name, ltp, change_val, change_percent, high, low, 
                             open_price, prev_close, qty, turnover, trades, source, timestamp, is_latest)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                        ''', (
                            stock_data['symbol'][:10],
                            stock_data.get('company_name', stock_data['symbol'])[:200],
                            stock_data['ltp'],
                            stock_data.get('change', 0),
                            stock_data.get('change_percent', 0),
                            stock_data.get('high', stock_data['ltp']),
                            stock_data.get('low', stock_data['ltp']),
                            stock_data.get('open_price', stock_data['ltp']),
                            stock_data.get('prev_close', stock_data['ltp']),
                            stock_data.get('qty', 0),
                            stock_data.get('turnover', 0),
                            stock_data.get('trades', 0),
                            source_name,
                            datetime.now()
                        ))
                    
                    # Update market statistics
                    change = stock_data.get('change', 0)
                    if change > 0:
                        advancing += 1
                    elif change < 0:
                        declining += 1
                    else:
                        unchanged += 1
                    
                    total_turnover += stock_data.get('turnover', 0)
                    total_trades += stock_data.get('trades', 0)
                    saved_count += 1
                    
                except Exception as e:
                    logger.debug(f"Error saving stock {stock_data.get('symbol', 'unknown')}: {e}")
                    continue
            
            # Save market summary
            if saved_count > 0:
                if self.db_type == 'sqlite':
                    cursor.execute('''
                        INSERT INTO market_summary 
                        (total_turnover, total_trades, total_scrips, advancing, declining, unchanged, is_latest)
                        VALUES (?, ?, ?, ?, ?, ?, TRUE)
                    ''', (total_turnover, total_trades, saved_count, advancing, declining, unchanged))
                else:  # MySQL
                    cursor.execute('''
                        INSERT INTO market_summary 
                        (total_turnover, total_trades, total_scrips, advancing, declining, unchanged, is_latest)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                    ''', (total_turnover, total_trades, saved_count, advancing, declining, unchanged))
            
            conn.commit()
            logger.info(f"Saved {saved_count}/{len(stock_data_list)} stocks from {source_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving stock data: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def _validate_stock_data(self, stock_data):
        """Validate stock data before saving"""
        if not stock_data.get('symbol') or not stock_data.get('ltp'):
            return False
        
        ltp = float(stock_data['ltp'])
        if ltp <= 0 or ltp > 10000:  # Reasonable bounds
            return False
        
        symbol = str(stock_data['symbol']).strip().upper()
        if len(symbol) < 2 or len(symbol) > 10:
            return False
        
        return True
    
    def get_all_stocks(self):
        """Get all latest stock data"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT symbol, company_name, ltp, change_val, change_percent, 
                       high, low, open_price, prev_close, qty, turnover, 
                       trades, source, timestamp
                FROM stocks 
                WHERE is_latest = TRUE
                ORDER BY symbol
            ''')
            
            columns = ['symbol', 'company_name', 'ltp', 'change', 'change_percent', 
                      'high', 'low', 'open_price', 'prev_close', 'qty', 'turnover', 
                      'trades', 'source', 'timestamp']
            
            stocks = []
            for row in cursor.fetchall():
                stocks.append(dict(zip(columns, row)))
            
            return stocks
        finally:
            conn.close()
    
    def get_stock_by_symbol(self, symbol):
        """Get specific stock data by symbol"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent, 
                           high, low, open_price, prev_close, qty, turnover, 
                           trades, source, timestamp
                    FROM stocks 
                    WHERE symbol = ? AND is_latest = TRUE 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                ''', (symbol.upper(),))
            else:  # MySQL
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent, 
                           high, low, open_price, prev_close, qty, turnover, 
                           trades, source, timestamp
                    FROM stocks 
                    WHERE symbol = %s AND is_latest = TRUE 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                ''', (symbol.upper(),))
            
            row = cursor.fetchone()
            if row:
                columns = ['symbol', 'company_name', 'ltp', 'change', 'change_percent', 
                          'high', 'low', 'open_price', 'prev_close', 'qty', 'turnover', 
                          'trades', 'source', 'timestamp']
                return dict(zip(columns, row))
            return None
        finally:
            conn.close()
    
    def get_top_gainers(self, limit=10):
        """Get top gaining stocks"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent
                    FROM stocks 
                    WHERE is_latest = TRUE AND change_val > 0
                    ORDER BY change_percent DESC 
                    LIMIT ?
                ''', (limit,))
            else:  # MySQL
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent
                    FROM stocks 
                    WHERE is_latest = TRUE AND change_val > 0
                    ORDER BY change_percent DESC 
                    LIMIT %s
                ''', (limit,))
            
            gainers = []
            for row in cursor.fetchall():
                gainers.append({
                    'symbol': row[0],
                    'company_name': row[1],
                    'ltp': row[2],
                    'change': row[3],
                    'change_percent': row[4]
                })
            return gainers
        finally:
            conn.close()
    
    def get_top_losers(self, limit=10):
        """Get top losing stocks"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent
                    FROM stocks 
                    WHERE is_latest = TRUE AND change_val < 0
                    ORDER BY change_percent ASC 
                    LIMIT ?
                ''', (limit,))
            else:  # MySQL
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent
                    FROM stocks 
                    WHERE is_latest = TRUE AND change_val < 0
                    ORDER BY change_percent ASC 
                    LIMIT %s
                ''', (limit,))
            
            losers = []
            for row in cursor.fetchall():
                losers.append({
                    'symbol': row[0],
                    'company_name': row[1],
                    'ltp': row[2],
                    'change': row[3],
                    'change_percent': row[4]
                })
            return losers
        finally:
            conn.close()
    
    def get_most_active(self, limit=10):
        """Get most actively traded stocks"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT symbol, company_name, ltp, turnover, qty
                    FROM stocks 
                    WHERE is_latest = TRUE
                    ORDER BY turnover DESC 
                    LIMIT ?
                ''', (limit,))
            else:  # MySQL
                cursor.execute('''
                    SELECT symbol, company_name, ltp, turnover, qty
                    FROM stocks 
                    WHERE is_latest = TRUE
                    ORDER BY turnover DESC 
                    LIMIT %s
                ''', (limit,))
            
            active = []
            for row in cursor.fetchall():
                active.append({
                    'symbol': row[0],
                    'company_name': row[1],
                    'ltp': row[2],
                    'turnover': row[3],
                    'qty': row[4]
                })
            return active
        finally:
            conn.close()
    
    def get_market_summary(self):
        """Get market summary statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT total_turnover, total_trades, total_scrips, 
                       advancing, declining, unchanged, timestamp
                FROM market_summary 
                WHERE is_latest = TRUE 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''')
            
            row = cursor.fetchone()
            if row:
                return {
                    'total_turnover': row[0],
                    'total_trades': row[1],
                    'total_scrips': row[2],
                    'advancing': row[3],
                    'declining': row[4],
                    'unchanged': row[5],
                    'timestamp': row[6]
                }
            
            # Fallback calculation if no summary exists
            return self._calculate_market_summary(cursor)
        finally:
            conn.close()
    
    def _calculate_market_summary(self, cursor):
        """Calculate market summary from current data"""
        cursor.execute('''
            SELECT 
                COUNT(*) as total_scrips,
                SUM(turnover) as total_turnover,
                SUM(trades) as total_trades,
                SUM(CASE WHEN change_val > 0 THEN 1 ELSE 0 END) as advancing,
                SUM(CASE WHEN change_val < 0 THEN 1 ELSE 0 END) as declining,
                SUM(CASE WHEN change_val = 0 THEN 1 ELSE 0 END) as unchanged
            FROM stocks 
            WHERE is_latest = TRUE
        ''')
        
        row = cursor.fetchone()
        if row:
            return {
                'total_scrips': row[0] or 0,
                'total_turnover': row[1] or 0,
                'total_trades': row[2] or 0,
                'advancing': row[3] or 0,
                'declining': row[4] or 0,
                'unchanged': row[5] or 0,
                'timestamp': datetime.now().isoformat()
            }
        
        return {
            'total_scrips': 0, 'total_turnover': 0, 'total_trades': 0,
            'advancing': 0, 'declining': 0, 'unchanged': 0,
            'timestamp': datetime.now().isoformat()
        }
    
    def search_stocks(self, query, limit=20):
        """Search stocks by symbol or company name"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            search_term = f"%{query.upper()}%"
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent
                    FROM stocks 
                    WHERE is_latest = TRUE 
                    AND (symbol LIKE ? OR UPPER(company_name) LIKE ?)
                    ORDER BY symbol
                    LIMIT ?
                ''', (search_term, search_term, limit))
            else:  # MySQL
                cursor.execute('''
                    SELECT symbol, company_name, ltp, change_val, change_percent
                    FROM stocks 
                    WHERE is_latest = TRUE 
                    AND (symbol LIKE %s OR UPPER(company_name) LIKE %s)
                    ORDER BY symbol
                    LIMIT %s
                ''', (search_term, search_term, limit))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'symbol': row[0],
                    'company_name': row[1],
                    'ltp': row[2],
                    'change': row[3],
                    'change_percent': row[4]
                })
            return results
        finally:
            conn.close()
    
    def get_stock_count(self):
        """Get total number of stocks"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM stocks WHERE is_latest = TRUE')
            return cursor.fetchone()[0]
        except:
            return 0
        finally:
            conn.close()
    
    def get_price_history(self, symbol, days=30):
        """Get price history for a stock"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            since_date = datetime.now() - timedelta(days=days)
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT date, open_price, high, low, close_price, volume, turnover
                    FROM price_history 
                    WHERE symbol = ? AND date >= ?
                    ORDER BY date DESC
                ''', (symbol.upper(), since_date.date()))
            else:  # MySQL
                cursor.execute('''
                    SELECT date, open_price, high, low, close_price, volume, turnover
                    FROM price_history 
                    WHERE symbol = %s AND date >= %s
                    ORDER BY date DESC
                ''', (symbol.upper(), since_date.date()))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'date': row[0],
                    'open': row[1],
                    'high': row[2],
                    'low': row[3],
                    'close': row[4],
                    'volume': row[5],
                    'turnover': row[6]
                })
            return history
        finally:
            conn.close()
    
    def get_market_status(self):
        """Get current market status"""
        return self.market_hours.get_market_status()


class MarketHours:
    """Handle NEPSE market hours and trading day logic"""
    
    def __init__(self):
        # Nepal timezone (UTC+5:45)
        self.nepal_tz = timezone(timedelta(hours=5, minutes=45))
        
        # NEPSE trading hours (Sunday to Thursday, 12:00 PM to 3:00 PM)
        self.market_open_time = time(12, 0)  # 12:00 PM
        self.market_close_time = time(15, 0)  # 3:00 PM
        
        # Trading days (0=Monday, 6=Sunday)
        self.trading_days = [6, 0, 1, 2, 3]  # Sunday to Thursday
    
    def get_nepal_time(self):
        """Get current Nepal time"""
        return datetime.now(self.nepal_tz)
    
    def is_trading_day(self, dt=None):
        """Check if given date is a trading day"""
        if dt is None:
            dt = self.get_nepal_time()
        return dt.weekday() in self.trading_days
    
    def is_market_hours(self, dt=None):
        """Check if current time is within market hours"""
        if dt is None:
            dt = self.get_nepal_time()
        
        if not self.is_trading_day(dt):
            return False
        
        current_time = dt.time()
        return self.market_open_time <= current_time <= self.market_close_time
    
    def is_market_open(self, dt=None):
        """Check if market is currently open"""
        return self.is_trading_day(dt) and self.is_market_hours(dt)
    
    def get_market_status(self):
        """Get current market status"""
        now = self.get_nepal_time()
        
        if not self.is_trading_day(now):
            return {
                'status': 'closed',
                'reason': 'Not a trading day',
                'next_open': self._next_market_open().isoformat()
            }
        
        if self.is_market_hours(now):
            return {
                'status': 'open',
                'reason': 'Market is open',
                'closes_at': now.replace(hour=15, minute=0, second=0, microsecond=0).isoformat()
            }
        
        current_time = now.time()
        if current_time < self.market_open_time:
            return {
                'status': 'pre_market',
                'reason': 'Before market hours',
                'opens_at': now.replace(hour=12, minute=0, second=0, microsecond=0).isoformat()
            }
        else:
            return {
                'status': 'after_hours',
                'reason': 'After market hours',
                'next_open': self._next_market_open().isoformat()
            }
    
    def _next_market_open(self):
        """Get the next market opening time"""
        now = self.get_nepal_time()
        
        if self.is_market_open(now):
            return now
        
        today_open = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if self.is_trading_day(now) and now.time() < self.market_open_time:
            return today_open
        
        for i in range(1, 8):
            next_day = now + timedelta(days=i)
            if self.is_trading_day(next_day):
                return next_day.replace(hour=12, minute=0, second=0, microsecond=0)
        
        return today_open