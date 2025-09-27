# price_service.py - Railway-compatible version with PostgreSQL support

import os
import logging
from datetime import datetime, timedelta, time, timezone
from db_service import DatabaseService

logger = logging.getLogger(__name__)

class PriceService:
    """Handle all stock price data operations and market information"""
    
    def __init__(self):
        self.db = DatabaseService()
        self.market_hours = MarketHours()
        self.placeholder = self.db.get_placeholder()
    
    def save_stock_prices(self, stock_data_list, source_name):
        """Save stock price data to database"""
        if not stock_data_list:
            return 0
        
        try:
            # Mark existing records as not latest
            self.db.execute_query(
                'UPDATE stocks SET is_latest = FALSE'
            )
            self.db.execute_query(
                'UPDATE market_summary SET is_latest = FALSE'
            )
            
            saved_count = 0
            advancing = declining = unchanged = 0
            total_turnover = total_trades = 0
            
            # Prepare bulk insert data
            stock_insert_data = []
            
            for stock_data in stock_data_list:
                if not self._validate_stock_data(stock_data):
                    continue
                
                # Prepare stock data for insertion
                stock_params = (
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
                    True  # is_latest
                )
                stock_insert_data.append(stock_params)
                
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
            
            # Bulk insert stocks
            if stock_insert_data:
                placeholders = ','.join([self.placeholder] * 14)
                insert_query = f'''
                    INSERT INTO stocks 
                    (symbol, company_name, ltp, change, change_percent, high, low, 
                     open_price, prev_close, qty, turnover, trades, source, is_latest)
                    VALUES ({placeholders})
                '''
                self.db.execute_many(insert_query, stock_insert_data)
            
            # Save market summary
            if saved_count > 0:
                summary_query = f'''
                    INSERT INTO market_summary 
                    (total_turnover, total_trades, total_scrips, advancing, declining, unchanged, is_latest)
                    VALUES ({','.join([self.placeholder] * 7)})
                '''
                self.db.execute_query(
                    summary_query,
                    (total_turnover, total_trades, saved_count, advancing, declining, unchanged, True)
                )
            
            logger.info(f"Saved {saved_count}/{len(stock_data_list)} stocks from {source_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving stock data: {e}")
            return 0
    
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
        query = '''
            SELECT symbol, company_name, ltp, change, change_percent, 
                   high, low, open_price, prev_close, qty, turnover, 
                   trades, source, timestamp
            FROM stocks 
            WHERE is_latest = TRUE
            ORDER BY symbol
        '''
        return self.db.execute_query(query, fetch='all')
    
    def get_stock_by_symbol(self, symbol):
        """Get specific stock data by symbol"""
        query = f'''
            SELECT symbol, company_name, ltp, change, change_percent, 
                   high, low, open_price, prev_close, qty, turnover, 
                   trades, source, timestamp
            FROM stocks 
            WHERE symbol = {self.placeholder} AND is_latest = TRUE 
            ORDER BY timestamp DESC 
            LIMIT 1
        '''
        return self.db.execute_query(query, (symbol.upper(),), fetch='one')
    
    def get_top_gainers(self, limit=10):
        """Get top gaining stocks"""
        query = f'''
            SELECT symbol, company_name, ltp, change, change_percent
            FROM stocks 
            WHERE is_latest = TRUE AND change > 0
            ORDER BY change_percent DESC 
            LIMIT {self.placeholder}
        '''
        return self.db.execute_query(query, (limit,), fetch='all')
    
    def get_top_losers(self, limit=10):
        """Get top losing stocks"""
        query = f'''
            SELECT symbol, company_name, ltp, change, change_percent
            FROM stocks 
            WHERE is_latest = TRUE AND change < 0
            ORDER BY change_percent ASC 
            LIMIT {self.placeholder}
        '''
        return self.db.execute_query(query, (limit,), fetch='all')
    
    def get_most_active(self, limit=10):
        """Get most actively traded stocks"""
        query = f'''
            SELECT symbol, company_name, ltp, turnover, qty
            FROM stocks 
            WHERE is_latest = TRUE
            ORDER BY turnover DESC 
            LIMIT {self.placeholder}
        '''
        return self.db.execute_query(query, (limit,), fetch='all')
    
    def get_market_summary(self):
        """Get market summary statistics"""
        query = '''
            SELECT total_turnover, total_trades, total_scrips, 
                   advancing, declining, unchanged, timestamp
            FROM market_summary 
            WHERE is_latest = TRUE 
            ORDER BY timestamp DESC 
            LIMIT 1
        '''
        
        result = self.db.execute_query(query, fetch='one')
        if result:
            return result
        
        # Fallback calculation if no summary exists
        return self._calculate_market_summary()
    
    def _calculate_market_summary(self):
        """Calculate market summary from current data"""
        query = '''
            SELECT 
                COUNT(*) as total_scrips,
                COALESCE(SUM(turnover), 0) as total_turnover,
                COALESCE(SUM(trades), 0) as total_trades,
                SUM(CASE WHEN change > 0 THEN 1 ELSE 0 END) as advancing,
                SUM(CASE WHEN change < 0 THEN 1 ELSE 0 END) as declining,
                SUM(CASE WHEN change = 0 THEN 1 ELSE 0 END) as unchanged
            FROM stocks 
            WHERE is_latest = TRUE
        '''
        
        result = self.db.execute_query(query, fetch='one')
        if result:
            result['timestamp'] = datetime.now().isoformat()
            return result
        
        return {
            'total_scrips': 0, 'total_turnover': 0, 'total_trades': 0,
            'advancing': 0, 'declining': 0, 'unchanged': 0,
            'timestamp': datetime.now().isoformat()
        }
    
    def search_stocks(self, query_term, limit=20):
        """Search stocks by symbol or company name"""
        search_term = f"%{query_term.upper()}%"
        query = f'''
            SELECT symbol, company_name, ltp, change, change_percent
            FROM stocks 
            WHERE is_latest = TRUE 
            AND (symbol LIKE {self.placeholder} OR UPPER(company_name) LIKE {self.placeholder})
            ORDER BY symbol
            LIMIT {self.placeholder}
        '''
        return self.db.execute_query(query, (search_term, search_term, limit), fetch='all')
    
    def get_stock_count(self):
        """Get total number of stocks"""
        query = 'SELECT COUNT(*) as count FROM stocks WHERE is_latest = TRUE'
        result = self.db.execute_query(query, fetch='one')
        return result['count'] if result else 0
    
    def get_price_history(self, symbol, days=30):
        """Get price history for a stock"""
        since_date = datetime.now() - timedelta(days=days)
        query = f'''
            SELECT date, open_price, high, low, close_price, volume, turnover
            FROM price_history 
            WHERE symbol = {self.placeholder} AND date >= {self.placeholder}
            ORDER BY date DESC
        '''
        return self.db.execute_query(query, (symbol.upper(), since_date.date()), fetch='all')
    
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