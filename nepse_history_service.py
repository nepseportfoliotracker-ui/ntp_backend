# nepse_history_service.py - NEPSE Historical Data Service

import logging
import requests
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)


class NepseHistoryService:
    """Service for managing NEPSE historical index data"""
    
    def __init__(self, db_service):
        self.db_service = db_service
        self.base_url = "https://www.nepalipaisa.com/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self._init_history_tables()
    
    def _init_history_tables(self):
        """Initialize tables for different time periods"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            # Weekly history table (last 7 days)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nepse_history_weekly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date DATE NOT NULL UNIQUE,
                    index_value REAL NOT NULL,
                    percent_change REAL,
                    difference REAL,
                    turnover REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Monthly history table (last 30 days)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nepse_history_monthly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date DATE NOT NULL UNIQUE,
                    index_value REAL NOT NULL,
                    percent_change REAL,
                    difference REAL,
                    turnover REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Yearly history table (last 365 days)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nepse_history_yearly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date DATE NOT NULL UNIQUE,
                    index_value REAL NOT NULL,
                    percent_change REAL,
                    difference REAL,
                    turnover REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Metadata table to track last update times
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nepse_history_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL UNIQUE,
                    last_update DATETIME,
                    record_count INTEGER DEFAULT 0,
                    date_range_start DATE,
                    date_range_end DATE
                )
            ''')
            
            # Create indexes for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_weekly_date ON nepse_history_weekly(trade_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_date ON nepse_history_monthly(trade_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_yearly_date ON nepse_history_yearly(trade_date DESC)')
            
            # Initialize metadata
            for table in ['weekly', 'monthly', 'yearly']:
                cursor.execute('''
                    INSERT OR IGNORE INTO nepse_history_metadata (table_name, last_update, record_count)
                    VALUES (?, NULL, 0)
                ''', (table,))
            
            conn.commit()
            logger.info("NEPSE history tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize history tables: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def fetch_history_from_api(self, start_date, end_date):
        """
        Fetch NEPSE index historical data from API
        
        Parameters:
        - start_date: Start date in 'YYYY-MM-DD' format
        - end_date: End date in 'YYYY-MM-DD' format
        
        Returns:
        - DataFrame with historical data or empty DataFrame on error
        """
        url = f"{self.base_url}/GetIndexSubIndexHistory"
        
        params = {
            'indexName': 'Nepse',
            'fromDate': start_date,
            'toDate': end_date
        }
        
        try:
            logger.info(f"Fetching NEPSE history from {start_date} to {end_date}...")
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('statusCode') == 200 and data.get('result'):
                    result = data['result']
                    
                    if isinstance(result, list) and len(result) > 0:
                        df = pd.DataFrame(result)
                        
                        # Convert date column
                        df['trade_date'] = pd.to_datetime(df['tradeDate']).dt.date
                        
                        # Convert numeric columns
                        df['index_value'] = pd.to_numeric(df.get('indexValue', 0), errors='coerce')
                        df['percent_change'] = pd.to_numeric(df.get('percentChange', 0), errors='coerce')
                        df['difference'] = pd.to_numeric(df.get('difference', 0), errors='coerce')
                        df['turnover'] = pd.to_numeric(df.get('turnover', 0), errors='coerce')
                        
                        # Select only needed columns
                        df = df[['trade_date', 'index_value', 'percent_change', 'difference', 'turnover']]
                        
                        # Sort by date descending (latest first)
                        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
                        
                        logger.info(f"Successfully fetched {len(df)} records")
                        return df
                    else:
                        logger.warning("No data in API response")
                        return pd.DataFrame()
                else:
                    logger.error(f"API error: {data.get('message', 'Unknown error')}")
                    return pd.DataFrame()
            else:
                logger.error(f"HTTP error {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching history: {e}")
            return pd.DataFrame()
    
    def save_to_table(self, df, table_name):
        """Save DataFrame to specified history table"""
        if df.empty:
            logger.warning(f"No data to save to {table_name}")
            return 0
        
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            saved_count = 0
            
            for _, row in df.iterrows():
                cursor.execute(f'''
                    INSERT OR REPLACE INTO nepse_history_{table_name}
                    (trade_date, index_value, percent_change, difference, turnover, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    row['trade_date'],
                    row['index_value'],
                    row['percent_change'],
                    row['difference'],
                    row['turnover']
                ))
                saved_count += 1
            
            # Update metadata
            cursor.execute(f'''
                UPDATE nepse_history_metadata 
                SET last_update = CURRENT_TIMESTAMP,
                    record_count = ?,
                    date_range_start = ?,
                    date_range_end = ?
                WHERE table_name = ?
            ''', (
                len(df),
                df['trade_date'].min(),
                df['trade_date'].max(),
                table_name
            ))
            
            conn.commit()
            logger.info(f"Saved {saved_count} records to nepse_history_{table_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving to {table_name}: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def scrape_weekly_data(self, force=False):
        """Scrape and save last 7 days of data"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info("Scraping weekly NEPSE data...")
        df = self.fetch_history_from_api(start_date, end_date)
        
        if not df.empty:
            return self.save_to_table(df, 'weekly')
        return 0
    
    def scrape_monthly_data(self, force=False):
        """Scrape and save last 30 days of data"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info("Scraping monthly NEPSE data...")
        df = self.fetch_history_from_api(start_date, end_date)
        
        if not df.empty:
            return self.save_to_table(df, 'monthly')
        return 0
    
    def scrape_yearly_data(self, force=False):
        """Scrape and save last 365 days of data"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        logger.info("Scraping yearly NEPSE data...")
        df = self.fetch_history_from_api(start_date, end_date)
        
        if not df.empty:
            return self.save_to_table(df, 'yearly')
        return 0
    
    def scrape_all_periods(self, force=False):
        """Scrape all time periods (weekly, monthly, yearly)"""
        results = {
            'weekly': self.scrape_weekly_data(force),
            'monthly': self.scrape_monthly_data(force),
            'yearly': self.scrape_yearly_data(force)
        }
        
        logger.info(f"Scraped all periods: {results}")
        return results
    
    def get_weekly_data(self):
        """Get weekly historical data"""
        return self._get_data_from_table('weekly')
    
    def get_monthly_data(self):
        """Get monthly historical data"""
        return self._get_data_from_table('monthly')
    
    def get_yearly_data(self):
        """Get yearly historical data"""
        return self._get_data_from_table('yearly')
    
    def _get_data_from_table(self, table_name):
        """Get data from specified table"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f'''
                SELECT trade_date, index_value, percent_change, difference, turnover
                FROM nepse_history_{table_name}
                ORDER BY trade_date DESC
            ''')
            
            data = []
            for row in cursor.fetchall():
                data.append({
                    'date': str(row[0]),
                    'index_value': row[1],
                    'percent_change': row[2],
                    'difference': row[3],
                    'turnover': row[4]
                })
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching {table_name} data: {e}")
            return []
        finally:
            conn.close()
    
    def get_metadata(self):
        """Get metadata for all tables"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT table_name, last_update, record_count, date_range_start, date_range_end
                FROM nepse_history_metadata
            ''')
            
            metadata = {}
            for row in cursor.fetchall():
                metadata[row[0]] = {
                    'last_update': row[1],
                    'record_count': row[2],
                    'date_range_start': row[3],
                    'date_range_end': row[4]
                }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return {}
        finally:
            conn.close()
    
    def get_statistics(self, period='monthly'):
        """Get statistics for a given period"""
        data = self._get_data_from_table(period)
        
        if not data:
            return None
        
        values = [d['index_value'] for d in data]
        
        return {
            'period': period,
            'record_count': len(data),
            'latest_value': data[0]['index_value'],
            'latest_date': data[0]['date'],
            'highest': max(values),
            'lowest': min(values),
            'average': sum(values) / len(values),
            'date_range': {
                'start': data[-1]['date'],
                'end': data[0]['date']
            }
        }
    
    def clean_old_data(self):
        """Remove data older than the intended period for each table"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            # Clean weekly table (keep only last 7 days)
            cutoff_weekly = (datetime.now() - timedelta(days=7)).date()
            cursor.execute('''
                DELETE FROM nepse_history_weekly 
                WHERE trade_date < ?
            ''', (cutoff_weekly,))
            
            # Clean monthly table (keep only last 30 days)
            cutoff_monthly = (datetime.now() - timedelta(days=30)).date()
            cursor.execute('''
                DELETE FROM nepse_history_monthly 
                WHERE trade_date < ?
            ''', (cutoff_monthly,))
            
            # Clean yearly table (keep only last 365 days)
            cutoff_yearly = (datetime.now() - timedelta(days=365)).date()
            cursor.execute('''
                DELETE FROM nepse_history_yearly 
                WHERE trade_date < ?
            ''', (cutoff_yearly,))
            
            conn.commit()
            logger.info("Cleaned old data from history tables")
            
        except Exception as e:
            logger.error(f"Error cleaning old data: {e}")
            conn.rollback()
        finally:
            conn.close()