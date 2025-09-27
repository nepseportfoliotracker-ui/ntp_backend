# db_service.py - Database service with PostgreSQL support for Railway

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class DatabaseService:
    """Database service that works with both SQLite and PostgreSQL"""
    
    def __init__(self):
        self.db_type = os.environ.get('DATABASE_TYPE', 'sqlite')
        self.connection_pool = None
        
        if self.db_type == 'postgresql':
            self._init_postgresql()
        else:
            self._init_sqlite()
        
        self._init_tables()
    
    def _init_postgresql(self):
        """Initialize PostgreSQL connection"""
        try:
            database_url = os.environ.get('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not set")
            
            # Create connection pool
            self.connection_pool = SimpleConnectionPool(
                1, 20, database_url, cursor_factory=RealDictCursor
            )
            logger.info("PostgreSQL connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL: {e}")
            raise
    
    def _init_sqlite(self):
        """Initialize SQLite (fallback for local development)"""
        self.db_path = os.environ.get('DATABASE_PATH', 'nepal_stock.db')
        logger.info(f"Using SQLite database: {self.db_path}")
    
    def get_connection(self):
        """Get database connection"""
        if self.db_type == 'postgresql':
            return self.connection_pool.getconn()
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            return conn
    
    def return_connection(self, conn):
        """Return connection to pool"""
        if self.db_type == 'postgresql':
            self.connection_pool.putconn(conn)
        else:
            conn.close()
    
    def _init_tables(self):
        """Initialize all database tables"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Stocks table
            if self.db_type == 'postgresql':
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stocks (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(10) NOT NULL,
                        company_name VARCHAR(200),
                        ltp DECIMAL(10,2),
                        change DECIMAL(10,2),
                        change_percent DECIMAL(8,4),
                        high DECIMAL(10,2),
                        low DECIMAL(10,2),
                        open_price DECIMAL(10,2),
                        prev_close DECIMAL(10,2),
                        qty INTEGER,
                        turnover DECIMAL(15,2),
                        trades INTEGER DEFAULT 0,
                        source VARCHAR(100),
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_latest BOOLEAN DEFAULT TRUE
                    )
                ''')
                
                # Market summary table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS market_summary (
                        id SERIAL PRIMARY KEY,
                        total_turnover DECIMAL(15,2),
                        total_trades INTEGER,
                        total_scrips INTEGER,
                        advancing INTEGER DEFAULT 0,
                        declining INTEGER DEFAULT 0,
                        unchanged INTEGER DEFAULT 0,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_latest BOOLEAN DEFAULT TRUE
                    )
                ''')
                
                # API Keys table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS api_keys (
                        id SERIAL PRIMARY KEY,
                        key_id VARCHAR(50) UNIQUE NOT NULL,
                        key_hash VARCHAR(64) NOT NULL,
                        key_type VARCHAR(20) NOT NULL CHECK (key_type IN ('admin', 'regular')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by VARCHAR(50),
                        is_active BOOLEAN DEFAULT TRUE,
                        last_used TIMESTAMP,
                        max_devices INTEGER DEFAULT 1,
                        description TEXT
                    )
                ''')
                
                # Device sessions table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS device_sessions (
                        id SERIAL PRIMARY KEY,
                        key_id VARCHAR(50) NOT NULL,
                        device_id VARCHAR(100) NOT NULL,
                        device_info TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        FOREIGN KEY (key_id) REFERENCES api_keys (key_id) ON DELETE CASCADE,
                        UNIQUE(key_id, device_id)
                    )
                ''')
                
                # API logs table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS api_logs (
                        id SERIAL PRIMARY KEY,
                        key_id VARCHAR(50),
                        device_id VARCHAR(100),
                        endpoint VARCHAR(200),
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ip_address INET,
                        user_agent TEXT,
                        FOREIGN KEY (key_id) REFERENCES api_keys (key_id) ON DELETE SET NULL
                    )
                ''')
                
                # Price history table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS price_history (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(10) NOT NULL,
                        date DATE,
                        open_price DECIMAL(10,2),
                        high DECIMAL(10,2),
                        low DECIMAL(10,2),
                        close_price DECIMAL(10,2),
                        volume INTEGER,
                        turnover DECIMAL(15,2),
                        UNIQUE(symbol, date)
                    )
                ''')
                
            else:
                # SQLite table creation (your existing code)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        company_name TEXT,
                        ltp REAL,
                        change REAL,
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
                
                # Add other SQLite tables here (same as your existing code)
            
            # Create indexes
            self._create_indexes(cursor)
            conn.commit()
            logger.info("Database tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize tables: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)
    
    def _create_indexes(self, cursor):
        """Create database indexes"""
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_stocks_symbol_latest ON stocks(symbol, is_latest)',
            'CREATE INDEX IF NOT EXISTS idx_stocks_timestamp ON stocks(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_api_keys_key_id ON api_keys(key_id)',
            'CREATE INDEX IF NOT EXISTS idx_device_sessions_key_device ON device_sessions(key_id, device_id)',
            'CREATE INDEX IF NOT EXISTS idx_api_logs_key_timestamp ON api_logs(key_id, timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_price_history_symbol_date ON price_history(symbol, date)',
            'CREATE INDEX IF NOT EXISTS idx_market_summary_latest ON market_summary(is_latest)'
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except Exception as e:
                logger.warning(f"Failed to create index: {e}")
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a database query"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            
            if fetch:
                if fetch == 'one':
                    result = cursor.fetchone()
                    return dict(result) if result else None
                else:
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
            else:
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)
    
    def execute_many(self, query, params_list):
        """Execute many queries with different parameters"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            if self.db_type == 'postgresql':
                from psycopg2.extras import execute_batch
                execute_batch(cursor, query, params_list)
            else:
                cursor.executemany(query, params_list)
            
            conn.commit()
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Batch query failed: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)
    
    def get_placeholder(self):
        """Get parameter placeholder for the database type"""
        return '%s' if self.db_type == 'postgresql' else '?'