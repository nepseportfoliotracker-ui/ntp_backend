# database_service.py - Split database: persistent auth + ephemeral data

import sqlite3
import logging
import os

logger = logging.getLogger(__name__)

class DatabaseService:
    """Database service with separate auth and data databases"""
    
    def __init__(self, data_db_path=None, auth_db_path=None):
        self.db_type = 'sqlite'
        
        # DATA DATABASE (ephemeral - stocks, IPOs, prices, history)
        if data_db_path is None:
            self.data_db_path = os.environ.get('DATA_DATABASE_PATH', 'nepal_stock_data.db')
        else:
            self.data_db_path = data_db_path
        
        # AUTH DATABASE (persistent - API keys, sessions, logs)
        if auth_db_path is None:
            # Check for Railway volume for auth database
            volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
            if volume_path:
                self.auth_db_path = os.path.join(volume_path, 'nepal_stock_auth.db')
                logger.info(f"Using Railway persistent volume for auth: {self.auth_db_path}")
            else:
                self.auth_db_path = os.environ.get('AUTH_DATABASE_PATH', 'nepal_stock_auth.db')
                logger.info(f"Using local auth database: {self.auth_db_path}")
        else:
            self.auth_db_path = auth_db_path
        
        # Ensure auth directory exists
        auth_dir = os.path.dirname(self.auth_db_path)
        if auth_dir and not os.path.exists(auth_dir):
            os.makedirs(auth_dir, exist_ok=True)
            logger.info(f"Created auth database directory: {auth_dir}")
        
        logger.info(f"Database service initialized:")
        logger.info(f"  - Data DB (ephemeral): {self.data_db_path}")
        logger.info(f"  - Auth DB (persistent): {self.auth_db_path}")
    
    def get_connection(self, db_type='data'):
        """
        Get database connection
        
        Args:
            db_type: 'data' for stocks/IPOs/prices, 'auth' for authentication
        """
        if db_type == 'auth':
            conn = sqlite3.connect(self.auth_db_path)
        else:
            conn = sqlite3.connect(self.data_db_path)
        
        conn.execute('PRAGMA foreign_keys = ON')
        return conn
    
    def get_auth_connection(self):
        """Convenience method to get auth database connection"""
        return self.get_connection('auth')
    
    def get_data_connection(self):
        """Convenience method to get data database connection"""
        return self.get_connection('data')
    
    def get_database_info(self):
        """Get information about both databases"""
        info = {
            'type': self.db_type,
            'databases': {}
        }
        
        # Data database info
        try:
            data_exists = os.path.exists(self.data_db_path)
            data_size = os.path.getsize(self.data_db_path) if data_exists else 0
            info['databases']['data'] = {
                'path': self.data_db_path,
                'exists': data_exists,
                'size_mb': round(data_size / (1024 * 1024), 2),
                'persistent': False,
                'description': 'Stocks, IPOs, prices, NEPSE history (ephemeral)'
            }
        except Exception as e:
            info['databases']['data'] = {'error': str(e)}
        
        # Auth database info
        try:
            auth_exists = os.path.exists(self.auth_db_path)
            auth_size = os.path.getsize(self.auth_db_path) if auth_exists else 0
            info['databases']['auth'] = {
                'path': self.auth_db_path,
                'exists': auth_exists,
                'size_mb': round(auth_size / (1024 * 1024), 2),
                'persistent': bool(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')),
                'description': 'API keys, sessions, logs (persistent)'
            }
        except Exception as e:
            info['databases']['auth'] = {'error': str(e)}
        
        return info
    
    # Backward compatibility - defaults to data database
    def __getattr__(self, name):
        """Provide backward compatibility for old code"""
        if name == 'db_path':
            return self.data_db_path
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")