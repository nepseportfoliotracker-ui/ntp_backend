# database_service.py - Simplified SQLite-only version

import sqlite3
import logging
import os

logger = logging.getLogger(__name__)

class DatabaseService:
    """Simple SQLite-only database service"""
    
    def __init__(self, db_path='nepal_stock.db'):
        self.db_type = 'sqlite'
        self.db_path = db_path
        logger.info(f"Database service initialized: SQLite at {self.db_path}")
    
    def get_connection(self):
        """Get SQLite database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA foreign_keys = ON')
        return conn