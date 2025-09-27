# auth_service.py - Fixed version with MySQL/SQLite support

import os
import sqlite3
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify

logger = logging.getLogger(__name__)

class AuthService:
    """Handle all authentication, authorization and security operations"""
    
    def __init__(self, db_service):
        """Initialize with DatabaseService instead of just db_path"""
        self.db_service = db_service
        self.db_type = db_service.db_type
        self._init_auth_tables()
    
    def _get_connection(self):
        """Get database connection using the database service"""
        return self.db_service.get_connection()
    
    def _init_auth_tables(self):
        """Initialize authentication-related database tables"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if self.db_type == 'sqlite':
            cursor.execute('PRAGMA foreign_keys = ON')
            
            # SQLite version
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key_id TEXT UNIQUE NOT NULL,
                    key_hash TEXT NOT NULL,
                    key_type TEXT NOT NULL CHECK (key_type IN ('admin', 'regular')),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_by TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_used DATETIME,
                    max_devices INTEGER DEFAULT 1,
                    description TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS device_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    device_info TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (key_id) REFERENCES api_keys (key_id) ON DELETE CASCADE,
                    UNIQUE(key_id, device_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key_id TEXT,
                    device_id TEXT,
                    endpoint TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    ip_address TEXT,
                    user_agent TEXT,
                    FOREIGN KEY (key_id) REFERENCES api_keys (key_id) ON DELETE SET NULL
                )
            ''')
            
            # Create indexes for SQLite
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_key_id ON api_keys(key_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_device_sessions_key_device ON device_sessions(key_id, device_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_api_logs_key_timestamp ON api_logs(key_id, timestamp)')
            
        else:  # MySQL
            # MySQL version
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key_id VARCHAR(100) UNIQUE NOT NULL,
                    key_hash VARCHAR(64) NOT NULL,
                    key_type ENUM('admin', 'regular') NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by VARCHAR(100),
                    is_active BOOLEAN DEFAULT TRUE,
                    last_used TIMESTAMP NULL,
                    max_devices INT DEFAULT 1,
                    description TEXT,
                    INDEX idx_key_id (key_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS device_sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key_id VARCHAR(100) NOT NULL,
                    device_id VARCHAR(100) NOT NULL,
                    device_info TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    UNIQUE KEY unique_key_device (key_id, device_id),
                    INDEX idx_key_device (key_id, device_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key_id VARCHAR(100),
                    device_id VARCHAR(100),
                    endpoint VARCHAR(200),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    INDEX idx_key_timestamp (key_id, timestamp)
                )
            ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Authentication tables initialized ({self.db_type})")
    
    def generate_api_key(self, key_type='regular', created_by='system', description=''):
        """Generate a new API key pair"""
        api_key = secrets.token_urlsafe(32)
        key_id = f"npse_{key_type}_{secrets.token_urlsafe(8)}"
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        max_devices = 5 if key_type == 'admin' else 1
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('PRAGMA foreign_keys = ON')
                cursor.execute('''
                    INSERT INTO api_keys (key_id, key_hash, key_type, created_by, max_devices, description)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (key_id, key_hash, key_type, created_by, max_devices, description))
            else:  # MySQL
                cursor.execute('''
                    INSERT INTO api_keys (key_id, key_hash, key_type, created_by, max_devices, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (key_id, key_hash, key_type, created_by, max_devices, description))
            
            conn.commit()
            logger.info(f"Generated new {key_type} API key: {key_id}")
            
            return {
                'key_id': key_id,
                'api_key': api_key,
                'key_type': key_type,
                'max_devices': max_devices,
                'created_at': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error generating API key: {e}")
            return None
        finally:
            conn.close()
    
    def validate_request(self, api_key, device_id, device_info='', endpoint='', ip_address='', user_agent=''):
        """Validate API request and manage device sessions"""
        if not api_key or not device_id:
            return {'valid': False, 'error': 'Missing API key or device ID'}
        
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Validate API key
            if self.db_type == 'sqlite':
                cursor.execute('PRAGMA foreign_keys = ON')
                cursor.execute('''
                    SELECT key_id, key_type, max_devices, is_active 
                    FROM api_keys 
                    WHERE key_hash = ? AND is_active = TRUE
                ''', (key_hash,))
            else:  # MySQL
                cursor.execute('''
                    SELECT key_id, key_type, max_devices, is_active 
                    FROM api_keys 
                    WHERE key_hash = %s AND is_active = TRUE
                ''', (key_hash,))
            
            key_record = cursor.fetchone()
            if not key_record:
                self._log_request(cursor, None, device_id, endpoint, ip_address, user_agent)
                return {'valid': False, 'error': 'Invalid API key'}
            
            key_id, key_type, max_devices, is_active = key_record
            
            # Update last used timestamp
            if self.db_type == 'sqlite':
                cursor.execute('UPDATE api_keys SET last_used = ? WHERE key_id = ?', 
                              (datetime.now(), key_id))
            else:  # MySQL
                cursor.execute('UPDATE api_keys SET last_used = %s WHERE key_id = %s', 
                              (datetime.now(), key_id))
            
            # Handle device session
            session_result = self._manage_device_session(
                cursor, key_id, device_id, device_info, max_devices
            )
            
            if not session_result['success']:
                self._log_request(cursor, key_id, device_id, endpoint, ip_address, user_agent)
                return {'valid': False, 'error': session_result['error']}
            
            # Log successful request
            self._log_request(cursor, key_id, device_id, endpoint, ip_address, user_agent)
            conn.commit()
            
            return {
                'valid': True,
                'key_id': key_id,
                'key_type': key_type,
                'max_devices': max_devices
            }
            
        except Exception as e:
            logger.error(f"Error validating request: {e}")
            return {'valid': False, 'error': 'Authentication error'}
        finally:
            conn.close()
    
    def _manage_device_session(self, cursor, key_id, device_id, device_info, max_devices):
        """Manage device sessions for API key"""
        # Check current active devices
        if self.db_type == 'sqlite':
            cursor.execute('''
                SELECT COUNT(*) FROM device_sessions 
                WHERE key_id = ? AND is_active = TRUE
            ''', (key_id,))
            
            # Check if this device already has a session
            cursor.execute('''
                SELECT id FROM device_sessions 
                WHERE key_id = ? AND device_id = ? AND is_active = TRUE
            ''', (key_id, device_id))
        else:  # MySQL
            cursor.execute('''
                SELECT COUNT(*) FROM device_sessions 
                WHERE key_id = %s AND is_active = TRUE
            ''', (key_id,))
        
        active_devices = cursor.fetchone()[0]
        
        if self.db_type == 'mysql':
            cursor.execute('''
                SELECT id FROM device_sessions 
                WHERE key_id = %s AND device_id = %s AND is_active = TRUE
            ''', (key_id, device_id))
        
        existing_session = cursor.fetchone()
        
        if existing_session:
            # Update existing session
            if self.db_type == 'sqlite':
                cursor.execute('''
                    UPDATE device_sessions 
                    SET last_activity = ?, device_info = ?
                    WHERE key_id = ? AND device_id = ?
                ''', (datetime.now(), device_info, key_id, device_id))
            else:  # MySQL
                cursor.execute('''
                    UPDATE device_sessions 
                    SET last_activity = %s, device_info = %s
                    WHERE key_id = %s AND device_id = %s
                ''', (datetime.now(), device_info, key_id, device_id))
            return {'success': True}
        
        # Check device limit for new sessions
        if active_devices >= max_devices:
            return {'success': False, 'error': f'Maximum devices ({max_devices}) reached'}
        
        # Create new device session
        if self.db_type == 'sqlite':
            cursor.execute('''
                INSERT INTO device_sessions (key_id, device_id, device_info, last_activity)
                VALUES (?, ?, ?, ?)
            ''', (key_id, device_id, device_info, datetime.now()))
        else:  # MySQL
            cursor.execute('''
                INSERT INTO device_sessions (key_id, device_id, device_info, last_activity)
                VALUES (%s, %s, %s, %s)
            ''', (key_id, device_id, device_info, datetime.now()))
        
        return {'success': True}
    
    def _log_request(self, cursor, key_id, device_id, endpoint, ip_address, user_agent):
        """Log API request"""
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    INSERT INTO api_logs (key_id, device_id, endpoint, ip_address, user_agent)
                    VALUES (?, ?, ?, ?, ?)
                ''', (key_id, device_id, endpoint, ip_address, user_agent))
            else:  # MySQL
                cursor.execute('''
                    INSERT INTO api_logs (key_id, device_id, endpoint, ip_address, user_agent)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (key_id, device_id, endpoint, ip_address, user_agent))
        except Exception as e:
            logger.warning(f"Failed to log API request: {e}")
    
    def get_key_details(self, key_id):
        """Get detailed information about an API key"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                           k.last_used, k.max_devices, k.description,
                           COUNT(d.id) as active_devices
                    FROM api_keys k
                    LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                    WHERE k.key_id = ?
                    GROUP BY k.key_id
                ''', (key_id,))
            else:  # MySQL
                cursor.execute('''
                    SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                           k.last_used, k.max_devices, k.description,
                           COUNT(d.id) as active_devices
                    FROM api_keys k
                    LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                    WHERE k.key_id = %s
                    GROUP BY k.key_id
                ''', (key_id,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'key_id': result[0],
                    'key_type': result[1],
                    'created_at': result[2],
                    'created_by': result[3],
                    'is_active': bool(result[4]),
                    'last_used': result[5],
                    'max_devices': result[6],
                    'description': result[7],
                    'active_devices': result[8]
                }
            return None
        finally:
            conn.close()
    
    def list_all_keys(self):
        """List all API keys (admin function)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('''
                    SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                           k.last_used, k.max_devices, k.description,
                           COUNT(d.id) as active_devices
                    FROM api_keys k
                    LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                    GROUP BY k.key_id
                    ORDER BY k.created_at DESC
                ''')
            else:  # MySQL
                cursor.execute('''
                    SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                           k.last_used, k.max_devices, k.description,
                           COUNT(d.id) as active_devices
                    FROM api_keys k
                    LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                    GROUP BY k.key_id
                    ORDER BY k.created_at DESC
                ''')
            
            keys = []
            for row in cursor.fetchall():
                keys.append({
                    'key_id': row[0],
                    'key_type': row[1],
                    'created_at': row[2],
                    'created_by': row[3],
                    'is_active': bool(row[4]),
                    'last_used': row[5],
                    'max_devices': row[6],
                    'description': row[7],
                    'active_devices': row[8]
                })
            return keys
        finally:
            conn.close()
    
    def deactivate_key(self, key_id):
        """Deactivate an API key"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if self.db_type == 'sqlite':
                cursor.execute('UPDATE api_keys SET is_active = FALSE WHERE key_id = ?', (key_id,))
                cursor.execute('UPDATE device_sessions SET is_active = FALSE WHERE key_id = ?', (key_id,))
            else:  # MySQL
                cursor.execute('UPDATE api_keys SET is_active = FALSE WHERE key_id = %s', (key_id,))
                cursor.execute('UPDATE device_sessions SET is_active = FALSE WHERE key_id = %s', (key_id,))
            
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deactivating key {key_id}: {e}")
            return False
        finally:
            conn.close()
    
    def get_usage_stats(self, key_id=None, days=7):
        """Get API usage statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            since_date = datetime.now() - timedelta(days=days)
            
            if key_id:
                if self.db_type == 'sqlite':
                    cursor.execute('''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE key_id = ? AND timestamp >= ?
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    ''', (key_id, since_date))
                else:  # MySQL
                    cursor.execute('''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE key_id = %s AND timestamp >= %s
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    ''', (key_id, since_date))
            else:
                if self.db_type == 'sqlite':
                    cursor.execute('''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE timestamp >= ?
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    ''', (since_date,))
                else:  # MySQL
                    cursor.execute('''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE timestamp >= %s
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    ''', (since_date,))
            
            stats = {}
            for row in cursor.fetchall():
                stats[row[0]] = row[1]
            
            return stats
        finally:
            conn.close()

def create_auth_decorators(auth_service):
    """Create authentication decorators with dependency injection"""
    
    def require_auth(f):
        """Decorator to require API key authentication"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
            device_id = request.headers.get('X-Device-ID') or request.args.get('device_id')
            device_info = request.headers.get('X-Device-Info', '')
            
            if not api_key or not device_id:
                return jsonify({
                    'success': False,
                    'error': 'API key and device ID are required'
                }), 401
            
            validation = auth_service.validate_request(
                api_key=api_key,
                device_id=device_id,
                device_info=device_info,
                endpoint=request.endpoint,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent', '')
            )
            
            if not validation['valid']:
                return jsonify({
                    'success': False,
                    'error': validation['error']
                }), 401
            
            request.auth_info = validation
            return f(*args, **kwargs)
        
        return decorated_function
    
    def require_admin(f):
        """Decorator to require admin privileges"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not hasattr(request, 'auth_info') or request.auth_info.get('key_type') != 'admin':
                return jsonify({
                    'success': False,
                    'error': 'Admin privileges required'
                }), 403
            return f(*args, **kwargs)
        
        return decorated_function
    
    return require_auth, require_admin