# auth_service.py - Complete SQLite version with POST support

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
        """Initialize with DatabaseService"""
        self.db_service = db_service
        self._init_auth_tables()
    
    def _get_connection(self):
        """Get database connection"""
        return self.db_service.get_auth_connection()
    
    def _init_auth_tables(self):
        """Initialize authentication-related database tables"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('PRAGMA foreign_keys = ON')
        
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
                method TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                status_code INTEGER,
                FOREIGN KEY (key_id) REFERENCES api_keys (key_id) ON DELETE SET NULL
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_key_id ON api_keys(key_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_device_sessions_key_device ON device_sessions(key_id, device_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_api_logs_key_timestamp ON api_logs(key_id, timestamp)')
        
        conn.commit()
        conn.close()
        logger.info("Authentication tables initialized (SQLite)")
    
    # auth_service.py - Add these FIXED methods to your AuthService class

    def delete_key_permanently(self, key_id):
        """
        Permanently delete an API key and all associated data
        This is the proper delete method that actually removes the key
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('PRAGMA foreign_keys = ON')
            
            # First check if key exists
            cursor.execute('SELECT key_id, key_type FROM api_keys WHERE key_id = ?', (key_id,))
            key_record = cursor.fetchone()
            
            if not key_record:
                logger.warning(f"Cannot delete - key not found: {key_id}")
                return False
            
            logger.info(f"Deleting key: {key_id} (type: {key_record[1]})")
            
            # Delete associated sessions first (due to foreign key constraint)
            cursor.execute('DELETE FROM device_sessions WHERE key_id = ?', (key_id,))
            deleted_sessions = cursor.rowcount
            logger.info(f"Deleted {deleted_sessions} sessions for key {key_id}")
            
            # Delete the key itself
            cursor.execute('DELETE FROM api_keys WHERE key_id = ?', (key_id,))
            deleted_keys = cursor.rowcount
            
            conn.commit()
            
            if deleted_keys > 0:
                logger.info(f"Successfully deleted key {key_id} and {deleted_sessions} associated sessions")
                return True
            else:
                logger.error(f"Failed to delete key {key_id} - no rows affected")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting key {key_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def deactivate_key(self, key_id):
        """
        Deactivate an API key (soft delete - keeps in database but inactive)
        Use delete_key_permanently() for actual deletion
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('PRAGMA foreign_keys = ON')
            
            # Check if key exists first
            cursor.execute('SELECT key_id FROM api_keys WHERE key_id = ?', (key_id,))
            if not cursor.fetchone():
                logger.warning(f"Cannot deactivate - key not found: {key_id}")
                return False
            
            # Deactivate the key
            cursor.execute('UPDATE api_keys SET is_active = FALSE WHERE key_id = ?', (key_id,))
            
            # Deactivate all sessions
            cursor.execute('UPDATE device_sessions SET is_active = FALSE WHERE key_id = ?', (key_id,))
            
            conn.commit()
            
            logger.info(f"Deactivated key: {key_id}")
            return cursor.rowcount > 0 or True  # Return True even if no sessions
            
        except Exception as e:
            logger.error(f"Error deactivating key {key_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def key_exists(self, key_id):
        """Check if a key exists in the database"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT key_id FROM api_keys WHERE key_id = ?', (key_id,))
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Error checking key existence: {e}")
            return False
        finally:
            conn.close()

    def get_key_with_sessions(self, key_id):
        """Get key info with session count"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT k.key_id, k.key_type, k.is_active,
                    k.created_at, k.last_used,
                    COUNT(d.id) as session_count
                FROM api_keys k
                LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                WHERE k.key_id = ?
                GROUP BY k.key_id
            ''', (key_id,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'key_id': result[0],
                    'key_type': result[1],
                    'is_active': bool(result[2]),
                    'created_at': result[3],
                    'last_used': result[4],
                    'active_sessions': result[5],
                    'exists': True
                }
            return {'exists': False}
        except Exception as e:
            logger.error(f"Error getting key info: {e}")
            return {'exists': False, 'error': str(e)}
        finally:
            conn.close()

    def generate_api_key(self, key_type='regular', created_by='system', description=''):
        """Generate a new API key pair"""
        api_key = secrets.token_urlsafe(32)
        key_id = f"npse_{key_type}_{secrets.token_urlsafe(8)}"
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        max_devices = 5 if key_type == 'admin' else 1
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('PRAGMA foreign_keys = ON')
            cursor.execute('''
                INSERT INTO api_keys (key_id, key_hash, key_type, created_by, max_devices, description)
                VALUES (?, ?, ?, ?, ?, ?)
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
    
    def validate_request(self, api_key, device_id, device_info='', endpoint='', 
                        method='GET', ip_address='', user_agent='', status_code=None):
        """Validate API request and manage device sessions"""
        if not api_key or not device_id:
            return {'valid': False, 'error': 'Missing API key or device ID'}
        
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('PRAGMA foreign_keys = ON')
            cursor.execute('''
                SELECT key_id, key_type, max_devices, is_active 
                FROM api_keys 
                WHERE key_hash = ? AND is_active = TRUE
            ''', (key_hash,))
            
            key_record = cursor.fetchone()
            if not key_record:
                self._log_request(cursor, None, device_id, endpoint, method, 
                                ip_address, user_agent, 401)
                conn.commit()
                return {'valid': False, 'error': 'Invalid API key'}
            
            key_id, key_type, max_devices, is_active = key_record
            
            # Update last used timestamp
            cursor.execute('UPDATE api_keys SET last_used = ? WHERE key_id = ?', 
                          (datetime.now(), key_id))
            
            # Handle device session
            session_result = self._manage_device_session(
                cursor, key_id, device_id, device_info, max_devices
            )
            
            if not session_result['success']:
                self._log_request(cursor, key_id, device_id, endpoint, method,
                                ip_address, user_agent, 403)
                conn.commit()
                return {'valid': False, 'error': session_result['error']}
            
            # Log successful request
            self._log_request(cursor, key_id, device_id, endpoint, method,
                            ip_address, user_agent, status_code or 200)
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
        cursor.execute('''
            SELECT COUNT(*) FROM device_sessions 
            WHERE key_id = ? AND is_active = TRUE
        ''', (key_id,))
        
        active_devices = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT id FROM device_sessions 
            WHERE key_id = ? AND device_id = ? AND is_active = TRUE
        ''', (key_id, device_id))
        
        existing_session = cursor.fetchone()
        
        if existing_session:
            cursor.execute('''
                UPDATE device_sessions 
                SET last_activity = ?, device_info = ?
                WHERE key_id = ? AND device_id = ?
            ''', (datetime.now(), device_info, key_id, device_id))
            return {'success': True}
        
        if active_devices >= max_devices:
            return {'success': False, 'error': f'Maximum devices ({max_devices}) reached'}
        
        cursor.execute('''
            INSERT INTO device_sessions (key_id, device_id, device_info, last_activity)
            VALUES (?, ?, ?, ?)
        ''', (key_id, device_id, device_info, datetime.now()))
        
        return {'success': True}
    
    def _log_request(self, cursor, key_id, device_id, endpoint, method, 
                    ip_address, user_agent, status_code):
        """Log API request"""
        try:
            cursor.execute('''
                INSERT INTO api_logs (key_id, device_id, endpoint, method, 
                                     ip_address, user_agent, status_code)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (key_id, device_id, endpoint, method, ip_address, user_agent, status_code))
        except Exception as e:
            logger.warning(f"Failed to log API request: {e}")
    
    def get_key_details(self, key_id):
        """Get detailed information about an API key"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                       k.last_used, k.max_devices, k.description,
                       COUNT(d.id) as active_devices
                FROM api_keys k
                LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                WHERE k.key_id = ?
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
            cursor.execute('UPDATE api_keys SET is_active = FALSE WHERE key_id = ?', (key_id,))
            cursor.execute('UPDATE device_sessions SET is_active = FALSE WHERE key_id = ?', (key_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deactivating key {key_id}: {e}")
            return False
        finally:
            conn.close()
    
    def reactivate_key(self, key_id):
        """Reactivate a previously deactivated API key"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('UPDATE api_keys SET is_active = TRUE WHERE key_id = ?', (key_id,))
            conn.commit()
            logger.info(f"Reactivated API key: {key_id}")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error reactivating key {key_id}: {e}")
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
                cursor.execute('''
                    SELECT DATE(timestamp) as date, COUNT(*) as requests
                    FROM api_logs 
                    WHERE key_id = ? AND timestamp >= ?
                    GROUP BY DATE(timestamp)
                    ORDER BY date DESC
                ''', (key_id, since_date))
            else:
                cursor.execute('''
                    SELECT DATE(timestamp) as date, COUNT(*) as requests
                    FROM api_logs 
                    WHERE timestamp >= ?
                    GROUP BY DATE(timestamp)
                    ORDER BY date DESC
                ''', (since_date,))
            
            stats = {}
            for row in cursor.fetchall():
                stats[row[0]] = row[1]
            
            return stats
        finally:
            conn.close()
    
    def get_endpoint_stats(self, key_id=None, days=7):
        """Get statistics by endpoint"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            since_date = datetime.now() - timedelta(days=days)
            
            if key_id:
                cursor.execute('''
                    SELECT endpoint, method, COUNT(*) as requests
                    FROM api_logs 
                    WHERE key_id = ? AND timestamp >= ?
                    GROUP BY endpoint, method
                    ORDER BY requests DESC
                ''', (key_id, since_date))
            else:
                cursor.execute('''
                    SELECT endpoint, method, COUNT(*) as requests
                    FROM api_logs 
                    WHERE timestamp >= ?
                    GROUP BY endpoint, method
                    ORDER BY requests DESC
                ''', (since_date,))
            
            stats = []
            for row in cursor.fetchall():
                stats.append({
                    'endpoint': row[0],
                    'method': row[1],
                    'requests': row[2]
                })
            
            return stats
        finally:
            conn.close()
    
    def cleanup_inactive_sessions(self, days=30):
        """Clean up inactive device sessions older than specified days"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            cursor.execute('''
                DELETE FROM device_sessions 
                WHERE is_active = FALSE AND last_activity < ?
            ''', (cutoff_date,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} inactive sessions")
            
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up sessions: {e}")
            return 0
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
                    'error': 'API key and device ID are required',
                    'flutter_ready': True
                }), 401
            
            validation = auth_service.validate_request(
                api_key=api_key,
                device_id=device_id,
                device_info=device_info,
                endpoint=request.endpoint or request.path,
                method=request.method,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent', '')
            )
            
            if not validation['valid']:
                return jsonify({
                    'success': False,
                    'error': validation['error'],
                    'flutter_ready': True
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
                    'error': 'Admin privileges required',
                    'flutter_ready': True
                }), 403
            return f(*args, **kwargs)
        
        return decorated_function
    
    return require_auth, require_admin