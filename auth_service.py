# auth_service.py - Railway-compatible authentication service

import os
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify
from db_service import DatabaseService

logger = logging.getLogger(__name__)

class AuthService:
    """Railway-compatible authentication service"""
    
    def __init__(self):
        self.db = DatabaseService()
        self.placeholder = self.db.get_placeholder()
    
    def generate_api_key(self, key_type='regular', created_by='system', description=''):
        """Generate a new API key pair"""
        api_key = secrets.token_urlsafe(32)
        key_id = f"npse_{key_type}_{secrets.token_urlsafe(8)}"
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        max_devices = 5 if key_type == 'admin' else 1
        
        try:
            query = f'''
                INSERT INTO api_keys (key_id, key_hash, key_type, created_by, max_devices, description)
                VALUES ({','.join([self.placeholder] * 6)})
            '''
            
            self.db.execute_query(
                query,
                (key_id, key_hash, key_type, created_by, max_devices, description)
            )
            
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
    
    def validate_request(self, api_key, device_id, device_info='', endpoint='', ip_address='', user_agent=''):
        """Validate API request and manage device sessions"""
        if not api_key or not device_id:
            return {'valid': False, 'error': 'Missing API key or device ID'}
        
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        try:
            # Validate API key
            key_query = f'''
                SELECT key_id, key_type, max_devices, is_active 
                FROM api_keys 
                WHERE key_hash = {self.placeholder} AND is_active = TRUE
            '''
            
            key_record = self.db.execute_query(key_query, (key_hash,), fetch='one')
            
            if not key_record:
                self._log_request(None, device_id, endpoint, ip_address, user_agent)
                return {'valid': False, 'error': 'Invalid API key'}
            
            key_id = key_record['key_id']
            key_type = key_record['key_type']
            max_devices = key_record['max_devices']
            
            # Update last used timestamp
            update_query = f'UPDATE api_keys SET last_used = CURRENT_TIMESTAMP WHERE key_id = {self.placeholder}'
            self.db.execute_query(update_query, (key_id,))
            
            # Handle device session
            session_result = self._manage_device_session(
                key_id, device_id, device_info, max_devices
            )
            
            if not session_result['success']:
                self._log_request(key_id, device_id, endpoint, ip_address, user_agent)
                return {'valid': False, 'error': session_result['error']}
            
            # Log successful request
            self._log_request(key_id, device_id, endpoint, ip_address, user_agent)
            
            return {
                'valid': True,
                'key_id': key_id,
                'key_type': key_type,
                'max_devices': max_devices
            }
            
        except Exception as e:
            logger.error(f"Error validating request: {e}")
            return {'valid': False, 'error': 'Authentication error'}
    
    def _manage_device_session(self, key_id, device_id, device_info, max_devices):
        """Manage device sessions for API key"""
        try:
            # Check current active devices
            count_query = f'''
                SELECT COUNT(*) as count FROM device_sessions 
                WHERE key_id = {self.placeholder} AND is_active = TRUE
            '''
            count_result = self.db.execute_query(count_query, (key_id,), fetch='one')
            active_devices = count_result['count'] if count_result else 0
            
            # Check if this device already has a session
            session_query = f'''
                SELECT id FROM device_sessions 
                WHERE key_id = {self.placeholder} AND device_id = {self.placeholder} AND is_active = TRUE
            '''
            existing_session = self.db.execute_query(
                session_query, (key_id, device_id), fetch='one'
            )
            
            if existing_session:
                # Update existing session
                update_query = f'''
                    UPDATE device_sessions 
                    SET last_activity = CURRENT_TIMESTAMP, device_info = {self.placeholder}
                    WHERE key_id = {self.placeholder} AND device_id = {self.placeholder}
                '''
                self.db.execute_query(update_query, (device_info, key_id, device_id))
                return {'success': True}
            
            # Check device limit for new sessions
            if active_devices >= max_devices:
                return {'success': False, 'error': f'Maximum devices ({max_devices}) reached'}
            
            # Create new device session
            insert_query = f'''
                INSERT INTO device_sessions (key_id, device_id, device_info, last_activity)
                VALUES ({','.join([self.placeholder] * 4)})
            '''
            self.db.execute_query(
                insert_query,
                (key_id, device_id, device_info, datetime.now())
            )
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error managing device session: {e}")
            return {'success': False, 'error': 'Session management error'}
    
    def _log_request(self, key_id, device_id, endpoint, ip_address, user_agent):
        """Log API request"""
        try:
            log_query = f'''
                INSERT INTO api_logs (key_id, device_id, endpoint, ip_address, user_agent)
                VALUES ({','.join([self.placeholder] * 5)})
            '''
            self.db.execute_query(
                log_query,
                (key_id, device_id, endpoint, ip_address, user_agent)
            )
        except Exception as e:
            logger.warning(f"Failed to log API request: {e}")
    
    def get_key_details(self, key_id):
        """Get detailed information about an API key"""
        try:
            query = f'''
                SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                       k.last_used, k.max_devices, k.description,
                       COUNT(d.id) as active_devices
                FROM api_keys k
                LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                WHERE k.key_id = {self.placeholder}
                GROUP BY k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                         k.last_used, k.max_devices, k.description
            '''
            
            result = self.db.execute_query(query, (key_id,), fetch='one')
            if result:
                return {
                    'key_id': result['key_id'],
                    'key_type': result['key_type'],
                    'created_at': result['created_at'],
                    'created_by': result['created_by'],
                    'is_active': bool(result['is_active']),
                    'last_used': result['last_used'],
                    'max_devices': result['max_devices'],
                    'description': result['description'],
                    'active_devices': result['active_devices']
                }
            return None
        except Exception as e:
            logger.error(f"Error getting key details: {e}")
            return None
    
    def list_all_keys(self):
        """List all API keys (admin function)"""
        try:
            query = '''
                SELECT k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                       k.last_used, k.max_devices, k.description,
                       COUNT(d.id) as active_devices
                FROM api_keys k
                LEFT JOIN device_sessions d ON k.key_id = d.key_id AND d.is_active = TRUE
                GROUP BY k.key_id, k.key_type, k.created_at, k.created_by, k.is_active, 
                         k.last_used, k.max_devices, k.description
                ORDER BY k.created_at DESC
            '''
            
            results = self.db.execute_query(query, fetch='all')
            keys = []
            for row in results:
                keys.append({
                    'key_id': row['key_id'],
                    'key_type': row['key_type'],
                    'created_at': row['created_at'],
                    'created_by': row['created_by'],
                    'is_active': bool(row['is_active']),
                    'last_used': row['last_used'],
                    'max_devices': row['max_devices'],
                    'description': row['description'],
                    'active_devices': row['active_devices']
                })
            return keys
        except Exception as e:
            logger.error(f"Error listing keys: {e}")
            return []
    
    def deactivate_key(self, key_id):
        """Deactivate an API key"""
        try:
            key_query = f'UPDATE api_keys SET is_active = FALSE WHERE key_id = {self.placeholder}'
            session_query = f'UPDATE device_sessions SET is_active = FALSE WHERE key_id = {self.placeholder}'
            
            key_rows = self.db.execute_query(key_query, (key_id,))
            self.db.execute_query(session_query, (key_id,))
            
            return key_rows > 0
        except Exception as e:
            logger.error(f"Error deactivating key {key_id}: {e}")
            return False
    
    def get_usage_stats(self, key_id=None, days=7):
        """Get API usage statistics"""
        try:
            since_date = datetime.now() - timedelta(days=days)
            
            if key_id:
                # PostgreSQL and SQLite handle date functions differently
                if self.db.db_type == 'postgresql':
                    query = f'''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE key_id = {self.placeholder} AND timestamp >= {self.placeholder}
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    '''
                else:
                    query = f'''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE key_id = {self.placeholder} AND timestamp >= {self.placeholder}
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    '''
                params = (key_id, since_date)
            else:
                if self.db.db_type == 'postgresql':
                    query = f'''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE timestamp >= {self.placeholder}
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    '''
                else:
                    query = f'''
                        SELECT DATE(timestamp) as date, COUNT(*) as requests
                        FROM api_logs 
                        WHERE timestamp >= {self.placeholder}
                        GROUP BY DATE(timestamp)
                        ORDER BY date DESC
                    '''
                params = (since_date,)
            
            results = self.db.execute_query(query, params, fetch='all')
            stats = {}
            for row in results:
                stats[row['date']] = row['requests']
            
            return stats
        except Exception as e:
            logger.error(f"Error getting usage stats: {e}")
            return {}


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