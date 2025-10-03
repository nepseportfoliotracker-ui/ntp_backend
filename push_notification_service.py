# push_notification_service.py - FCM Push Notification Service

import logging
import json
from datetime import datetime
from firebase_admin import credentials, initialize_app, messaging
import os

logger = logging.getLogger(__name__)

class PushNotificationService:
    """Service for sending push notifications via Firebase Cloud Messaging"""
    
    def __init__(self, db_service):
        self.db_service = db_service
        self.fcm_initialized = False
        self._init_fcm()
        self._create_tables()
    
    def _init_fcm(self):
        """Initialize Firebase Cloud Messaging"""
        try:
            # Option 1: Using service account file
            cred_path = os.environ.get('FIREBASE_CREDENTIALS_PATH', 'firebase-credentials.json')
            if os.path.exists(cred_path):
                cred = credentials.Certificate(cred_path)
                initialize_app(cred)
                self.fcm_initialized = True
                logger.info("FCM initialized successfully with service account file")
                return
            
            # Option 2: Using environment variable JSON
            cred_json = os.environ.get('FIREBASE_CREDENTIALS_JSON')
            if cred_json:
                cred_dict = json.loads(cred_json)
                cred = credentials.Certificate(cred_dict)
                initialize_app(cred)
                self.fcm_initialized = True
                logger.info("FCM initialized successfully with environment variable")
                return
            
            logger.warning("FCM not initialized - no credentials found")
            
        except Exception as e:
            logger.error(f"Failed to initialize FCM: {e}")
            self.fcm_initialized = False
    
    def _create_tables(self):
        """Create tables for device tokens and notification history"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # Device tokens table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS device_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT UNIQUE NOT NULL,
                    fcm_token TEXT NOT NULL,
                    platform TEXT DEFAULT 'android' CHECK (platform IN ('android', 'ios')),
                    is_active INTEGER DEFAULT 1,
                    notification_enabled INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Notification history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS notification_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    data TEXT,
                    company_name TEXT,
                    sent_to_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failure_count INTEGER DEFAULT 0,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Notification tracking table (per device)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS notification_device_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_id INTEGER,
                    device_id TEXT,
                    status TEXT CHECK (status IN ('success', 'failed')),
                    error_message TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (notification_id) REFERENCES notification_history(id)
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_device_tokens_active ON device_tokens (is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_history_type ON notification_history (notification_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_history_date ON notification_history (sent_at)')
            
            conn.commit()
            logger.info("Push notification tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating push notification tables: {e}")
            raise
        finally:
            try:
                conn.close()
            except:
                pass
    
    def register_device(self, device_id, fcm_token, platform='android'):
        """Register or update a device token"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO device_tokens (device_id, fcm_token, platform, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(device_id) DO UPDATE SET
                    fcm_token = excluded.fcm_token,
                    platform = excluded.platform,
                    is_active = 1,
                    updated_at = CURRENT_TIMESTAMP
            ''', (device_id, fcm_token, platform))
            
            conn.commit()
            logger.info(f"Device registered: {device_id} ({platform})")
            return True
            
        except Exception as e:
            logger.error(f"Error registering device: {e}")
            return False
        finally:
            try:
                conn.close()
            except:
                pass
    
    def unregister_device(self, device_id):
        """Deactivate a device"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE device_tokens 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE device_id = ?
            ''', (device_id,))
            
            conn.commit()
            logger.info(f"Device unregistered: {device_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error unregistering device: {e}")
            return False
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_active_tokens(self):
        """Get all active device tokens"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT device_id, fcm_token, platform 
                FROM device_tokens 
                WHERE is_active = 1 AND notification_enabled = 1
            ''')
            
            tokens = [
                {'device_id': row[0], 'token': row[1], 'platform': row[2]}
                for row in cursor.fetchall()
            ]
            
            return tokens
            
        except Exception as e:
            logger.error(f"Error getting active tokens: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def send_ipo_notification(self, ipo_data, is_single=True):
        """Send IPO notification to all active devices"""
        if not self.fcm_initialized:
            logger.warning("FCM not initialized - cannot send notifications")
            return {'success': False, 'error': 'FCM not initialized'}
        
        try:
            tokens = self.get_active_tokens()
            if not tokens:
                logger.info("No active device tokens to send notifications")
                return {'success': True, 'sent': 0, 'message': 'No active devices'}
            
            # Build notification
            if is_single:
                title = f"IPO Open: {ipo_data['company_name']}"
                body = self._build_notification_body(ipo_data)
                data = {
                    'type': 'ipo_open',
                    'company_name': ipo_data['company_name'],
                    'symbol': ipo_data.get('symbol', ''),
                    'share_type': ipo_data.get('share_type', 'Ordinary'),
                    'price': str(ipo_data.get('price', 0)),
                    'open_date': ipo_data.get('open_date', ''),
                    'close_date': ipo_data.get('close_date', '')
                }
            else:
                count = len(ipo_data)
                title = f"{count} IPOs Open with Ordinary Shares"
                companies = ', '.join([ipo['company_name'] for ipo in ipo_data[:3]])
                if count > 3:
                    body = f"{companies} and {count - 3} more"
                else:
                    body = companies
                data = {
                    'type': 'multiple_ipos',
                    'count': str(count),
                    'companies': json.dumps([ipo['company_name'] for ipo in ipo_data])
                }
            
            # Send to all devices
            success_count = 0
            failure_count = 0
            
            for token_info in tokens:
                try:
                    message = messaging.Message(
                        notification=messaging.Notification(
                            title=title,
                            body=body
                        ),
                        data=data,
                        token=token_info['token'],
                        android=messaging.AndroidConfig(
                            priority='high',
                            notification=messaging.AndroidNotification(
                                channel_id='ipo_ordinary_channel',
                                color='#2196F3',
                                sound='default'
                            )
                        ),
                        apns=messaging.APNSConfig(
                            payload=messaging.APNSPayload(
                                aps=messaging.Aps(
                                    alert=messaging.ApsAlert(
                                        title=title,
                                        body=body
                                    ),
                                    sound='default',
                                    badge=1
                                )
                            )
                        )
                    )
                    
                    response = messaging.send(message)
                    success_count += 1
                    logger.info(f"Notification sent to {token_info['device_id']}: {response}")
                    
                except Exception as e:
                    failure_count += 1
                    logger.error(f"Failed to send to {token_info['device_id']}: {e}")
                    
                    # If token is invalid, deactivate it
                    if 'invalid' in str(e).lower() or 'not-found' in str(e).lower():
                        self.unregister_device(token_info['device_id'])
            
            # Log notification history
            self._log_notification(
                notification_type='ipo_open',
                title=title,
                body=body,
                data=json.dumps(data),
                company_name=ipo_data['company_name'] if is_single else 'Multiple',
                sent_to_count=len(tokens),
                success_count=success_count,
                failure_count=failure_count
            )
            
            return {
                'success': True,
                'sent': len(tokens),
                'success_count': success_count,
                'failure_count': failure_count
            }
            
        except Exception as e:
            logger.error(f"Error sending IPO notification: {e}")
            return {'success': False, 'error': str(e)}
    
    def _build_notification_body(self, ipo):
        """Build notification body for single IPO"""
        parts = []
        
        if ipo.get('symbol'):
            parts.append(f"Symbol: {ipo['symbol']}")
        if ipo.get('price'):
            parts.append(f"Price: Rs. {ipo['price']}")
        if ipo.get('close_date'):
            parts.append(f"Closes: {ipo['close_date']}")
        
        return ' • '.join(parts) if parts else 'Ordinary shares available'
    
    def _log_notification(self, notification_type, title, body, data, company_name, 
                         sent_to_count, success_count, failure_count):
        """Log notification to history"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO notification_history 
                (notification_type, title, body, data, company_name, 
                 sent_to_count, success_count, failure_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (notification_type, title, body, data, company_name,
                  sent_to_count, success_count, failure_count))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error logging notification: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_notification_history(self, limit=50):
        """Get notification history"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM notification_history 
                ORDER BY sent_at DESC 
                LIMIT ?
            ''', (limit,))
            
            columns = [desc[0] for desc in cursor.description]
            history = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting notification history: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_device_count(self):
        """Get count of active devices"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT COUNT(*) FROM device_tokens 
                WHERE is_active = 1 AND notification_enabled = 1
            ''')
            
            result = cursor.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting device count: {e}")
            return 0
        finally:
            try:
                conn.close()
            except:
                pass