# ema_notification_service.py - Push Notifications for EMA Trading Signals
# Integrated with existing push_notification_service.py

import logging
import json
from datetime import datetime
from firebase_admin import messaging

logger = logging.getLogger(__name__)


class EMANotificationService:
    """
    Service to send push notifications for EMA trading signals
    Uses the existing PushNotificationService infrastructure
    """
    
    def __init__(self, db_service, push_service, ema_signal_service):
        self.db_service = db_service
        self.push_service = push_service
        self.ema_signal_service = ema_signal_service
        self._init_notification_table()
    
    def _init_notification_table(self):
        """Initialize table to track sent EMA signal notifications"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ema_signal_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_date DATE NOT NULL,
                    signal_type TEXT NOT NULL,
                    signal_price REAL NOT NULL,
                    ema_value REAL NOT NULL,
                    can_trade INTEGER DEFAULT 1,
                    holding_period_active INTEGER DEFAULT 0,
                    notification_sent INTEGER DEFAULT 0,
                    devices_notified INTEGER DEFAULT 0,
                    sent_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(signal_date)
                )
            ''')
            
            conn.commit()
            logger.info("EMA notification table initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize EMA notification table: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def check_and_notify_latest_signal(self):
        """
        Check for new EMA signal and send push notifications if needed
        Called automatically after signal generation
        
        Returns dict with notification results
        """
        try:
            logger.info("=== Checking for new EMA signals to notify ===")
            
            # Get latest signal
            latest_signal = self.ema_signal_service.get_latest_signal()
            
            if not latest_signal:
                logger.info("No EMA signals available yet")
                return {
                    'success': False,
                    'error': 'No signals available',
                    'notified': 0
                }
            
            signal_date = latest_signal['date']
            signal_type = latest_signal['signal']
            signal_price = latest_signal['price']
            ema_value = latest_signal['ema']
            can_trade = latest_signal['can_trade']
            holding_period_active = latest_signal['holding_period_active']
            
            # Check if we've already sent notification for this signal
            if self._is_notification_sent(signal_date):
                logger.info(f"Notification already sent for signal on {signal_date}")
                return {
                    'success': True,
                    'already_sent': True,
                    'signal_date': signal_date,
                    'notified': 0
                }
            
            # Prepare notification based on signal type
            notification_data = self._prepare_notification(
                signal_type, 
                signal_price, 
                ema_value,
                signal_date,
                can_trade,
                holding_period_active
            )
            
            # Send push notifications using existing infrastructure
            result = self._send_ema_broadcast(
                title=notification_data['title'],
                body=notification_data['body'],
                signal_type=signal_type,
                signal_date=signal_date,
                price=signal_price,
                ema=ema_value,
                can_trade=can_trade,
                holding_period_active=holding_period_active
            )
            
            if result['success']:
                # Record that notification was sent
                self._record_notification_sent(
                    signal_date,
                    signal_type,
                    signal_price,
                    ema_value,
                    can_trade,
                    holding_period_active,
                    result.get('success_count', 0)
                )
                
                # Also log in existing notification_history table
                self.push_service._log_notification(
                    notification_type='ema_signal',
                    title=notification_data['title'],
                    body=notification_data['body'],
                    data=json.dumps({
                        'type': 'ema_signal',
                        'signal_type': signal_type,
                        'signal_date': signal_date,
                        'price': str(signal_price),
                        'ema': str(ema_value)
                    }),
                    company_name='NEPSE Index',
                    sent_to_count=result.get('sent', 0),
                    success_count=result.get('success_count', 0),
                    failure_count=result.get('failure_count', 0)
                )
                
                logger.info(f"EMA signal notification sent successfully")
                logger.info(f"  Signal: {signal_type} on {signal_date}")
                logger.info(f"  Price: {signal_price}, EMA: {ema_value}")
                logger.info(f"  Devices notified: {result.get('success_count', 0)}")
                
                return {
                    'success': True,
                    'signal_date': signal_date,
                    'signal_type': signal_type,
                    'notified': result.get('success_count', 0),
                    'failed': result.get('failure_count', 0)
                }
            else:
                logger.error(f"Failed to send EMA signal notifications: {result.get('error')}")
                return {
                    'success': False,
                    'error': result.get('error', 'Notification failed'),
                    'notified': 0
                }
                
        except Exception as e:
            logger.error(f"Error checking and notifying EMA signals: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'notified': 0
            }
    
    def _send_ema_broadcast(self, title, body, signal_type, signal_date, price, ema, 
                            can_trade, holding_period_active):
        """
        Send EMA signal notification to all active devices
        Uses existing FCM infrastructure
        """
        if not self.push_service.fcm_initialized:
            logger.warning("FCM not initialized - cannot send notifications")
            return {'success': False, 'error': 'FCM not initialized'}
        
        try:
            # Get all active device tokens from existing table
            tokens = self.push_service.get_active_tokens()
            
            if not tokens:
                logger.info("No active device tokens to send notifications")
                return {'success': True, 'sent': 0, 'success_count': 0, 'failure_count': 0}
            
            # Build notification data - ALL VALUES MUST BE STRINGS
            data = {
                'type': 'ema_signal',
                'signal_type': str(signal_type),
                'signal_date': str(signal_date),
                'price': str(price),
                'ema': str(ema),
                'can_trade': str(can_trade),
                'holding_period_active': str(holding_period_active),
                'screen': 'ema_trading_signals'
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
                                channel_id='ema_signal_channel',
                                color='#2196F3' if signal_type == 'BUY' else '#F44336',
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
                    logger.info(f"EMA notification sent to {token_info['device_id']}: {response}")
                    
                except Exception as e:
                    failure_count += 1
                    logger.error(f"Failed to send EMA notification to {token_info['device_id']}: {e}")
                    
                    # If token is invalid, deactivate it
                    if 'invalid' in str(e).lower() or 'not-found' in str(e).lower():
                        self.push_service.unregister_device(token_info['device_id'])
            
            return {
                'success': True,
                'sent': len(tokens),
                'success_count': success_count,
                'failure_count': failure_count
            }
            
        except Exception as e:
            logger.error(f"Error sending EMA broadcast notification: {e}")
            return {'success': False, 'error': str(e)}
    
    def _prepare_notification(self, signal_type, price, ema, date, can_trade, holding_period_active):
        """Prepare notification title and body based on signal type"""
        
        signal_upper = signal_type.upper()
        
        if signal_upper == 'BUY':
            emoji = '📈'
            action = 'BUY'
            description = f'NEPSE crossed above 4-day EMA at NPR {price:.2f}'
            color = 'green'
        elif signal_upper == 'SELL':
            emoji = '📉'
            action = 'SELL'
            description = f'NEPSE crossed below 4-day EMA at NPR {price:.2f}'
            color = 'red'
        else:  # HOLD
            emoji = '⏸️'
            action = 'HOLD'
            if holding_period_active:
                description = 'Holding period active - maintain position'
            else:
                description = f'No clear crossover at NPR {price:.2f}'
            color = 'orange'
        
        title = f"{emoji} {action} Signal"
        
        if can_trade:
            body = f"{description} • EMA: NPR {ema:.2f}"
        else:
            body = f"Holding period active • NEPSE: NPR {price:.2f} • EMA: NPR {ema:.2f}"
        
        return {
            'title': title,
            'body': body,
            'emoji': emoji,
            'action': action,
            'color': color
        }
    
    def _is_notification_sent(self, signal_date):
        """Check if notification was already sent for this signal date"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT notification_sent 
                FROM ema_signal_notifications 
                WHERE signal_date = ?
            ''', (signal_date,))
            
            result = cursor.fetchone()
            
            if result and result[0] == 1:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking notification status: {e}")
            return False
        finally:
            conn.close()
    
    def _record_notification_sent(self, signal_date, signal_type, price, ema, can_trade, 
                                   holding_period_active, devices_count):
        """Record that notification was sent for this signal"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO ema_signal_notifications
                (signal_date, signal_type, signal_price, ema_value, can_trade, 
                 holding_period_active, notification_sent, devices_notified, sent_at)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
            ''', (
                signal_date,
                signal_type,
                price,
                ema,
                int(can_trade),
                int(holding_period_active),
                devices_count
            ))
            
            conn.commit()
            logger.info(f"Recorded EMA notification sent for {signal_date}")
            
        except Exception as e:
            logger.error(f"Error recording notification: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_notification_history(self, limit=50):
        """Get history of sent EMA signal notifications"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT signal_date, signal_type, signal_price, ema_value, 
                       can_trade, holding_period_active, devices_notified, sent_at
                FROM ema_signal_notifications
                WHERE notification_sent = 1
                ORDER BY signal_date DESC
                LIMIT ?
            ''', (limit,))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'signal_date': row[0],
                    'signal_type': row[1],
                    'signal_price': row[2],
                    'ema_value': row[3],
                    'can_trade': bool(row[4]),
                    'holding_period_active': bool(row[5]),
                    'devices_notified': row[6],
                    'sent_at': row[7]
                })
            
            return history
            
        except Exception as e:
            logger.error(f"Error fetching notification history: {e}")
            return []
        finally:
            conn.close()
    
    def get_notification_stats(self):
        """Get statistics about EMA signal notifications"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_notifications,
                    SUM(devices_notified) as total_devices_notified,
                    MAX(sent_at) as last_notification_sent,
                    (SELECT signal_type FROM ema_signal_notifications 
                     WHERE notification_sent = 1 
                     ORDER BY signal_date DESC LIMIT 1) as last_signal_type
                FROM ema_signal_notifications
                WHERE notification_sent = 1
            ''')
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'total_notifications': row[0] or 0,
                    'total_devices_notified': row[1] or 0,
                    'last_notification_sent': row[2],
                    'last_signal_type': row[3],
                    'fcm_initialized': self.push_service.fcm_initialized,
                    'active_devices': self.push_service.get_device_count()
                }
            
            return {
                'total_notifications': 0,
                'total_devices_notified': 0,
                'last_notification_sent': None,
                'last_signal_type': None,
                'fcm_initialized': self.push_service.fcm_initialized,
                'active_devices': self.push_service.get_device_count()
            }
            
        except Exception as e:
            logger.error(f"Error fetching notification stats: {e}")
            return {
                'total_notifications': 0,
                'total_devices_notified': 0,
                'last_notification_sent': None,
                'last_signal_type': None,
                'fcm_initialized': False,
                'active_devices': 0
            }
        finally:
            conn.close()
    
    def reset_notification_for_date(self, signal_date):
        """Reset notification status for a specific date (for testing)"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE ema_signal_notifications
                SET notification_sent = 0, devices_notified = 0, sent_at = NULL
                WHERE signal_date = ?
            ''', (signal_date,))
            
            conn.commit()
            logger.info(f"Reset notification status for {signal_date}")
            return True
            
        except Exception as e:
            logger.error(f"Error resetting notification: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def send_test_notification(self):
        """Send a test EMA signal notification (for testing purposes)"""
        if not self.push_service.fcm_initialized:
            return {'success': False, 'error': 'FCM not initialized'}
        
        try:
            latest_signal = self.ema_signal_service.get_latest_signal()
            
            if not latest_signal:
                return {'success': False, 'error': 'No signals available'}
            
            notification_data = self._prepare_notification(
                latest_signal['signal'],
                latest_signal['price'],
                latest_signal['ema'],
                latest_signal['date'],
                latest_signal['can_trade'],
                latest_signal['holding_period_active']
            )
            
            result = self._send_ema_broadcast(
                title=f"[TEST] {notification_data['title']}",
                body=notification_data['body'],
                signal_type=latest_signal['signal'],
                signal_date=latest_signal['date'],
                price=latest_signal['price'],
                ema=latest_signal['ema'],
                can_trade=latest_signal['can_trade'],
                holding_period_active=latest_signal['holding_period_active']
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error sending test notification: {e}")
            return {'success': False, 'error': str(e)}