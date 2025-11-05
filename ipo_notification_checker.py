# ipo_notification_checker.py - Backend IPO notification checker (Daily at 5:00 PM)

import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

class IPONotificationChecker:
    """Check for ordinary share IPOs and send push notifications daily at 5:00 PM"""
    
    def __init__(self, ipo_service, push_notification_service, db_service):
        self.ipo_service = ipo_service
        self.push_service = push_notification_service
        self.db_service = db_service
        self._create_tracking_table()
    
    def _create_tracking_table(self):
        """Create table to track notification history"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ipo_notification_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_date DATE NOT NULL,
                    notification_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ipo_count INTEGER DEFAULT 0,
                    company_names TEXT,
                    devices_notified INTEGER DEFAULT 0,
                    success INTEGER DEFAULT 1
                )
            ''')
            
            conn.commit()
            logger.info("IPO notification history table created")
            
        except Exception as e:
            logger.error(f"Error creating tracking table: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def is_ordinary_share(self, issue):
        """Check if an issue has ordinary share type"""
        if not issue.get('share_type'):
            return False
        
        share_type = issue['share_type'].lower().strip()
        return any(keyword in share_type for keyword in ['ordinary', 'common', 'ord'])
    
    def record_notification(self, ipo_count, company_names, devices_notified, success):
        """Record notification history"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            today = date.today().isoformat()
            companies_str = ', '.join(company_names) if company_names else ''
            
            cursor.execute('''
                INSERT INTO ipo_notification_history
                (notification_date, ipo_count, company_names, devices_notified, success)
                VALUES (?, ?, ?, ?, ?)
            ''', (today, ipo_count, companies_str, devices_notified, int(success)))
            
            conn.commit()
            logger.info(f"Recorded notification: {ipo_count} IPOs, {devices_notified} devices")
            
        except Exception as e:
            logger.error(f"Error recording notification: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_today_notification_count(self):
        """Get count of notifications sent today"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            today = date.today().isoformat()
            
            cursor.execute('''
                SELECT COUNT(*) FROM ipo_notification_history
                WHERE notification_date = ?
            ''', (today,))
            
            result = cursor.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting today's notification count: {e}")
            return 0
        finally:
            try:
                conn.close()
            except:
                pass
    
    def check_and_notify(self):
        """
        Check for open ordinary share IPOs and send notifications.
        This runs daily at 5:00 PM, sending notifications for all open IPOs.
        """
        try:
            logger.info("=== IPO Notification Check Started (5:00 PM) ===")
            
            # Get all open IPOs
            open_ipos = self.ipo_service.get_open_issues('IPO')
            
            if not open_ipos:
                logger.info("No open IPOs found")
                self.record_notification(0, [], 0, True)
                return {
                    'success': True,
                    'checked': 0,
                    'ordinary_ipos': 0,
                    'notified': 0,
                    'message': 'No open IPOs'
                }
            
            logger.info(f"Found {len(open_ipos)} open IPOs")
            
            # Filter for ordinary shares only
            ordinary_ipos = [ipo for ipo in open_ipos if self.is_ordinary_share(ipo)]
            
            if not ordinary_ipos:
                logger.info("No ordinary share IPOs found")
                self.record_notification(0, [], 0, True)
                return {
                    'success': True,
                    'checked': len(open_ipos),
                    'ordinary_ipos': 0,
                    'notified': 0,
                    'message': 'No ordinary share IPOs'
                }
            
            logger.info(f"Found {len(ordinary_ipos)} ordinary share IPOs")
            logger.info(f"Companies: {[ipo['company_name'] for ipo in ordinary_ipos]}")
            
            # Send notifications (always send, regardless of previous notifications)
            if len(ordinary_ipos) == 1:
                result = self.push_service.send_ipo_notification(ordinary_ipos[0], is_single=True)
            else:
                result = self.push_service.send_ipo_notification(ordinary_ipos, is_single=False)
            
            # Record notification
            company_names = [ipo['company_name'] for ipo in ordinary_ipos]
            devices_notified = result.get('success_count', 0)
            
            self.record_notification(
                len(ordinary_ipos),
                company_names,
                devices_notified,
                result.get('success', False)
            )
            
            if result.get('success'):
                logger.info(f"Successfully sent notifications for {len(ordinary_ipos)} IPOs to {devices_notified} devices")
                
                return {
                    'success': True,
                    'checked': len(open_ipos),
                    'ordinary_ipos': len(ordinary_ipos),
                    'notified': devices_notified,
                    'failed': result.get('failure_count', 0),
                    'companies': company_names,
                    'message': f'Sent notifications for {len(ordinary_ipos)} IPOs'
                }
            else:
                logger.error(f"Failed to send notifications: {result.get('error')}")
                return {
                    'success': False,
                    'error': result.get('error'),
                    'checked': len(open_ipos),
                    'ordinary_ipos': len(ordinary_ipos)
                }
            
        except Exception as e:
            logger.error(f"Error in IPO notification check: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_notification_stats(self):
        """Get notification statistics"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            today = date.today().isoformat()
            
            # Get today's notification info
            cursor.execute('''
                SELECT ipo_count, company_names, devices_notified, notification_time
                FROM ipo_notification_history
                WHERE notification_date = ?
                ORDER BY notification_time DESC
                LIMIT 1
            ''', (today,))
            
            today_row = cursor.fetchone()
            today_info = None
            if today_row:
                today_info = {
                    'ipo_count': today_row[0],
                    'companies': today_row[1],
                    'devices_notified': today_row[2],
                    'time': today_row[3]
                }
            
            # Get active device count
            device_count = self.push_service.get_device_count()
            
            # Get recent notification history (last 10 days)
            cursor.execute('''
                SELECT notification_date, ipo_count, company_names, devices_notified
                FROM ipo_notification_history
                ORDER BY notification_date DESC, notification_time DESC
                LIMIT 10
            ''')
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'date': row[0],
                    'ipo_count': row[1],
                    'companies': row[2],
                    'devices_notified': row[3]
                })
            
            return {
                'today_notification': today_info,
                'active_devices': device_count,
                'recent_history': history,
                'fcm_initialized': self.push_service.fcm_initialized,
                'last_check': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting notification stats: {e}")
            return {
                'error': str(e)
            }
        finally:
            try:
                conn.close()
            except:
                pass