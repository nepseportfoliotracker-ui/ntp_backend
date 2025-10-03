# ipo_notification_checker.py - Backend IPO notification checker

import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

class IPONotificationChecker:
    """Check for ordinary share IPOs and send push notifications"""
    
    def __init__(self, ipo_service, push_notification_service, db_service):
        self.ipo_service = ipo_service
        self.push_service = push_notification_service
        self.db_service = db_service
        self._create_tracking_table()
    
    def _create_tracking_table(self):
        """Create table to track notified IPOs"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ipo_notification_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL,
                    notification_date DATE NOT NULL,
                    notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_name, notification_date)
                )
            ''')
            
            conn.commit()
            logger.info("IPO notification tracking table created")
            
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
    
    def has_been_notified_today(self, company_name):
        """Check if company has been notified today"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            today = date.today().isoformat()
            
            cursor.execute('''
                SELECT COUNT(*) FROM ipo_notification_tracking
                WHERE company_name = ? AND notification_date = ?
            ''', (company_name, today))
            
            result = cursor.fetchone()
            return result[0] > 0 if result else False
            
        except Exception as e:
            logger.error(f"Error checking notification status: {e}")
            return False
        finally:
            try:
                conn.close()
            except:
                pass
    
    def mark_as_notified(self, company_name):
        """Mark company as notified today"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            today = date.today().isoformat()
            
            cursor.execute('''
                INSERT OR IGNORE INTO ipo_notification_tracking
                (company_name, notification_date)
                VALUES (?, ?)
            ''', (company_name, today))
            
            conn.commit()
            logger.info(f"Marked {company_name} as notified for {today}")
            
        except Exception as e:
            logger.error(f"Error marking as notified: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def check_and_notify(self):
        """Check for open ordinary share IPOs and send notifications"""
        try:
            logger.info("=== IPO Notification Check Started ===")
            
            # Get all open IPOs
            open_ipos = self.ipo_service.get_open_issues('IPO')
            
            if not open_ipos:
                logger.info("No open IPOs found")
                return {
                    'success': True,
                    'checked': 0,
                    'ordinary_ipos': 0,
                    'notified': 0
                }
            
            logger.info(f"Found {len(open_ipos)} open IPOs")
            
            # Filter for ordinary shares
            ordinary_ipos = [ipo for ipo in open_ipos if self.is_ordinary_share(ipo)]
            
            if not ordinary_ipos:
                logger.info("No ordinary share IPOs found")
                return {
                    'success': True,
                    'checked': len(open_ipos),
                    'ordinary_ipos': 0,
                    'notified': 0
                }
            
            logger.info(f"Found {len(ordinary_ipos)} ordinary share IPOs")
            
            # Filter out already notified IPOs
            new_ipos = [
                ipo for ipo in ordinary_ipos 
                if not self.has_been_notified_today(ipo['company_name'])
            ]
            
            if not new_ipos:
                logger.info("All ordinary share IPOs already notified today")
                return {
                    'success': True,
                    'checked': len(open_ipos),
                    'ordinary_ipos': len(ordinary_ipos),
                    'notified': 0,
                    'message': 'All IPOs already notified today'
                }
            
            logger.info(f"Sending notifications for {len(new_ipos)} new ordinary share IPOs")
            
            # Send notifications
            if len(new_ipos) == 1:
                result = self.push_service.send_ipo_notification(new_ipos[0], is_single=True)
            else:
                result = self.push_service.send_ipo_notification(new_ipos, is_single=False)
            
            # Mark as notified
            if result.get('success'):
                for ipo in new_ipos:
                    self.mark_as_notified(ipo['company_name'])
                
                logger.info(f"Successfully sent notifications for {len(new_ipos)} IPOs")
                
                return {
                    'success': True,
                    'checked': len(open_ipos),
                    'ordinary_ipos': len(ordinary_ipos),
                    'new_ipos': len(new_ipos),
                    'notified': result.get('success_count', 0),
                    'failed': result.get('failure_count', 0),
                    'companies': [ipo['company_name'] for ipo in new_ipos]
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
            
            # Count today's notifications
            cursor.execute('''
                SELECT COUNT(*) FROM ipo_notification_tracking
                WHERE notification_date = ?
            ''', (today,))
            
            today_count = cursor.fetchone()[0] if cursor.fetchone() else 0
            
            # Get active device count
            device_count = self.push_service.get_device_count()
            
            # Get recent notification history
            history = self.push_service.get_notification_history(limit=10)
            
            return {
                'notified_today': today_count,
                'active_devices': device_count,
                'recent_notifications': history,
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