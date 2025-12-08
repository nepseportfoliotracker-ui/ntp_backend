# routes_ema_notifications.py - API Routes for EMA Signal Notifications

from flask import jsonify, request
import logging

logger = logging.getLogger(__name__)


def register_ema_notification_routes(app):
    """Register EMA signal notification routes"""
    
    ema_notification_service = app.config.get('ema_notification_service')
    require_auth = app.config.get('require_auth')
    require_admin = app.config.get('require_admin')
    
    @app.route('/api/ema-signals/notifications/send', methods=['POST'])
    @require_admin
    def trigger_ema_notification():
        """
        Manually trigger EMA signal notification (Admin only)
        Useful for testing or manual sends
        """
        try:
            if not ema_notification_service:
                return jsonify({
                    'success': False,
                    'error': 'EMA notification service not available'
                }), 503
            
            result = ema_notification_service.check_and_notify_latest_signal()
            
            if result['success']:
                if result.get('already_sent'):
                    return jsonify({
                        'success': True,
                        'message': 'Notification already sent for latest signal',
                        'data': result
                    }), 200
                else:
                    return jsonify({
                        'success': True,
                        'message': 'EMA signal notification sent successfully',
                        'data': result
                    }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': result.get('error', 'Failed to send notification'),
                    'data': result
                }), 500
                
        except Exception as e:
            logger.error(f"Failed to trigger EMA notification: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/notifications/test', methods=['POST'])
    @require_admin
    def send_test_ema_notification():
        """
        Send a test EMA notification (Admin only)
        Sends test notification to all registered devices
        """
        try:
            if not ema_notification_service:
                return jsonify({
                    'success': False,
                    'error': 'EMA notification service not available'
                }), 503
            
            result = ema_notification_service.send_test_notification()
            
            if result['success']:
                return jsonify({
                    'success': True,
                    'message': 'Test notification sent successfully',
                    'data': result
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': result.get('error', 'Failed to send test notification')
                }), 500
                
        except Exception as e:
            logger.error(f"Failed to send test notification: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/notifications/history', methods=['GET'])
    @require_auth
    def get_ema_notification_history():
        """
        Get EMA signal notification history
        
        Query params:
        - limit: Number of records to return (default: 50)
        """
        try:
            if not ema_notification_service:
                return jsonify({
                    'success': False,
                    'error': 'EMA notification service not available'
                }), 503
            
            limit = request.args.get('limit', 50, type=int)
            history = ema_notification_service.get_notification_history(limit=limit)
            
            return jsonify({
                'success': True,
                'count': len(history),
                'data': history
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get notification history: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/notifications/stats', methods=['GET'])
    @require_auth
    def get_ema_notification_stats():
        """
        Get EMA signal notification statistics
        
        Returns:
        {
            "success": true,
            "data": {
                "total_notifications": 45,
                "total_devices_notified": 1250,
                "last_notification_sent": "2024-12-07 15:15:00",
                "last_signal_type": "BUY",
                "fcm_initialized": true,
                "active_devices": 28
            }
        }
        """
        try:
            if not ema_notification_service:
                return jsonify({
                    'success': False,
                    'error': 'EMA notification service not available'
                }), 503
            
            stats = ema_notification_service.get_notification_stats()
            
            return jsonify({
                'success': True,
                'data': stats
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get notification stats: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/notifications/reset/<date>', methods=['POST'])
    @require_admin
    def reset_ema_notification(date):
        """
        Reset notification status for a specific date (Admin only)
        Used for testing - allows resending notification for a date
        
        Path param:
        - date: Signal date in YYYY-MM-DD format
        """
        try:
            if not ema_notification_service:
                return jsonify({
                    'success': False,
                    'error': 'EMA notification service not available'
                }), 503
            
            success = ema_notification_service.reset_notification_for_date(date)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f'Notification status reset for {date}',
                    'date': date
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to reset notification status'
                }), 500
                
        except Exception as e:
            logger.error(f"Failed to reset notification: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    logger.info("EMA notification routes registered successfully")