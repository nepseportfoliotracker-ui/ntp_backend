# routes_push.py - Push Notification Routes

import logging
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_push_notification_routes(app):
    """Register push notification routes"""
    
    # Get decorators from app config
    require_auth = app.config['require_auth']
    
    @app.route('/api/push-notification/register', methods=['POST'])
    @require_auth
    def register_push_device():
        """Register device for push notifications"""
        try:
            push_service = app.config['push_service']
            data = request.get_json()
            device_id = data.get('device_id')
            fcm_token = data.get('fcm_token')
            platform = data.get('platform', 'android')
            
            if not device_id or not fcm_token:
                return jsonify({
                    'success': False,
                    'error': 'device_id and fcm_token are required',
                    'flutter_ready': True
                }), 400
            
            success = push_service.register_device(device_id, fcm_token, platform)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Device registered successfully',
                    'device_id': device_id,
                    'platform': platform,
                    'flutter_ready': True
                }), 201
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to register device',
                    'flutter_ready': True
                }), 500
                
        except Exception as e:
            logger.error(f"Error registering push device: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500

    @app.route('/api/push-notification/unregister', methods=['POST'])
    @require_auth
    def unregister_push_device():
        """Unregister device from push notifications"""
        try:
            push_service = app.config['push_service']
            data = request.get_json()
            device_id = data.get('device_id')
            
            if not device_id:
                return jsonify({
                    'success': False,
                    'error': 'device_id is required',
                    'flutter_ready': True
                }), 400
            
            success = push_service.unregister_device(device_id)
            
            return jsonify({
                'success': success,
                'message': 'Device unregistered successfully' if success else 'Failed to unregister',
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error unregistering push device: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500

    @app.route('/api/push-notification/history', methods=['GET'])
    @require_auth
    def get_push_notification_history():
        """Get push notification history"""
        try:
            push_service = app.config['push_service']
            limit = min(int(request.args.get('limit', 20)), 100)
            history = push_service.get_notification_history(limit)
            
            return jsonify({
                'success': True,
                'history': history,
                'count': len(history),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error getting notification history: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500

    @app.route('/api/push-notification/stats', methods=['GET'])
    @require_auth
    def get_push_notification_stats():
        """Get push notification statistics"""
        try:
            notification_checker = app.config['notification_checker']
            stats = notification_checker.get_notification_stats()
            
            return jsonify({
                'success': True,
                'stats': stats,
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error getting notification stats: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500