# routes_admin.py - Admin Routes

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_admin_routes(app):
    """Register admin routes"""
    
    # Get decorators from app config
    require_auth = app.config['require_auth']
    require_admin = app.config['require_admin']
    
    @app.route('/api/trigger-scrape', methods=['POST'])
    @require_auth
    def trigger_scrape():
        """Manually trigger scraping"""
        try:
            scraping_service = app.config['scraping_service']
            data = request.get_json() or {}
            force = data.get('force', True)
            scrape_type = data.get('type', 'all')
            
            results = {}
            
            if scrape_type in ['stocks', 'all']:
                stock_count = scraping_service.scrape_all_sources(force=force)
                results['stocks'] = stock_count
            
            if scrape_type in ['issues', 'ipos', 'all']:
                ipo_count = scraping_service.scrape_ipo_sources(force=force)
                results['issues'] = ipo_count
            
            total_count = sum(results.values())
            
            return jsonify({
                'success': True,
                'message': f'Scraping completed. {total_count} total items updated.',
                'results': results,
                'total_count': total_count,
                'scrape_type': scrape_type,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            }), 201
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/key-info', methods=['GET'])
    @require_auth
    def get_key_info():
        """Get information about the authenticated key"""
        try:
            auth_service = app.config['auth_service']
            key_info = auth_service.get_key_details(request.auth_info['key_id'])
            if key_info:
                return jsonify({
                    'success': True,
                    'key_info': key_info,
                    'flutter_ready': True
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Key information not found',
                    'flutter_ready': True
                }), 404
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/admin/generate-key', methods=['POST'])
    @require_auth
    @require_admin
    def admin_generate_key():
        """Generate new API key (admin only)"""
        try:
            auth_service = app.config['auth_service']
            data = request.get_json() or {}
            key_type = data.get('key_type', 'regular')
            description = data.get('description', '')
            
            if key_type not in ['admin', 'regular']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid key type',
                    'flutter_ready': True
                }), 400
            
            key_pair = auth_service.generate_api_key(
                key_type=key_type,
                created_by=request.auth_info['key_id'],
                description=description
            )
            
            if key_pair:
                return jsonify({
                    'success': True,
                    'key_pair': key_pair,
                    'flutter_ready': True
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to generate key',
                    'flutter_ready': True
                }), 500
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/admin/list-keys', methods=['GET'])
    @require_auth
    @require_admin
    def admin_list_keys():
        """List all API keys (admin only)"""
        try:
            auth_service = app.config['auth_service']
            logger.info(f"Admin list keys request from: {request.auth_info['key_id']}")
            keys = auth_service.list_all_keys()
            
            logger.info(f"Found {len(keys)} keys")
            
            return jsonify({
                'success': True,
                'keys': keys,
                'count': len(keys),
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error listing keys: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/admin/keys/<key_id>/delete', methods=['DELETE'])
    @require_auth
    @require_admin
    def admin_delete_key(key_id):
        """Delete an API key (admin only)"""
        try:
            auth_service = app.config['auth_service']
            logger.info(f"Delete key request for {key_id} from: {request.auth_info['key_id']}")
            
            # Prevent deleting own key
            if key_id == request.auth_info['key_id']:
                return jsonify({
                    'success': False,
                    'error': 'Cannot delete your own key',
                    'flutter_ready': True
                }), 400
            
            # Deactivate the key
            success = auth_service.deactivate_key(key_id)
            
            if success:
                logger.info(f"Key {key_id} deleted successfully")
                return jsonify({
                    'success': True,
                    'message': f'Key {key_id} deleted successfully',
                    'flutter_ready': True
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to delete key - key may not exist',
                    'flutter_ready': True
                }), 404
        except Exception as e:
            logger.error(f"Error deleting key: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500

    @app.route('/api/admin/stats', methods=['GET'])
    @require_auth
    @require_admin
    def admin_get_stats():
        """Get system statistics (admin only)"""
        try:
            services = {
                'auth_service': app.config['auth_service'],
                'db_service': app.config['db_service'],
                'price_service': app.config['price_service'],
                'ipo_service': app.config['ipo_service'],
                'smart_scheduler': app.config['smart_scheduler'],
                'notification_checker': app.config['notification_checker']
            }
            
            usage_stats = services['auth_service'].get_usage_stats(days=1)
            total_requests_24h = sum(usage_stats.values()) if usage_stats else 0
            
            all_keys = services['auth_service'].list_all_keys()
            active_keys = len([k for k in all_keys if k['is_active']])
            
            active_sessions = 0
            try:
                conn = services['db_service'].get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM device_sessions WHERE is_active = 1')
                result = cursor.fetchone()
                active_sessions = result[0] if result else 0
                conn.close()
            except Exception as e:
                logger.warning(f"Error counting sessions: {e}")
            
            stock_count = services['price_service'].get_stock_count()
            issue_stats = services['ipo_service'].get_statistics()
            scheduler_status = services['smart_scheduler'].get_scheduler_status()
            
            # Push notification stats
            push_stats = services['notification_checker'].get_notification_stats()
            
            stats = {
                'active_keys': active_keys,
                'total_keys': len(all_keys),
                'active_sessions': active_sessions,
                'requests_24h': total_requests_24h,
                'stock_count': stock_count,
                'issue_statistics': issue_stats['summary'],
                'issues_by_category': issue_stats['by_category'],
                'scheduler_status': scheduler_status,
                'push_notification_stats': push_stats,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            }
            
            return jsonify({
                'success': True,
                'stats': stats,
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/admin/scheduler/control', methods=['POST'])
    @require_auth
    @require_admin
    def admin_scheduler_control():
        """Control scheduler (admin only)"""
        try:
            smart_scheduler = app.config['smart_scheduler']
            data = request.get_json() or {}
            action = data.get('action', '').lower()
            
            if action not in ['start', 'stop', 'restart', 'force_scrape', 'force_ipo_check']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid action',
                    'flutter_ready': True
                }), 400
            
            if action == 'stop':
                smart_scheduler.stop()
                message = 'Scheduler stopped'
            elif action == 'start':
                if not smart_scheduler.scheduler.running:
                    smart_scheduler.start()
                    message = 'Scheduler started'
                else:
                    message = 'Scheduler already running'
            elif action == 'restart':
                smart_scheduler.stop()
                smart_scheduler.start()
                message = 'Scheduler restarted'
            elif action == 'force_scrape':
                smart_scheduler.scheduled_scrape()
                message = 'Force scrape executed'
            elif action == 'force_ipo_check':
                smart_scheduler.scheduled_ipo_check()
                message = 'Force IPO check executed'
            
            status = smart_scheduler.get_scheduler_status()
            
            return jsonify({
                'success': True,
                'message': message,
                'action': action,
                'scheduler_status': status,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/admin/trigger-ipo-check', methods=['POST'])
    @require_auth
    @require_admin
    def admin_trigger_ipo_check():
        """Manually trigger IPO notification check (admin only)"""
        try:
            notification_checker = app.config['notification_checker']
            result = notification_checker.check_and_notify()
            
            return jsonify({
                'success': result['success'],
                'result': result,
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error triggering IPO check: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500


def register_error_handlers(app):
    """Register error handlers"""
    
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({
            'success': False,
            'error': 'Endpoint not found',
            'flutter_ready': True
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {error}")
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'flutter_ready': True
        }), 500