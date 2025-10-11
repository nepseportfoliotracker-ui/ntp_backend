# routes.py - API Routes Registration (Main Entry Point)

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_all_routes(app):
    """Register all API routes"""
    
    # Import route modules
    from routes_stock import register_stock_routes
    from routes_issues import register_issue_routes
    from routes_push import register_push_notification_routes
    from routes_admin import register_admin_routes, register_error_handlers
    
    # Get decorators from app config
    require_auth = app.config['require_auth']
    require_admin = app.config['require_admin']
    
    # ==================== HEALTH AND STATUS ROUTES ====================
    
    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Health check endpoint with scheduler status"""
        try:
            services = {
                'price_service': app.config['price_service'],
                'ipo_service': app.config['ipo_service'],
                'scraping_service': app.config['scraping_service'],
                'smart_scheduler': app.config['smart_scheduler'],
                'push_service': app.config['push_service'],
                'db_service': app.config['db_service']
            }
            
            stock_count = services['price_service'].get_stock_count()
            market_status = services['price_service'].get_market_status()
            last_scrape = services['scraping_service'].get_last_scrape_time()
            last_ipo_scrape = services['scraping_service'].get_last_ipo_scrape_time()
            
            ipo_stats = services['ipo_service'].get_statistics()
            scheduler_status = services['smart_scheduler'].get_scheduler_status()
            
            push_stats = {
                'fcm_initialized': services['push_service'].fcm_initialized,
                'active_devices': services['push_service'].get_device_count()
            }
            
            return jsonify({
                'success': True,
                'status': 'healthy',
                'platform': 'Local',
                'database': {
                    'type': 'sqlite',
                    'path': services['db_service'].db_path
                },
                'stock_count': stock_count,
                'ipo_statistics': ipo_stats['summary'],
                'ipo_by_category': ipo_stats['by_category'],
                'market_status': market_status,
                'scheduler_status': scheduler_status,
                'push_notification_status': push_stats,
                'last_stock_scrape': last_scrape.isoformat() if last_scrape else None,
                'last_ipo_scrape': last_ipo_scrape.isoformat() if last_ipo_scrape else None,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return jsonify({
                'success': False,
                'status': 'error',
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/scheduler/status', methods=['GET'])
    @app.config['require_auth']
    def get_scheduler_status():
        """Get detailed scheduler status"""
        try:
            smart_scheduler = app.config['smart_scheduler']
            status = smart_scheduler.get_scheduler_status()
            return jsonify({
                'success': True,
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
    
    @app.route('/api/market-status', methods=['GET'])
    def get_market_status():
        """Get market status endpoint"""
        try:
            services = {
                'price_service': app.config['price_service'],
                'smart_scheduler': app.config['smart_scheduler']
            }
            
            market_status = services['price_service'].get_market_status()
            nepal_time = services['smart_scheduler']._get_current_nepal_time()
            
            enhanced_status = {
                **market_status,
                'nepal_time': nepal_time.isoformat(),
                'is_market_day': services['smart_scheduler']._is_market_day(nepal_time),
                'is_market_hours': services['smart_scheduler']._is_market_hours(nepal_time),
                'should_be_open': services['smart_scheduler']._is_market_open(nepal_time)
            }
            
            return jsonify({
                'success': True,
                'market_status': enhanced_status,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    # ==================== REGISTER SPECIALIZED ROUTE MODULES ====================
    
    logger.info("Registering stock routes...")
    register_stock_routes(app)
    
    logger.info("Registering issue routes...")
    register_issue_routes(app)
    
    logger.info("Registering push notification routes...")
    register_push_notification_routes(app)
    
    logger.info("Registering admin routes...")
    register_admin_routes(app)
    
    logger.info("Registering error handlers...")
    register_error_handlers(app)
    
    logger.info("All routes registered successfully")