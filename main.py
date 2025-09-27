# main.py - Main Application File

import os
import logging
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

# Import our service modules
from auth_service import AuthService, create_auth_decorators
from price_service import PriceService
from scraping_service import ScrapingService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NepalStockApp:
    """Main application class that orchestrates all services"""
    
    def __init__(self):
        # Configuration
        self.db_path = os.environ.get('DATABASE_PATH', 'nepal_stock.db')
        self.flask_host = os.environ.get('FLASK_HOST', '0.0.0.0')
        self.flask_port = int(os.environ.get('PORT', 5000))
        self.flask_debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
        
        # Initialize services
        self.auth_service = AuthService(self.db_path)
        self.price_service = PriceService(self.db_path)
        self.scraping_service = ScrapingService(self.price_service)
        
        # Create Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Create authentication decorators
        self.require_auth, self.require_admin = create_auth_decorators(self.auth_service)
        
        # Register routes
        self._register_routes()
        
        # Initialize data
        self._initialize_app()
    
    def _initialize_app(self):
        """Initialize application with default data"""
        logger.info("Initializing Nepal Stock Scraper Application...")
        
        # Check for admin keys
        self._ensure_admin_key()
        
        # Run initial data scrape
        logger.info("Running initial stock data scrape...")
        initial_count = self.scraping_service.scrape_all_sources(force=True)
        logger.info(f"Application initialized with {initial_count} stocks")
    
    def _ensure_admin_key(self):
        """Ensure at least one admin key exists"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM api_keys WHERE key_type = "admin" AND is_active = TRUE')
        admin_count = cursor.fetchone()[0]
        conn.close()
        
        if admin_count == 0:
            logger.info("No admin keys found, creating initial admin key...")
            initial_admin = self.auth_service.generate_api_key(
                key_type='admin',
                created_by='system',
                description='Initial admin key'
            )
            if initial_admin:
                logger.info("=" * 60)
                logger.info("INITIAL ADMIN KEY CREATED:")
                logger.info(f"Key ID: {initial_admin['key_id']}")
                logger.info(f"API Key: {initial_admin['api_key']}")
                logger.info("SAVE THIS KEY SECURELY - IT WON'T BE SHOWN AGAIN!")
                logger.info("=" * 60)
    
    def _register_routes(self):
        """Register all API routes"""
        
        # Public routes
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            try:
                stock_count = self.price_service.get_stock_count()
                market_status = self.price_service.get_market_status()
                last_scrape = self.scraping_service.get_last_scrape_time()
                
                return jsonify({
                    'success': True,
                    'status': 'healthy',
                    'stock_count': stock_count,
                    'market_status': market_status,
                    'last_scrape': last_scrape.isoformat() if last_scrape else None,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({
                    'success': False,
                    'status': 'error',
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/market-status', methods=['GET'])
        def get_market_status():
            """Get market status endpoint"""
            try:
                market_status = self.price_service.get_market_status()
                return jsonify({
                    'success': True,
                    'market_status': market_status,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # Authenticated routes
        @self.app.route('/api/stocks', methods=['GET'])
        @self.require_auth
        def get_stocks():
            """Get all stock data"""
            try:
                symbol = request.args.get('symbol')
                
                if symbol:
                    data = self.price_service.get_stock_by_symbol(symbol)
                    if not data:
                        return jsonify({
                            'success': False,
                            'error': 'Stock not found'
                        }), 404
                    data = [data]  # Make it a list for consistency
                else:
                    data = self.price_service.get_all_stocks()
                
                market_status = self.price_service.get_market_status()
                last_scrape = self.scraping_service.get_last_scrape_time()
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'market_status': market_status,
                    'last_scrape': last_scrape.isoformat() if last_scrape else None,
                    'timestamp': datetime.now().isoformat(),
                    'auth_info': {
                        'key_type': request.auth_info['key_type'],
                        'key_id': request.auth_info['key_id']
                    }
                })
            except Exception as e:
                logger.error(f"Get stocks error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/stocks/<symbol>', methods=['GET'])
        @self.require_auth
        def get_stock_by_symbol(symbol):
            """Get specific stock data by symbol"""
            try:
                data = self.price_service.get_stock_by_symbol(symbol)
                if data:
                    return jsonify({
                        'success': True,
                        'data': data,
                        'market_status': self.price_service.get_market_status(),
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': f'Stock {symbol.upper()} not found'
                    }), 404
            except Exception as e:
                logger.error(f"Get stock by symbol error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/stocks/search', methods=['GET'])
        @self.require_auth
        def search_stocks():
            """Search stocks by symbol or company name"""
            try:
                query = request.args.get('q', '').strip()
                if not query or len(query) < 2:
                    return jsonify({
                        'success': False,
                        'error': 'Search query must be at least 2 characters'
                    }), 400
                
                limit = min(int(request.args.get('limit', 20)), 100)  # Max 100 results
                
                results = self.price_service.search_stocks(query, limit)
                return jsonify({
                    'success': True,
                    'data': results,
                    'count': len(results),
                    'query': query,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Search stocks error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/stocks/gainers', methods=['GET'])
        @self.require_auth
        def get_top_gainers():
            """Get top gaining stocks"""
            try:
                limit = min(int(request.args.get('limit', 10)), 50)
                gainers = self.price_service.get_top_gainers(limit)
                
                return jsonify({
                    'success': True,
                    'data': gainers,
                    'count': len(gainers),
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/stocks/losers', methods=['GET'])
        @self.require_auth
        def get_top_losers():
            """Get top losing stocks"""
            try:
                limit = min(int(request.args.get('limit', 10)), 50)
                losers = self.price_service.get_top_losers(limit)
                
                return jsonify({
                    'success': True,
                    'data': losers,
                    'count': len(losers),
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/stocks/active', methods=['GET'])
        @self.require_auth
        def get_most_active():
            """Get most actively traded stocks"""
            try:
                limit = min(int(request.args.get('limit', 10)), 50)
                active = self.price_service.get_most_active(limit)
                
                return jsonify({
                    'success': True,
                    'data': active,
                    'count': len(active),
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/market-summary', methods=['GET'])
        @self.require_auth
        def get_market_summary():
            """Get market summary statistics"""
            try:
                summary = self.price_service.get_market_summary()
                return jsonify({
                    'success': True,
                    'data': summary,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/scrape', methods=['POST'])
        @self.require_auth
        def trigger_scrape():
            """Manually trigger scraping"""
            try:
                force = request.json.get('force', True) if request.is_json else True
                
                logger.info(f"Manual scrape triggered by {request.auth_info['key_id']} (force={force})")
                count = self.scraping_service.scrape_all_sources(force=force)
                
                return jsonify({
                    'success': True,
                    'message': f'Scraping completed successfully. {count} stocks updated.',
                    'count': count,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Manual scrape failed: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # Authentication routes
        @self.app.route('/api/auth/key-info', methods=['GET'])
        @self.require_auth
        def get_key_info():
            """Get information about the authenticated key"""
            try:
                key_info = self.auth_service.get_key_details(request.auth_info['key_id'])
                if key_info:
                    return jsonify({
                        'success': True,
                        'key_info': key_info
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Key information not found'
                    }), 404
            except Exception as e:
                logger.error(f"Error getting key info: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/auth/usage-stats', methods=['GET'])
        @self.require_auth
        def get_usage_stats():
            """Get usage statistics for the authenticated key"""
            try:
                days = min(int(request.args.get('days', 7)), 30)  # Max 30 days
                stats = self.auth_service.get_usage_stats(
                    key_id=request.auth_info['key_id'], 
                    days=days
                )
                
                return jsonify({
                    'success': True,
                    'usage_stats': stats,
                    'period_days': days,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        # Admin routes
        @self.app.route('/api/admin/generate-key', methods=['POST'])
        @self.require_auth
        @self.require_admin
        def admin_generate_key():
            """Generate new API key (admin only)"""
            try:
                data = request.get_json() or {}
                key_type = data.get('key_type', 'regular')
                description = data.get('description', '')
                
                if key_type not in ['admin', 'regular']:
                    return jsonify({
                        'success': False,
                        'error': 'Invalid key type. Must be "admin" or "regular"'
                    }), 400
                
                key_pair = self.auth_service.generate_api_key(
                    key_type=key_type,
                    created_by=request.auth_info['key_id'],
                    description=description
                )
                
                if key_pair:
                    return jsonify({
                        'success': True,
                        'key_pair': key_pair
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Failed to generate key'
                    }), 500
                    
            except Exception as e:
                logger.error(f"Error generating key: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/admin/keys', methods=['GET'])
        @self.require_auth
        @self.require_admin
        def admin_list_keys():
            """List all API keys (admin only)"""
            try:
                keys = self.auth_service.list_all_keys()
                return jsonify({
                    'success': True,
                    'keys': keys,
                    'total': len(keys)
                })
            except Exception as e:
                logger.error(f"Error listing keys: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/admin/keys/<key_id>', methods=['DELETE'])
        @self.require_auth
        @self.require_admin
        def admin_deactivate_key(key_id):
            """Deactivate an API key (admin only)"""
            try:
                if key_id == request.auth_info['key_id']:
                    return jsonify({
                        'success': False,
                        'error': 'Cannot deactivate your own key'
                    }), 400
                
                success = self.auth_service.deactivate_key(key_id)
                if success:
                    return jsonify({
                        'success': True,
                        'message': f'Key {key_id} deactivated successfully'
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Key not found or already inactive'
                    }), 404
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/admin/usage-stats', methods=['GET'])
        @self.require_auth
        @self.require_admin
        def admin_get_usage_stats():
            """Get overall usage statistics (admin only)"""
            try:
                days = min(int(request.args.get('days', 7)), 30)
                stats = self.auth_service.get_usage_stats(days=days)
                
                return jsonify({
                    'success': True,
                    'usage_stats': stats,
                    'period_days': days,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        # Error handlers
        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({
                'success': False,
                'error': 'Endpoint not found'
            }), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            logger.error(f"Internal server error: {error}")
            return jsonify({
                'success': False,
                'error': 'Internal server error'
            }), 500
        
        @self.app.errorhandler(403)
        def forbidden(error):
            return jsonify({
                'success': False,
                'error': 'Access forbidden'
            }), 403
        
        @self.app.errorhandler(401)
        def unauthorized(error):
            return jsonify({
                'success': False,
                'error': 'Authentication required'
            }), 401
    
    def run(self):
        """Run the Flask application"""
        logger.info(f"Starting Nepal Stock Scraper API on {self.flask_host}:{self.flask_port}")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Stock count: {self.price_service.get_stock_count()}")
        logger.info(f"Market status: {self.price_service.get_market_status()}")
        
        self.app.run(
            host=self.flask_host,
            port=self.flask_port,
            debug=self.flask_debug
        )


# Application entry point
if __name__ == '__main__':
    try:
        app = NepalStockApp()
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        raise