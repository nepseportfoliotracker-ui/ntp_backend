# app.py - Flutter-ready version with separate IPO/FPO/Rights tables

import os
import logging
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

# Import your existing service modules
from auth_service import AuthService, create_auth_decorators
from price_service import PriceService
from scraping_service import EnhancedScrapingService
# Import the updated IPO service
from ipo_service import IPOService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseService:
    """Database service adapter that works with both SQLite and MySQL"""
    
    def __init__(self):
        self.db_type = self._detect_database_type()
        self.connection_params = self._get_connection_params()
        logger.info(f"Database service initialized: {self.db_type}")
    
    def _detect_database_type(self):
        """Detect which database to use based on environment"""
        # Check for MySQL connection string or MySQL-specific env vars
        mysql_indicators = [
            'MYSQL_URL', 'DATABASE_URL', 'MYSQL_HOST', 
            'MYSQL_DATABASE', 'MYSQLDATABASE'
        ]
        
        for indicator in mysql_indicators:
            env_val = os.environ.get(indicator, '')
            if env_val and ('mysql' in env_val.lower() or indicator.startswith('MYSQL')):
                return 'mysql'
        
        # Default to SQLite
        return 'sqlite'
    
    def _get_connection_params(self):
        """Get connection parameters based on database type"""
        if self.db_type == 'mysql':
            # Try to parse DATABASE_URL first (Railway format)
            database_url = os.environ.get('DATABASE_URL') or os.environ.get('MYSQL_URL', '')
            
            if database_url and 'mysql' in database_url:
                try:
                    import urllib.parse as urlparse
                    parsed = urlparse.urlparse(database_url)
                    return {
                        'host': parsed.hostname or 'localhost',
                        'port': parsed.port or 3306,
                        'user': parsed.username or 'root',
                        'password': parsed.password or '',
                        'database': parsed.path.lstrip('/') or 'railway',
                        'charset': 'utf8mb4',
                        'autocommit': True
                    }
                except Exception as e:
                    logger.warning(f"Failed to parse DATABASE_URL: {e}")
            
            # Fallback to individual environment variables
            return {
                'host': os.environ.get('MYSQL_HOST', os.environ.get('MYSQLHOST', 'localhost')),
                'port': int(os.environ.get('MYSQL_PORT', os.environ.get('MYSQLPORT', 3306))),
                'user': os.environ.get('MYSQL_USER', os.environ.get('MYSQLUSER', 'root')),
                'password': os.environ.get('MYSQL_PASSWORD', os.environ.get('MYSQLPASSWORD', '')),
                'database': os.environ.get('MYSQL_DATABASE', os.environ.get('MYSQLDATABASE', 'railway')),
                'charset': 'utf8mb4',
                'autocommit': True
            }
        else:
            # SQLite
            return {
                'database': os.environ.get('DATABASE_PATH', 'nepal_stock.db')
            }
    
    def get_connection(self):
        """Get database connection based on type"""
        if self.db_type == 'mysql':
            try:
                import pymysql
                conn = pymysql.connect(**self.connection_params)
                return conn
            except ImportError:
                logger.error("PyMySQL not installed. Install with: pip install PyMySQL")
                raise
            except Exception as e:
                logger.error(f"MySQL connection failed: {e}")
                logger.error(f"Connection params: {self.connection_params}")
                raise
        else:
            conn = sqlite3.connect(self.connection_params['database'])
            conn.execute('PRAGMA foreign_keys = ON')
            return conn

class NepalStockApp:
    """Flutter-ready application class with separate IPO/FPO/Rights tables"""
    
    def __init__(self):
        # Initialize database service
        self.db_service = DatabaseService()
        
        # Configuration
        self.flask_host = os.environ.get('FLASK_HOST', '0.0.0.0')
        self.flask_port = int(os.environ.get('PORT', 5000))
        self.flask_debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
        
        # Railway detection
        self.is_railway = 'RAILWAY_ENVIRONMENT' in os.environ or 'RAILWAY_PROJECT_ID' in os.environ
        
        # Initialize services with database service
        logger.info(f"Initializing services for {self.db_service.db_type}...")
        
        # Pass the database service to all service classes
        self.auth_service = AuthService(self.db_service)
        self.price_service = PriceService(self.db_service)
        self.ipo_service = IPOService(self.db_service)  # Updated IPO service
        self.scraping_service = EnhancedScrapingService(self.price_service, self.ipo_service)
        
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
        db_type = self.db_service.db_type.upper()
        platform = "Railway" if self.is_railway else "Local"
        logger.info(f"Initializing Flutter-ready Nepal Stock API on {platform} with {db_type}...")
        
        # Log database connection info
        if self.db_service.db_type == 'mysql':
            params = self.db_service.connection_params.copy()
            params['password'] = '***' if params.get('password') else 'None'
            logger.info(f"MySQL connection: {params}")
        else:
            logger.info(f"SQLite database path: {self.db_service.connection_params['database']}")
        
        # Check for admin keys
        self._ensure_admin_key()
        
        # Run initial data scrape
        logger.info("Running initial stock and IPO data scrape...")
        try:
            initial_counts = self.scraping_service.scrape_all_data(force=True)
            logger.info(f"Application initialized with {initial_counts['stocks']} stocks and {initial_counts['ipos']} IPOs/FPOs/Rights")
        except Exception as e:
            logger.warning(f"Initial scrape failed: {e}")
    
    def _ensure_admin_key(self):
        """Ensure at least one admin key exists"""
        try:
            conn = self.db_service.get_connection()
            try:
                if self.db_service.db_type == 'mysql':
                    cursor = conn.cursor()
                    cursor.execute('SELECT COUNT(*) FROM api_keys WHERE key_type = %s AND is_active = TRUE', ('admin',))
                    result = cursor.fetchone()
                    admin_count = result[0] if result else 0
                else:
                    cursor = conn.cursor()
                    cursor.execute('SELECT COUNT(*) FROM api_keys WHERE key_type = "admin" AND is_active = TRUE')
                    admin_count = cursor.fetchone()[0]
            except Exception as e:
                # Tables don't exist yet, they'll be created by the services
                logger.info(f"Admin key check failed (tables may not exist yet): {e}")
                admin_count = 0
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            admin_count = 0
        
        if admin_count == 0:
            db_type = self.db_service.db_type.upper()
            platform = "Railway" if self.is_railway else "Local"
            logger.info(f"No admin keys found, creating initial admin key for {platform} ({db_type})...")
            
            initial_admin = self.auth_service.generate_api_key(
                key_type='admin',
                created_by=f'{platform.lower()}-system',
                description=f'Initial {platform} admin key ({db_type})'
            )
            if initial_admin:
                logger.info("=" * 60)
                logger.info(f"{platform.upper()} ADMIN KEY CREATED ({db_type}):")
                logger.info(f"Key ID: {initial_admin['key_id']}")
                logger.info(f"API Key: {initial_admin['api_key']}")
                logger.info("SAVE THIS KEY SECURELY - IT WON'T BE SHOWN AGAIN!")
                logger.info("=" * 60)
    
    def _register_routes(self):
        """Register all Flutter-ready API routes"""
        
        # Public routes
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Flutter-ready health check endpoint"""
            try:
                stock_count = self.price_service.get_stock_count()
                market_status = self.price_service.get_market_status()
                last_scrape = self.scraping_service.get_last_scrape_time()
                last_ipo_scrape = self.scraping_service.get_last_ipo_scrape_time()
                
                # Get IPO statistics using the updated service
                ipo_stats = self.ipo_service.get_statistics()
                
                platform = "Railway" if self.is_railway else "Local"
                
                # Safe connection info for health check
                db_info = {
                    'type': self.db_service.db_type,
                    'platform': platform
                }
                
                if self.db_service.db_type == 'mysql':
                    params = self.db_service.connection_params
                    db_info.update({
                        'host': params.get('host', 'unknown'),
                        'port': params.get('port', 'unknown'),
                        'database': params.get('database', 'unknown')
                    })
                else:
                    db_info['path'] = self.db_service.connection_params['database']
                
                return jsonify({
                    'success': True,
                    'status': 'healthy',
                    'platform': platform,
                    'database': db_info,
                    'stock_count': stock_count,
                    'ipo_statistics': ipo_stats['summary'],  # Flutter-ready summary
                    'ipo_by_category': ipo_stats['by_category'],  # IPO/FPO/Rights breakdown
                    'market_status': market_status,
                    'last_stock_scrape': last_scrape.isoformat() if last_scrape else None,
                    'last_ipo_scrape': last_ipo_scrape.isoformat() if last_ipo_scrape else None,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True  # Indicates this API is Flutter-ready
                })
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({
                    'success': False,
                    'status': 'error',
                    'error': str(e),
                    'database_type': self.db_service.db_type,
                    'flutter_ready': True
                }), 500
        
        @self.app.route('/api/market-status', methods=['GET'])
        def get_market_status():
            """Get market status endpoint"""
            try:
                market_status = self.price_service.get_market_status()
                return jsonify({
                    'success': True,
                    'market_status': market_status,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        # Stock routes (existing but with Flutter-ready enhancements)
        @self.app.route('/api/stocks', methods=['GET'])
        @self.require_auth
        def get_stocks():
            """Get all stock data - Flutter ready"""
            try:
                symbol = request.args.get('symbol')
                
                if symbol:
                    data = self.price_service.get_stock_by_symbol(symbol)
                    if not data:
                        return jsonify({
                            'success': False,
                            'error': 'Stock not found',
                            'flutter_ready': True
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
                    'flutter_ready': True,
                    'auth_info': {
                        'key_type': request.auth_info['key_type'],
                        'key_id': request.auth_info['key_id']
                    }
                })
            except Exception as e:
                logger.error(f"Get stocks error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        # Enhanced IPO/FPO/Rights endpoints for Flutter
        @self.app.route('/api/issues', methods=['GET'])
        @self.require_auth
        def get_all_issues():
            """Get all issues (IPO/FPO/Rights) - Flutter optimized"""
            try:
                status = request.args.get('status', 'all')  # all, open, coming_soon, closed
                category = request.args.get('category')  # IPO, FPO, Rights
                limit = min(int(request.args.get('limit', 50)), 100)
                
                if status == 'open':
                    data = self.ipo_service.get_open_issues(category)
                elif status == 'coming_soon':
                    data = self.ipo_service.get_coming_soon_issues()
                else:
                    # Get all issues
                    all_issues = []
                    all_issues.extend(self.ipo_service.get_all_ipos())
                    all_issues.extend(self.ipo_service.get_all_fpos())
                    all_issues.extend(self.ipo_service.get_all_rights_dividends())
                    
                    # Filter by category if specified
                    if category:
                        category_upper = category.upper()
                        data = [issue for issue in all_issues if issue.get('issue_category', '').upper() == category_upper]
                    else:
                        data = all_issues
                    
                    # Sort by scraped_at (most recent first)
                    data.sort(key=lambda x: x.get('scraped_at', ''), reverse=True)
                
                # Apply limit
                data = data[:limit]
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'filters': {
                        'status': status,
                        'category': category,
                        'limit': limit
                    },
                    'last_ipo_scrape': self.scraping_service.get_last_ipo_scrape_time().isoformat() if self.scraping_service.get_last_ipo_scrape_time() else None,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
                
            except Exception as e:
                logger.error(f"Get all issues error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        @self.app.route('/api/issues/ipos', methods=['GET'])
        @self.require_auth
        def get_ipos_only():
            """Get IPOs only - Flutter optimized"""
            try:
                data = self.ipo_service.get_all_ipos()
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'category': 'IPO',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/issues/fpos', methods=['GET'])
        @self.require_auth
        def get_fpos_only():
            """Get FPOs only - Flutter optimized"""
            try:
                data = self.ipo_service.get_all_fpos()
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'category': 'FPO',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/issues/rights', methods=['GET'])
        @self.require_auth
        def get_rights_only():
            """Get Rights/Dividends only - Flutter optimized"""
            try:
                data = self.ipo_service.get_all_rights_dividends()
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'category': 'Rights',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/issues/open', methods=['GET'])
        @self.require_auth
        def get_open_issues():
            """Get currently open issues - Flutter optimized"""
            try:
                category = request.args.get('category')  # Optional filter
                data = self.ipo_service.get_open_issues(category)
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'status': 'open',
                    'category_filter': category,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/issues/coming-soon', methods=['GET'])
        @self.require_auth
        def get_coming_soon_issues():
            """Get coming soon issues - Flutter optimized"""
            try:
                data = self.ipo_service.get_coming_soon_issues()
                
                return jsonify({
                    'success': True,
                    'data': data,
                    'count': len(data),
                    'status': 'coming_soon',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/issues/search', methods=['GET'])
        @self.require_auth
        def search_issues():
            """Search all issues - Flutter optimized"""
            try:
                query = request.args.get('q', '').strip()
                if not query or len(query) < 2:
                    return jsonify({
                        'success': False,
                        'error': 'Search query must be at least 2 characters',
                        'flutter_ready': True
                    }), 400
                
                limit = min(int(request.args.get('limit', 20)), 100)
                results = self.ipo_service.search_issues(query, limit)
                
                return jsonify({
                    'success': True,
                    'data': results,
                    'count': len(results),
                    'query': query,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/issues/statistics', methods=['GET'])
        @self.require_auth
        def get_issue_statistics():
            """Get detailed statistics for Flutter dashboard"""
            try:
                stats = self.ipo_service.get_statistics()
                
                return jsonify({
                    'success': True,
                    'statistics': stats,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        # Keep existing stock routes with Flutter-ready enhancements
        @self.app.route('/api/stocks/<symbol>', methods=['GET'])
        @self.require_auth
        def get_stock_by_symbol(symbol):
            """Get specific stock data by symbol - Flutter ready"""
            try:
                data = self.price_service.get_stock_by_symbol(symbol)
                if data:
                    return jsonify({
                        'success': True,
                        'data': data,
                        'market_status': self.price_service.get_market_status(),
                        'timestamp': datetime.now().isoformat(),
                        'flutter_ready': True
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': f'Stock {symbol.upper()} not found',
                        'flutter_ready': True
                    }), 404
            except Exception as e:
                logger.error(f"Get stock by symbol error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        @self.app.route('/api/stocks/search', methods=['GET'])
        @self.require_auth
        def search_stocks():
            """Search stocks by symbol or company name - Flutter ready"""
            try:
                query = request.args.get('q', '').strip()
                if not query or len(query) < 2:
                    return jsonify({
                        'success': False,
                        'error': 'Search query must be at least 2 characters',
                        'flutter_ready': True
                    }), 400
                
                limit = min(int(request.args.get('limit', 20)), 100)
                
                results = self.price_service.search_stocks(query, limit)
                return jsonify({
                    'success': True,
                    'data': results,
                    'count': len(results),
                    'query': query,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                logger.error(f"Search stocks error: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        @self.app.route('/api/stocks/gainers', methods=['GET'])
        @self.require_auth
        def get_top_gainers():
            """Get top gaining stocks - Flutter ready"""
            try:
                limit = min(int(request.args.get('limit', 10)), 50)
                gainers = self.price_service.get_top_gainers(limit)
                
                return jsonify({
                    'success': True,
                    'data': gainers,
                    'count': len(gainers),
                    'category': 'gainers',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/stocks/losers', methods=['GET'])
        @self.require_auth
        def get_top_losers():
            """Get top losing stocks - Flutter ready"""
            try:
                limit = min(int(request.args.get('limit', 10)), 50)
                losers = self.price_service.get_top_losers(limit)
                
                return jsonify({
                    'success': True,
                    'data': losers,
                    'count': len(losers),
                    'category': 'losers',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/stocks/active', methods=['GET'])
        @self.require_auth
        def get_most_active():
            """Get most actively traded stocks - Flutter ready"""
            try:
                limit = min(int(request.args.get('limit', 10)), 50)
                active = self.price_service.get_most_active(limit)
                
                return jsonify({
                    'success': True,
                    'data': active,
                    'count': len(active),
                    'category': 'active',
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        @self.app.route('/api/market-summary', methods=['GET'])
        @self.require_auth
        def get_market_summary():
            """Get market summary statistics - Flutter ready"""
            try:
                summary = self.price_service.get_market_summary()
                return jsonify({
                    'success': True,
                    'data': summary,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            except Exception as e:
                return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
        
        # Enhanced scraping endpoint
        @self.app.route('/api/trigger-scrape', methods=['POST'])
        @self.require_auth
        def trigger_scrape():
            """Manually trigger scraping for both stocks and IPOs - Flutter ready"""
            try:
                data = request.get_json() or {}
                force = data.get('force', True)
                scrape_type = data.get('type', 'all')  # 'stocks', 'issues', or 'all'
                
                logger.info(f"Manual scrape triggered by {request.auth_info['key_id']} (type={scrape_type}, force={force})")
                
                results = {}
                
                if scrape_type in ['stocks', 'all']:
                    stock_count = self.scraping_service.scrape_all_sources(force=force)
                    results['stocks'] = stock_count
                
                if scrape_type in ['issues', 'ipos', 'all']:
                    ipo_count = self.scraping_service.scrape_ipo_sources(force=force)
                    results['issues'] = ipo_count
                
                total_count = sum(results.values())
                
                return jsonify({
                    'success': True,
                    'message': f'Scraping completed successfully. {total_count} total items updated.',
                    'results': results,
                    'total_count': total_count,
                    'scrape_type': scrape_type,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                }), 201
            except Exception as e:
                logger.error(f"Manual scrape failed: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        # Authentication routes (with Flutter-ready enhancements)
        @self.app.route('/api/key-info', methods=['GET'])
        @self.require_auth
        def get_key_info():
            """Get information about the authenticated key - Flutter ready"""
            try:
                key_info = self.auth_service.get_key_details(request.auth_info['key_id'])
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
                logger.error(f"Error getting key info: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        # Admin routes (existing ones with Flutter-ready enhancements)
        @self.app.route('/api/admin/generate-key', methods=['POST'])
        @self.require_auth
        @self.require_admin
        def admin_generate_key():
            """Generate new API key (admin only) - Flutter ready"""
            try:
                data = request.get_json() or {}
                key_type = data.get('key_type', 'regular')
                description = data.get('description', '')
                
                if key_type not in ['admin', 'regular']:
                    return jsonify({
                        'success': False,
                        'error': 'Invalid key type. Must be "admin" or "regular"',
                        'flutter_ready': True
                    }), 400
                
                key_pair = self.auth_service.generate_api_key(
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
                logger.error(f"Error generating key: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        @self.app.route('/api/admin/stats', methods=['GET'])
        @self.require_auth
        @self.require_admin
        def admin_get_stats():
            """Get enhanced system statistics (admin only) - Flutter ready"""
            try:
                # Get usage stats for the last 24 hours
                usage_stats = self.auth_service.get_usage_stats(days=1)
                total_requests_24h = sum(usage_stats.values()) if usage_stats else 0
                
                # Get all keys count
                all_keys = self.auth_service.list_all_keys()
                active_keys = len([k for k in all_keys if k['is_active']])
                
                # Count active sessions
                active_sessions = 0
                try:
                    conn = self.db_service.get_connection()
                    cursor = conn.cursor()
                    if self.db_service.db_type == 'sqlite':
                        cursor.execute('SELECT COUNT(*) FROM device_sessions WHERE is_active = 1')
                    else:
                        cursor.execute('SELECT COUNT(*) FROM device_sessions WHERE is_active = TRUE')
                    result = cursor.fetchone()
                    active_sessions = result[0] if result else 0
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error counting active sessions: {e}")
                
                # Get stock count
                try:
                    stock_count = self.price_service.get_stock_count()
                except Exception as e:
                    logger.warning(f"Error getting stock count: {e}")
                    stock_count = 0
                
                # Get issue statistics using the updated service
                try:
                    issue_stats = self.ipo_service.get_statistics()
                except Exception as e:
                    logger.warning(f"Error getting issue stats: {e}")
                    issue_stats = {
                        'summary': {'total_issues': 0, 'open_issues': 0, 'coming_soon_issues': 0},
                        'by_category': {}
                    }
                
                stats = {
                    'active_keys': active_keys,
                    'total_keys': len(all_keys),
                    'active_sessions': active_sessions,
                    'requests_24h': total_requests_24h,
                    'stock_count': stock_count,
                    'issue_statistics': issue_stats['summary'],
                    'issues_by_category': issue_stats['by_category'],
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                }
                
                return jsonify({
                    'success': True,
                    'stats': stats,
                    'flutter_ready': True
                })
                
            except Exception as e:
                logger.error(f"Error getting admin stats: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'flutter_ready': True
                }), 500
        
        # Error handlers with Flutter-ready responses
        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({
                'success': False,
                'error': 'Endpoint not found',
                'flutter_ready': True
            }), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            logger.error(f"Internal server error: {error}")
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'flutter_ready': True
            }), 500
        
        @self.app.errorhandler(403)
        def forbidden(error):
            return jsonify({
                'success': False,
                'error': 'Access forbidden',
                'flutter_ready': True
            }), 403
        
        @self.app.errorhandler(401)
        def unauthorized(error):
            return jsonify({
                'success': False,
                'error': 'Authentication required',
                'flutter_ready': True
            }), 401
    
    def run(self):
        """Run the Flask application"""
        db_type = self.db_service.db_type.upper()
        platform = "Railway" if self.is_railway else "Local"
        
        logger.info(f"Starting Flutter-ready Nepal Stock API on {platform}")
        logger.info(f"Host: {self.flask_host}, Port: {self.flask_port}")
        logger.info(f"Database: {db_type}")
        logger.info("Features: Stock prices, IPO/FPO/Rights tracking (separate tables)")
        
        try:
            stock_count = self.price_service.get_stock_count()
            logger.info(f"Stock count: {stock_count}")
            
            issue_stats = self.ipo_service.get_statistics()
            logger.info(f"Issues: {issue_stats['summary']}")
            logger.info(f"By category: {issue_stats['by_category']}")
            
            market_status = self.price_service.get_market_status()
            logger.info(f"Market status: {market_status['status']}")
        except Exception as e:
            logger.warning(f"Could not get initial stats: {e}")
        
        self.app.run(
            host=self.flask_host,
            port=self.flask_port,
            debug=self.flask_debug
        )


# For Gunicorn (Railway and production)
def create_app():
    """Factory function for Gunicorn"""
    try:
        nepal_app = NepalStockApp()
        logger.info("Flutter-ready application factory completed successfully")
        return nepal_app.app
    except Exception as e:
        logger.error(f"Application factory failed: {e}")
        raise

# Create the app instance that Gunicorn will use
app = create_app()

# For local development with python main.py
if __name__ == '__main__':
    try:
        # This runs when you do "python main.py" locally
        nepal_app = NepalStockApp()
        nepal_app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        raise