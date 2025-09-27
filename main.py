# main.py - Complete Railway-compatible version with MySQL/SQLite support

import os
import logging
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

# Import your existing service modules
from auth_service import AuthService, create_auth_decorators
from price_service import PriceService
from scraping_service import ScrapingService

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
    """Main application class - works with MySQL or SQLite"""
    
    def __init__(self):
        # Initialize database service
        self.db_service = DatabaseService()
        
        # Configuration
        self.flask_host = os.environ.get('FLASK_HOST', '0.0.0.0')
        self.flask_port = int(os.environ.get('PORT', 5000))
        self.flask_debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
        
        # Railway detection
        self.is_railway = 'RAILWAY_ENVIRONMENT' in os.environ or 'RAILWAY_PROJECT_ID' in os.environ
        
        # Initialize services based on database type
        if self.db_service.db_type == 'mysql':
            # For MySQL, we need to modify the services to use PyMySQL
            logger.info("Initializing services for MySQL...")
            self._init_mysql_tables()
            # Pass database service to services (you'd need to modify your service classes)
            db_path = None  # Not used for MySQL
        else:
            # For SQLite, use existing path-based initialization
            logger.info("Initializing services for SQLite...")
            db_path = self.db_service.connection_params['database']
        
        # Initialize services with existing code (works for SQLite)
        self.auth_service = AuthService(db_path or self.db_service.connection_params['database'])
        self.price_service = PriceService(db_path or self.db_service.connection_params['database'])
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
    
    def _init_mysql_tables(self):
        """Initialize MySQL tables (different from SQLite)"""
        if self.db_service.db_type != 'mysql':
            return
        
        conn = self.db_service.get_connection()
        try:
            cursor = conn.cursor()
            
            logger.info("Creating MySQL tables...")
            
            # API Keys table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key_id VARCHAR(100) UNIQUE NOT NULL,
                    key_hash VARCHAR(64) NOT NULL,
                    key_type ENUM('admin', 'regular') NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by VARCHAR(100),
                    is_active BOOLEAN DEFAULT TRUE,
                    last_used TIMESTAMP NULL,
                    max_devices INT DEFAULT 1,
                    description TEXT,
                    INDEX idx_key_id (key_id)
                )
            ''')
            
            # Stocks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    company_name VARCHAR(200),
                    ltp DECIMAL(10,2),
                    change_val DECIMAL(10,2),
                    change_percent DECIMAL(8,4),
                    high DECIMAL(10,2),
                    low DECIMAL(10,2),
                    open_price DECIMAL(10,2),
                    prev_close DECIMAL(10,2),
                    qty INT,
                    turnover DECIMAL(15,2),
                    trades INT DEFAULT 0,
                    source VARCHAR(100),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_latest BOOLEAN DEFAULT TRUE,
                    INDEX idx_symbol_latest (symbol, is_latest),
                    INDEX idx_timestamp (timestamp)
                )
            ''')
            
            # Device sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS device_sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key_id VARCHAR(100) NOT NULL,
                    device_id VARCHAR(100) NOT NULL,
                    device_info TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    UNIQUE KEY unique_key_device (key_id, device_id),
                    INDEX idx_key_device (key_id, device_id)
                )
            ''')
            
            # API logs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    key_id VARCHAR(100),
                    device_id VARCHAR(100),
                    endpoint VARCHAR(200),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    INDEX idx_key_timestamp (key_id, timestamp)
                )
            ''')
            
            # Market summary table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    total_turnover DECIMAL(18,2),
                    total_trades INT,
                    total_scrips INT,
                    advancing INT DEFAULT 0,
                    declining INT DEFAULT 0,
                    unchanged INT DEFAULT 0,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_latest BOOLEAN DEFAULT TRUE,
                    INDEX idx_latest (is_latest)
                )
            ''')
            
            conn.commit()
            logger.info("MySQL tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating MySQL tables: {e}")
            raise
        finally:
            conn.close()
    
    def _initialize_app(self):
        """Initialize application with default data"""
        db_type = self.db_service.db_type.upper()
        platform = "Railway" if self.is_railway else "Local"
        logger.info(f"Initializing Nepal Stock Scraper Application on {platform} with {db_type}...")
        
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
        logger.info("Running initial stock data scrape...")
        try:
            initial_count = self.scraping_service.scrape_all_sources(force=True)
            logger.info(f"Application initialized with {initial_count} stocks")
        except Exception as e:
            logger.warning(f"Initial scrape failed: {e}")
    
    def _ensure_admin_key(self):
        """Ensure at least one admin key exists"""
        try:
            if self.db_service.db_type == 'mysql':
                conn = self.db_service.get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM api_keys WHERE key_type = %s AND is_active = TRUE', ('admin',))
                result = cursor.fetchone()
                admin_count = result[0] if result else 0
                conn.close()
            else:
                # SQLite version (existing logic)
                conn = sqlite3.connect(self.db_service.connection_params['database'])
                cursor = conn.cursor()
                try:
                    cursor.execute('SELECT COUNT(*) FROM api_keys WHERE key_type = "admin" AND is_active = TRUE')
                    admin_count = cursor.fetchone()[0]
                except sqlite3.OperationalError:
                    # Tables don't exist yet, they'll be created by the services
                    admin_count = 0
                finally:
                    conn.close()
        except Exception as e:
            logger.error(f"Database table error: {e}")
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
        """Register all API routes"""
        
        # Public routes
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            try:
                stock_count = self.price_service.get_stock_count()
                market_status = self.price_service.get_market_status()
                last_scrape = self.scraping_service.get_last_scrape_time()
                
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
                    'market_status': market_status,
                    'last_scrape': last_scrape.isoformat() if last_scrape else None,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({
                    'success': False,
                    'status': 'error',
                    'error': str(e),
                    'database_type': self.db_service.db_type
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
        db_type = self.db_service.db_type.upper()
        platform = "Railway" if self.is_railway else "Local"
        
        logger.info(f"Starting Nepal Stock Scraper API on {platform}")
        logger.info(f"Host: {self.flask_host}, Port: {self.flask_port}")
        logger.info(f"Database: {db_type}")
        
        try:
            stock_count = self.price_service.get_stock_count()
            logger.info(f"Stock count: {stock_count}")
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
        logger.info("Application factory completed successfully")
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