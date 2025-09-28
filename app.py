# app.py - Flutter-ready version with intelligent scheduled scraping

import os
import logging
import sqlite3
from datetime import datetime, time, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import hashlib
import json

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

class SmartScheduler:
    """Intelligent scheduler for market-aware scraping"""
    
    def __init__(self, scraping_service, price_service, db_service):
        self.scraping_service = scraping_service
        self.price_service = price_service
        self.db_service = db_service
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Kathmandu'))
        
        # Market configuration for Nepal (Sunday-Thursday, 11 AM - 3 PM)
        self.market_days = [6, 0, 1, 2, 3]  # Sunday=6, Monday=0, ..., Thursday=3
        self.market_start_time = time(11, 0)  # 11:00 AM
        self.market_end_time = time(15, 0)    # 3:00 PM
        self.nepal_tz = pytz.timezone('Asia/Kathmandu')
        
        # Smart detection settings
        self.daily_scrape_count = 0
        self.daily_no_change_count = 0
        self.last_data_hash = None
        self.market_closed_today = False
        
        # Initialize scheduler table
        self._init_scheduler_table()
    
    def _init_scheduler_table(self):
        """Initialize table to track scraping history and market status"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            if self.db_service.db_type == 'mysql':
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS scheduler_history (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        date DATE NOT NULL,
                        scrape_time DATETIME NOT NULL,
                        data_hash VARCHAR(64),
                        data_changed BOOLEAN DEFAULT TRUE,
                        scrape_count INT DEFAULT 1,
                        market_detected_closed BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_date_time (date, scrape_time)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS scheduler_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        scrape_time TEXT NOT NULL,
                        data_hash TEXT,
                        data_changed INTEGER DEFAULT 1,
                        scrape_count INTEGER DEFAULT 1,
                        market_detected_closed INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(date, scrape_time)
                    )
                """)
            
            conn.commit()
            conn.close()
            logger.info("Scheduler history table initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize scheduler table: {e}")
    
    def _get_current_nepal_time(self):
        """Get current time in Nepal timezone"""
        return datetime.now(self.nepal_tz)
    
    def _is_market_day(self, dt=None):
        """Check if given datetime (or now) is a market day"""
        if dt is None:
            dt = self._get_current_nepal_time()
        return dt.weekday() in self.market_days
    
    def _is_market_hours(self, dt=None):
        """Check if given datetime (or now) is within market hours"""
        if dt is None:
            dt = self._get_current_nepal_time()
        current_time = dt.time()
        return self.market_start_time <= current_time <= self.market_end_time
    
    def _is_market_open(self, dt=None):
        """Check if market should be open (market day + market hours)"""
        return self._is_market_day(dt) and self._is_market_hours(dt)
    
    def _calculate_data_hash(self, stocks_data):
        """Calculate hash of current stock data to detect changes"""
        try:
            # Create a simple representation of key stock data
            data_for_hash = []
            for stock in stocks_data[:50]:  # Use first 50 stocks for performance
                data_for_hash.append({
                    'symbol': stock.get('symbol', ''),
                    'ltp': stock.get('ltp', 0),
                    'change': stock.get('change', 0),
                    'volume': stock.get('total_traded_quantity', 0)
                })
            
            # Create hash
            data_str = json.dumps(data_for_hash, sort_keys=True)
            return hashlib.md5(data_str.encode()).hexdigest()
            
        except Exception as e:
            logger.warning(f"Failed to calculate data hash: {e}")
            return None
    
    def _get_today_scrape_info(self):
        """Get today's scrape information"""
        try:
            today = self._get_current_nepal_time().date()
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            if self.db_service.db_type == 'mysql':
                cursor.execute("""
                    SELECT COUNT(*) as scrape_count, 
                           SUM(CASE WHEN data_changed = FALSE THEN 1 ELSE 0 END) as no_change_count,
                           MAX(CASE WHEN market_detected_closed = TRUE THEN 1 ELSE 0 END) as market_closed
                    FROM scheduler_history 
                    WHERE date = %s
                """, (today,))
            else:
                cursor.execute("""
                    SELECT COUNT(*) as scrape_count, 
                           SUM(CASE WHEN data_changed = 0 THEN 1 ELSE 0 END) as no_change_count,
                           MAX(CASE WHEN market_detected_closed = 1 THEN 1 ELSE 0 END) as market_closed
                    FROM scheduler_history 
                    WHERE date = ?
                """, (today.isoformat(),))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'scrape_count': result[0] or 0,
                    'no_change_count': result[1] or 0,
                    'market_closed': bool(result[2]) if result[2] is not None else False
                }
            else:
                return {'scrape_count': 0, 'no_change_count': 0, 'market_closed': False}
                
        except Exception as e:
            logger.error(f"Failed to get today's scrape info: {e}")
            return {'scrape_count': 0, 'no_change_count': 0, 'market_closed': False}
    
    def _record_scrape_result(self, data_changed, data_hash=None):
        """Record the result of a scrape"""
        try:
            now = self._get_current_nepal_time()
            today = now.date()
            scrape_info = self._get_today_scrape_info()
            
            # Determine if market appears closed
            market_detected_closed = False
            if scrape_info['scrape_count'] >= 2 and scrape_info['no_change_count'] >= 2:
                market_detected_closed = True
            
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            if self.db_service.db_type == 'mysql':
                cursor.execute("""
                    INSERT INTO scheduler_history 
                    (date, scrape_time, data_hash, data_changed, scrape_count, market_detected_closed)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    data_hash = VALUES(data_hash),
                    data_changed = VALUES(data_changed),
                    scrape_count = VALUES(scrape_count),
                    market_detected_closed = VALUES(market_detected_closed)
                """, (
                    today, now, data_hash, data_changed, 
                    scrape_info['scrape_count'] + 1, market_detected_closed
                ))
            else:
                cursor.execute("""
                    INSERT OR REPLACE INTO scheduler_history 
                    (date, scrape_time, data_hash, data_changed, scrape_count, market_detected_closed)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    today.isoformat(), now.isoformat(), data_hash, int(data_changed), 
                    scrape_info['scrape_count'] + 1, int(market_detected_closed)
                ))
            
            conn.commit()
            conn.close()
            
            # Update instance variables
            self.market_closed_today = market_detected_closed
            
        except Exception as e:
            logger.error(f"Failed to record scrape result: {e}")
    
    def should_scrape_now(self):
        """Determine if scraping should happen now based on intelligent rules"""
        now = self._get_current_nepal_time()
        
        # Check if it's a market day and within market hours
        if not self._is_market_open(now):
            logger.info(f"Skipping scrape - outside market hours or not a market day")
            return False
        
        # Get today's scrape information
        scrape_info = self._get_today_scrape_info()
        
        # If market was already detected as closed today, skip
        if scrape_info['market_closed']:
            logger.info(f"Skipping scrape - market detected as closed today")
            return False
        
        # If this is early in the day and we haven't done the initial detection, allow scraping
        if scrape_info['scrape_count'] < 2:
            logger.info(f"Allowing scrape for market detection (scrape #{scrape_info['scrape_count'] + 1})")
            return True
        
        # If we've detected the market is active today (some data changed), continue scraping
        if scrape_info['no_change_count'] < 2:
            logger.info(f"Allowing scrape - market appears active")
            return True
        
        logger.info(f"Skipping scrape - market appears closed today")
        return False
    
    def scheduled_scrape(self):
        """Execute scheduled scraping with intelligent logic"""
        try:
            logger.info("=== Scheduled Scrape Started ===")
            
            # Check if we should scrape
            if not self.should_scrape_now():
                return
            
            # Get current data for comparison
            current_stocks = self.price_service.get_all_stocks()
            current_hash = self._calculate_data_hash(current_stocks)
            
            # Perform the scrape
            logger.info("Performing scheduled stock data scrape...")
            stock_count = self.scraping_service.scrape_all_sources(force=True)
            
            # Get updated data
            updated_stocks = self.price_service.get_all_stocks()
            new_hash = self._calculate_data_hash(updated_stocks)
            
            # Determine if data changed
            data_changed = current_hash != new_hash
            
            # Record the result
            self._record_scrape_result(data_changed, new_hash)
            
            # Log the result
            scrape_info = self._get_today_scrape_info()
            logger.info(f"Scheduled scrape completed: {stock_count} stocks processed")
            logger.info(f"Data changed: {data_changed}")
            logger.info(f"Today's stats: {scrape_info['scrape_count']} scrapes, {scrape_info['no_change_count']} no-change")
            
            if self.market_closed_today:
                logger.info("Market detected as closed - future scrapes will be skipped today")
            
        except Exception as e:
            logger.error(f"Scheduled scrape failed: {e}")
    
    def start(self):
        """Start the intelligent scheduler"""
        try:
            # Schedule scraping every 15 minutes during market hours on market days
            self.scheduler.add_job(
                func=self.scheduled_scrape,
                trigger=CronTrigger(
                    day_of_week='sun,mon,tue,wed,thu',  # Market days
                    hour='11-14',  # 11 AM to 2 PM (last trigger at 2:45 PM)
                    minute='*/15',  # Every 15 minutes
                    timezone=self.nepal_tz
                ),
                id='market_scraper',
                name='Intelligent Market Data Scraper',
                max_instances=1,
                replace_existing=True
            )
            
            self.scheduler.start()
            logger.info("Intelligent scheduler started successfully")
            logger.info("Schedule: Every 15 minutes during market hours (11 AM - 3 PM, Sun-Thu)")
            logger.info("Features: Market closure detection, automatic pause on closed days")
            
            # Log next scheduled run
            next_run = self.scheduler.get_job('market_scraper').next_run_time
            if next_run:
                logger.info(f"Next scheduled scrape: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Intelligent scheduler stopped")
    
    def get_scheduler_status(self):
        """Get current scheduler status for API"""
        try:
            status = {
                'scheduler_running': self.scheduler.running if hasattr(self, 'scheduler') else False,
                'next_run': None,
                'current_nepal_time': self._get_current_nepal_time().isoformat(),
                'market_currently_open': self._is_market_open(),
                'today_scrape_info': self._get_today_scrape_info(),
                'market_detected_closed_today': self.market_closed_today
            }
            
            if self.scheduler.running:
                job = self.scheduler.get_job('market_scraper')
                if job and job.next_run_time:
                    status['next_run'] = job.next_run_time.isoformat()
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {'error': str(e)}

class NepalStockApp:
    """Flutter-ready application class with intelligent scheduled scraping"""
    
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
        
        # Initialize intelligent scheduler
        self.smart_scheduler = SmartScheduler(
            self.scraping_service, 
            self.price_service, 
            self.db_service
        )
        
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
        
        # Start intelligent scheduler
        try:
            self.smart_scheduler.start()
        except Exception as e:
            logger.error(f"Failed to start intelligent scheduler: {e}")
    
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
            """Flutter-ready health check endpoint with scheduler status"""
            try:
                stock_count = self.price_service.get_stock_count()
                market_status = self.price_service.get_market_status()
                last_scrape = self.scraping_service.get_last_scrape_time()
                last_ipo_scrape = self.scraping_service.get_last_ipo_scrape_time()
                
                # Get IPO statistics using the updated service
                ipo_stats = self.ipo_service.get_statistics()
                
                # Get scheduler status
                scheduler_status = self.smart_scheduler.get_scheduler_status()
                
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
                    'scheduler_status': scheduler_status,  # New: Scheduler information
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
        
        @self.app.route('/api/scheduler/status', methods=['GET'])
        @self.require_auth
        def get_scheduler_status():
            """Get detailed scheduler status"""
            try:
                status = self.smart_scheduler.get_scheduler_status()
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
        
        @self.app.route('/api/market-status', methods=['GET'])
        def get_market_status():
            """Get market status endpoint"""
            try:
                market_status = self.price_service.get_market_status()
                nepal_time = self.smart_scheduler._get_current_nepal_time()
                
                # Enhanced market status with scheduler insights
                enhanced_status = {
                    **market_status,
                    'nepal_time': nepal_time.isoformat(),
                    'is_market_day': self.smart_scheduler._is_market_day(nepal_time),
                    'is_market_hours': self.smart_scheduler._is_market_hours(nepal_time),
                    'should_be_open': self.smart_scheduler._is_market_open(nepal_time)
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
                
                # Get scheduler status
                try:
                    scheduler_status = self.smart_scheduler.get_scheduler_status()
                except Exception as e:
                    logger.warning(f"Error getting scheduler status: {e}")
                    scheduler_status = {'error': str(e)}
                
                stats = {
                    'active_keys': active_keys,
                    'total_keys': len(all_keys),
                    'active_sessions': active_sessions,
                    'requests_24h': total_requests_24h,
                    'stock_count': stock_count,
                    'issue_statistics': issue_stats['summary'],
                    'issues_by_category': issue_stats['by_category'],
                    'scheduler_status': scheduler_status,
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
        
        # New admin endpoint to control scheduler
        @self.app.route('/api/admin/scheduler/control', methods=['POST'])
        @self.require_auth
        @self.require_admin
        def admin_scheduler_control():
            """Control scheduler (start/stop/restart) - Admin only"""
            try:
                data = request.get_json() or {}
                action = data.get('action', '').lower()
                
                if action not in ['start', 'stop', 'restart', 'force_scrape']:
                    return jsonify({
                        'success': False,
                        'error': 'Invalid action. Must be start, stop, restart, or force_scrape',
                        'flutter_ready': True
                    }), 400
                
                if action == 'stop':
                    self.smart_scheduler.stop()
                    message = 'Scheduler stopped successfully'
                    
                elif action == 'start':
                    if not self.smart_scheduler.scheduler.running:
                        self.smart_scheduler.start()
                        message = 'Scheduler started successfully'
                    else:
                        message = 'Scheduler is already running'
                        
                elif action == 'restart':
                    self.smart_scheduler.stop()
                    self.smart_scheduler.start()
                    message = 'Scheduler restarted successfully'
                    
                elif action == 'force_scrape':
                    # Force a scrape regardless of market conditions
                    logger.info(f"Force scrape triggered by admin {request.auth_info['key_id']}")
                    self.smart_scheduler.scheduled_scrape()
                    message = 'Force scrape executed successfully'
                
                # Get updated status
                status = self.smart_scheduler.get_scheduler_status()
                
                return jsonify({
                    'success': True,
                    'message': message,
                    'action': action,
                    'scheduler_status': status,
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
                
            except Exception as e:
                logger.error(f"Error controlling scheduler: {e}")
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
        
        logger.info(f"Starting Flutter-ready Nepal Stock API with Intelligent Scheduler on {platform}")
        logger.info(f"Host: {self.flask_host}, Port: {self.flask_port}")
        logger.info(f"Database: {db_type}")
        logger.info("Features: Stock prices, IPO/FPO/Rights tracking, Smart scheduled scraping")
        
        try:
            stock_count = self.price_service.get_stock_count()
            logger.info(f"Stock count: {stock_count}")
            
            issue_stats = self.ipo_service.get_statistics()
            logger.info(f"Issues: {issue_stats['summary']}")
            logger.info(f"By category: {issue_stats['by_category']}")
            
            market_status = self.price_service.get_market_status()
            logger.info(f"Market status: {market_status['status']}")
            
            scheduler_status = self.smart_scheduler.get_scheduler_status()
            logger.info(f"Scheduler running: {scheduler_status.get('scheduler_running', False)}")
            if scheduler_status.get('next_run'):
                logger.info(f"Next scheduled scrape: {scheduler_status['next_run']}")
                
        except Exception as e:
            logger.warning(f"Could not get initial stats: {e}")
        
        # Graceful shutdown handler
        import signal
        import sys
        
        def signal_handler(sig, frame):
            logger.info('Shutting down gracefully...')
            if hasattr(self, 'smart_scheduler'):
                self.smart_scheduler.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
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
        logger.info("Flutter-ready application with intelligent scheduler factory completed successfully")
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