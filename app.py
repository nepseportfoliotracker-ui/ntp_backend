# app.py - Updated with Market Overview Service

import os
import logging
import signal
import sys
from datetime import datetime
from flask import Flask
from flask_cors import CORS

# Import services
from database_service import DatabaseService
from auth_service import AuthService, create_auth_decorators
from price_service import PriceService
from ipo_service import IPOService
from scraping_service import EnhancedScrapingService
from index_service import IndexService
from push_notification_service import PushNotificationService
from ipo_notification_checker import IPONotificationChecker
from nepse_history_service import NepseHistoryService
from technical_analysis_service import TechnicalAnalysisService
from market_overview_service import MarketOverviewService
from technical_signals_service import TechnicalSignalsService
from routes_trading_signals import register_trading_signals_routes

# Import modular components
from scheduler import SmartScheduler
from routes import register_all_routes
from routes_nepse_history import register_nepse_history_routes
from routes_technical_analysis import register_technical_analysis_routes
from routes_market_overview import register_market_overview_routes

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NepalStockApp:
    
    def __init__(self):
        # Initialize database service with split databases
        # Data DB: ephemeral (stocks, IPOs, prices, history)
        # Auth DB: persistent (API keys, sessions, logs)
        data_db_path = os.environ.get('DATA_DATABASE_PATH', 'nepal_stock_data.db')
        auth_db_path = os.environ.get('AUTH_DATABASE_PATH')  # Will use volume if available
        
        self.db_service = DatabaseService(
            data_db_path=data_db_path,
            auth_db_path=auth_db_path
        )
        
        # Configuration
        self.flask_host = os.environ.get('FLASK_HOST', '0.0.0.0')
        self.flask_port = int(os.environ.get('PORT', 5000))
        self.flask_debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
        
        # Initialize services
        logger.info("Initializing services with split database...")
        
        self.auth_service = AuthService(self.db_service)
        self.price_service = PriceService(self.db_service)
        self.ipo_service = IPOService(self.db_service)
        
        # Initialize Index Service
        self.index_service = IndexService(self.db_service)
        logger.info("Index service initialized - market_indices table created")
        
        # Initialize scraping service with index support
        self.scraping_service = EnhancedScrapingService(
            self.price_service, 
            self.ipo_service,
            index_service=self.index_service
        )
        logger.info("Scraping service initialized with stock, IPO, and index support")
        
        # Initialize push notification service
        self.push_service = PushNotificationService(self.db_service)
        logger.info(f"Push notification service initialized (FCM: {self.push_service.fcm_initialized})")
        
        # Initialize IPO notification checker
        self.notification_checker = IPONotificationChecker(
            self.ipo_service,
            self.push_service,
            self.db_service
        )
        logger.info("IPO notification checker initialized")
        
        # Initialize NEPSE history service
        self.nepse_history_service = NepseHistoryService(self.db_service)
        logger.info("NEPSE history service initialized")
        
        # Initialize Technical Analysis Service
        self.technical_analysis_service = TechnicalAnalysisService(self.nepse_history_service)
        logger.info("Technical analysis service initialized")
        
        # Initialize Market Overview Service
        self.market_overview_service = MarketOverviewService(
            self.db_service,
            self.price_service
        )
        logger.info("Market overview service initialized")
        
        # Initialize Technical Signals Service
        self.technical_signals_service = TechnicalSignalsService(
            self.db_service,
            self.nepse_history_service
        )
        logger.info("Technical signals service initialized")
        
        # Initialize intelligent scheduler
        self.smart_scheduler = SmartScheduler(
            self.scraping_service, 
            self.price_service, 
            self.db_service,
            self.notification_checker,
            self.nepse_history_service,
            self.market_overview_service
        )
        
        # Add signals service to scheduler
        self.smart_scheduler.signals_service = self.technical_signals_service
        
        # Create Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Store services in app config
        self.app.config['db_service'] = self.db_service
        self.app.config['auth_service'] = self.auth_service
        self.app.config['price_service'] = self.price_service
        self.app.config['ipo_service'] = self.ipo_service
        self.app.config['index_service'] = self.index_service
        self.app.config['scraping_service'] = self.scraping_service
        self.app.config['push_service'] = self.push_service
        self.app.config['notification_checker'] = self.notification_checker
        self.app.config['smart_scheduler'] = self.smart_scheduler
        self.app.config['nepse_history_service'] = self.nepse_history_service
        self.app.config['technical_analysis_service'] = self.technical_analysis_service
        self.app.config['market_overview_service'] = self.market_overview_service
        self.app.config['technical_signals_service'] = self.technical_signals_service
        
        # Create authentication decorators
        self.require_auth, self.require_admin = create_auth_decorators(self.auth_service)
        self.app.config['require_auth'] = self.require_auth
        self.app.config['require_admin'] = self.require_admin
        
        # Register all routes
        register_all_routes(self.app)
        register_nepse_history_routes(self.app)
        register_technical_analysis_routes(self.app)
        register_market_overview_routes(self.app)
        register_trading_signals_routes(self.app)
        
        # Initialize data
        self._initialize_app()
    
    def _initialize_app(self):
        logger.info("Initializing Nepal Stock API with split database...")
        
        # Show database info
        db_info = self.db_service.get_database_info()
        logger.info("=" * 60)
        logger.info("DATABASE CONFIGURATION:")
        for db_name, info in db_info.get('databases', {}).items():
            logger.info(f"  {db_name.upper()}: {info.get('description', '')}")
            logger.info(f"    Path: {info.get('path', 'N/A')}")
            logger.info(f"    Persistent: {info.get('persistent', False)}")
            logger.info(f"    Size: {info.get('size_mb', 0)} MB")
        logger.info("=" * 60)
        
        # Check for admin keys
        self._ensure_admin_key()
        
        # Run initial data scrape (stocks, IPOs, and indices)
        logger.info("Running initial stock, IPO, and market indices data scrape...")
        try:
            initial_counts = self.scraping_service.scrape_all_data(force=True)
            logger.info(f"Application initialized:")
            logger.info(f"  - Stocks: {initial_counts['stocks']}")
            logger.info(f"  - Market Indices: {initial_counts['indices']}")
            logger.info(f"  - IPOs/FPOs/Rights: {initial_counts['ipos']}")
        except Exception as e:
            logger.warning(f"Initial scrape failed: {e}")
        
        # Run initial NEPSE history scrape
        logger.info("Running initial NEPSE history scrape...")
        try:
            history_results = self.nepse_history_service.scrape_all_periods(force=True)
            logger.info(f"NEPSE history initialized: {history_results}")
        except Exception as e:
            logger.warning(f"Initial NEPSE history scrape failed: {e}")

        # Generate initial trading signals
        logger.info("Generating initial trading signals...")
        try:
            signals_result = self.technical_signals_service.generate_signals(
                ema_period=3,
                min_holding_days=3
            )
            if signals_result['success']:
                logger.info(f"Initial trading signals generated successfully")
                if signals_result['latest_signal']:
                    sig = signals_result['latest_signal']
                    logger.info(f"  Latest signal: {sig['type'].upper()} on {sig['date']} at price {sig['price']}")
                    logger.info(f"  EMA value: {sig['ema']}")
                    if sig.get('days_since_last'):
                        logger.info(f"  Days since last signal: {sig['days_since_last']}")
                
                # Show trade statistics
                if signals_result.get('trades'):
                    trades = signals_result['trades']
                    logger.info(f"  Completed trades: {trades['completed']}")
                    logger.info(f"  Win rate: {trades['win_rate']}%")
                    logger.info(f"  Total return: {trades['total_return']}%")
            else:
                logger.warning(f"Signal generation failed: {signals_result.get('error')}")
        except Exception as e:
            logger.warning(f"Initial signal generation failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Calculate initial market overview
        logger.info("Calculating initial market overview...")
        try:
            initial_overview = self.market_overview_service.save_overview_snapshot(limit=10)
            if initial_overview:
                logger.info(f"Initial market overview snapshot created (ID: {initial_overview})")
        except Exception as e:
            logger.warning(f"Initial market overview calculation failed: {e}")
        
        # Start intelligent scheduler
        try:
            self.smart_scheduler.start()
        except Exception as e:
            logger.error(f"Failed to start intelligent scheduler: {e}")
    
    def _ensure_admin_key(self):
        """Ensure at least one admin key exists in auth database"""
        try:
            conn = self.db_service.get_auth_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM api_keys WHERE key_type = "admin" AND is_active = TRUE')
            admin_count = cursor.fetchone()[0]
            conn.close()
        except Exception as e:
            logger.info(f"Admin key check failed (tables may not exist yet): {e}")
            admin_count = 0
        
        if admin_count == 0:
            logger.info("No admin keys found, creating initial admin key...")
            
            initial_admin = self.auth_service.generate_api_key(
                key_type='admin',
                created_by='system',
                description='Initial admin key (persistent auth DB)'
            )
            if initial_admin:
                logger.info("=" * 60)
                logger.info("ADMIN KEY CREATED (PERSISTENT AUTH DATABASE):")
                logger.info(f"Key ID: {initial_admin['key_id']}")
                logger.info(f"API Key: {initial_admin['api_key']}")
                logger.info("SAVE THIS KEY SECURELY - IT WON'T BE SHOWN AGAIN!")
                logger.info("This key will persist across deployments.")
                logger.info("=" * 60)
        else:
            logger.info(f"Found {admin_count} active admin key(s) in persistent auth database")
    
    def run(self):
        """Run the Flask application"""
        logger.info("Starting Nepal Stock API with Split Database Architecture")
        logger.info(f"Host: {self.flask_host}, Port: {self.flask_port}")
        logger.info(f"Auth Database: Persistent (survives deployments)")
        logger.info(f"Data Database: Ephemeral (fresh data on each deployment)")
        
        # Graceful shutdown handler
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


# For Gunicorn
def create_app():
    """Factory function for Gunicorn"""
    try:
        nepal_app = NepalStockApp()
        logger.info("Application factory completed successfully")
        return nepal_app.app
    except Exception as e:
        logger.error(f"Application factory failed: {e}")
        raise


# Create the app instance
app = create_app()

# For local development
if __name__ == '__main__':
    try:
        nepal_app = NepalStockApp()
        nepal_app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        raise