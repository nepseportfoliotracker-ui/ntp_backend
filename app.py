# app.py - Updated with EMA Signal Service

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
from price_history_service import PriceHistoryService
from ema_signal_service import EMASignalService

# Import modular components
from scheduler import SmartScheduler
from routes import register_all_routes
from routes_nepse_history import register_nepse_history_routes
from routes_technical_analysis import register_technical_analysis_routes
from routes_market_overview import register_market_overview_routes
from routes_price_history import register_price_history_routes
from routes_ema_signals import register_ema_signal_routes

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
        
        # Initialize Persistent Price History Service
        self.price_history_service = PriceHistoryService(self.db_service)
        logger.info("Persistent price history service initialized (30-day rolling window)")
        
        # Initialize EMA Signal Service (4-day EMA with 2-day minimum holding period)
        self.ema_signal_service = EMASignalService(
            self.db_service,
            self.nepse_history_service,
            ema_period=4,
            min_holding_days=2
        )
        logger.info("EMA signal service initialized (4-day EMA, 2-day holding period)")
        
        # Initialize intelligent scheduler with EMA signal service
        self.smart_scheduler = SmartScheduler(
            self.scraping_service, 
            self.price_service, 
            self.db_service,
            self.notification_checker,
            self.nepse_history_service,
            self.market_overview_service,
            self.price_history_service,
            self.ema_signal_service
        )
        
        # Add signals service to scheduler for compatibility
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
        self.app.config['price_history_service'] = self.price_history_service
        self.app.config['ema_signal_service'] = self.ema_signal_service
        
        # Create authentication decorators
        self.require_auth, self.require_admin = create_auth_decorators(self.auth_service)
        self.app.config['require_auth'] = self.require_auth
        self.app.config['require_admin'] = self.require_admin
        
        # Register all routes
        register_all_routes(self.app)
        register_nepse_history_routes(self.app)
        register_technical_analysis_routes(self.app)
        register_market_overview_routes(self.app)
        register_price_history_routes(self.app)
        register_ema_signal_routes(self.app)
        
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
        
        # Show price history database info
        history_info = self.price_history_service.get_history_database_info()
        logger.info("=" * 60)
        logger.info("PRICE HISTORY DATABASE (PERSISTENT):")
        logger.info(f"  Path: {history_info.get('path', 'N/A')}")
        logger.info(f"  Persistent: {history_info.get('persistent', False)}")
        logger.info(f"  Volume Mount: {history_info.get('volume_mount', 'N/A')}")
        logger.info(f"  Size: {history_info.get('size_mb', 0)} MB")
        logger.info(f"  Records: {history_info.get('total_records', 0)}")
        logger.info(f"  Symbols: {history_info.get('total_symbols', 0)}")
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

        # Generate initial technical signals (3-day EMA)
        logger.info("Generating initial technical trading signals (3-day EMA)...")
        try:
            signals_result = self.technical_signals_service.generate_signals(
                ema_period=3,
                min_holding_days=3
            )
            if signals_result['success']:
                logger.info(f"Initial technical signals generated successfully")
                if signals_result['latest_signal']:
                    sig = signals_result['latest_signal']
                    logger.info(f"  Latest signal: {sig['type'].upper()} on {sig['date']} at price {sig['price']}")
                    logger.info(f"  EMA value: {sig['ema']}")
                
                if signals_result.get('trades'):
                    trades = signals_result['trades']
                    logger.info(f"  Completed trades: {trades['completed']}")
                    logger.info(f"  Win rate: {trades['win_rate']}%")
            else:
                logger.warning(f"Technical signal generation failed: {signals_result.get('error')}")
        except Exception as e:
            logger.warning(f"Initial technical signal generation failed: {e}")
        
        # Generate initial EMA trading signals (4-day EMA with 2-day holding)
        logger.info("Generating initial EMA trading signals (4-day EMA, 2-day holding)...")
        try:
            ema_result = self.ema_signal_service.generate_signals(force=True)
            
            if ema_result['success']:
                logger.info(f"Initial EMA signals generated successfully")
                logger.info(f"Signals generated: {ema_result['signals_generated']}")
                
                if ema_result['latest_signal']:
                    signal = ema_result['latest_signal']
                    logger.info(f"Latest EMA Signal: {signal['signal']} on {signal['date']}")
                    logger.info(f"  Price: {signal['price']:.2f}")
                    logger.info(f"  EMA (4-day): {signal['ema']:.2f}")
                    logger.info(f"  Can Trade: {signal['can_trade']}")
                    
                    if signal['holding_period_active']:
                        logger.info(f"  Holding Period: {signal['holding_days_remaining']} days remaining")
                    
                    if signal['days_since_last_signal']:
                        logger.info(f"  Days Since Last Signal: {signal['days_since_last_signal']}")
                
                if ema_result['trade_summary']:
                    summary = ema_result['trade_summary']
                    logger.info(f"EMA Trade Statistics:")
                    logger.info(f"  Total Signals: {summary.get('total_signals', 0)}")
                    logger.info(f"  Buy Signals: {summary.get('buy_signals', 0)}")
                    logger.info(f"  Sell Signals: {summary.get('sell_signals', 0)}")
                    logger.info(f"  Total Trades: {summary.get('total_trades', 0)}")
                    logger.info(f"  Win Rate: {summary.get('win_rate', 0):.1f}%")
                    logger.info(f"  Avg P&L: {summary.get('avg_profit_loss', 0):.2f}%")
                    logger.info(f"  Total Return: {summary.get('total_return', 0):.2f}%")
            else:
                logger.warning(f"EMA signal generation failed: {ema_result.get('error', 'Unknown error')}")
        except Exception as e:
            logger.warning(f"Initial EMA signal generation failed: {e}")
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
        
        # Save initial daily prices to persistent history on startup
        logger.info("Saving initial daily prices to persistent history on startup...")
        try:
            stocks_data = self.price_service.get_all_stocks()
            if stocks_data:
                result = self.price_history_service.save_daily_prices(stocks_data)
                if result['success']:
                    logger.info(f"Initial price history save completed on startup")
                    logger.info(f"  Saved: {result['saved']} stocks")
                    logger.info(f"  Skipped: {result['skipped']} stocks")
                    logger.info(f"  Rotated: {result['rotated']} old records")
                    logger.info(f"  Date: {result['date']}")
                else:
                    logger.warning(f"Initial price history save failed: {result.get('error')}")
            else:
                logger.warning("No stock data available for initial price history save")
        except Exception as e:
            logger.warning(f"Initial price history save failed: {e}")
        
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
        logger.info(f"Price History: Persistent 30-day rolling window")
        logger.info(f"EMA Signal Service: 4-day EMA with 2-day minimum holding period")
        
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