# app.py - Main Application Entry Point (Updated with NEPSE History)

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
from push_notification_service import PushNotificationService
from ipo_notification_checker import IPONotificationChecker
from nepse_history_service import NepseHistoryService
from technical_analysis_service import TechnicalAnalysisService

# Import modular components
from scheduler import SmartScheduler
from routes import register_all_routes
from routes_nepse_history import register_nepse_history_routes
from routes_technical_analysis import register_technical_analysis_routes

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NepalStockApp:
    """Flutter-ready application with push notifications and NEPSE history"""
    
    def __init__(self):
        # Initialize database service (SQLite only)
        db_path = os.environ.get('DATABASE_PATH', 'nepal_stock.db')
        self.db_service = DatabaseService(db_path)
        
        # Configuration
        self.flask_host = os.environ.get('FLASK_HOST', '0.0.0.0')
        self.flask_port = int(os.environ.get('PORT', 5000))
        self.flask_debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
        
        # Initialize services
        logger.info("Initializing services for SQLite...")
        
        self.auth_service = AuthService(self.db_service)
        self.price_service = PriceService(self.db_service)
        self.ipo_service = IPOService(self.db_service)
        self.scraping_service = EnhancedScrapingService(self.price_service, self.ipo_service)
        
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
        
        # Initialize intelligent scheduler with NEPSE history support
        self.smart_scheduler = SmartScheduler(
            self.scraping_service, 
            self.price_service, 
            self.db_service,
            self.notification_checker,
            self.nepse_history_service
        )
        
        # Create Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Store services in app config for route access
        self.app.config['db_service'] = self.db_service
        self.app.config['auth_service'] = self.auth_service
        self.app.config['price_service'] = self.price_service
        self.app.config['ipo_service'] = self.ipo_service
        self.app.config['scraping_service'] = self.scraping_service
        self.app.config['push_service'] = self.push_service
        self.app.config['notification_checker'] = self.notification_checker
        self.app.config['smart_scheduler'] = self.smart_scheduler
        self.app.config['nepse_history_service'] = self.nepse_history_service
        
        # Create authentication decorators
        self.require_auth, self.require_admin = create_auth_decorators(self.auth_service)
        self.app.config['require_auth'] = self.require_auth
        self.app.config['require_admin'] = self.require_admin
        
        # Register all routes
        register_all_routes(self.app)
        register_nepse_history_routes(self.app)
        
        # Initialize data
        self._initialize_app()
    
    def _initialize_app(self):
        """Initialize application with default data"""
        logger.info("Initializing Flutter-ready Nepal Stock API with SQLite...")
        logger.info(f"SQLite database path: {self.db_service.db_path}")
        
        # Check for admin keys
        self._ensure_admin_key()
        
        # Run initial data scrape
        logger.info("Running initial stock and IPO data scrape...")
        try:
            initial_counts = self.scraping_service.scrape_all_data(force=True)
            logger.info(f"Application initialized with {initial_counts['stocks']} stocks and {initial_counts['ipos']} IPOs/FPOs/Rights")
        except Exception as e:
            logger.warning(f"Initial scrape failed: {e}")
        
        # Run initial NEPSE history scrape
        logger.info("Running initial NEPSE history scrape...")
        try:
            history_results = self.nepse_history_service.scrape_all_periods(force=True)
            logger.info(f"NEPSE history initialized: {history_results}")
        except Exception as e:
            logger.warning(f"Initial NEPSE history scrape failed: {e}")
        
        # Start intelligent scheduler
        try:
            self.smart_scheduler.start()
        except Exception as e:
            logger.error(f"Failed to start intelligent scheduler: {e}")
    
    def _ensure_admin_key(self):
        """Ensure at least one admin key exists"""
        try:
            conn = self.db_service.get_connection()
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
                description='Initial SQLite admin key'
            )
            if initial_admin:
                logger.info("=" * 60)
                logger.info("ADMIN KEY CREATED (SQLite):")
                logger.info(f"Key ID: {initial_admin['key_id']}")
                logger.info(f"API Key: {initial_admin['api_key']}")
                logger.info("SAVE THIS KEY SECURELY - IT WON'T BE SHOWN AGAIN!")
                logger.info("=" * 60)
    
    def run(self):
        """Run the Flask application"""
        logger.info("Starting Flutter-ready Nepal Stock API with Push Notifications & NEPSE History")
        logger.info(f"Host: {self.flask_host}, Port: {self.flask_port}")
        logger.info(f"Database: SQLite at {self.db_service.db_path}")
        logger.info(f"Push Notifications: {'Enabled' if self.push_service.fcm_initialized else 'Disabled (FCM not configured)'}")
        logger.info(f"NEPSE History: Enabled (weekly, monthly, yearly)")
        
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