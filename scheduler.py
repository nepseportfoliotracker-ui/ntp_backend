# scheduler.py - Smart Scheduler with NEPSE History Support

import logging
import hashlib
import json
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

logger = logging.getLogger(__name__)


class SmartScheduler:
    """Intelligent scheduler for market-aware scraping, IPO notifications, and NEPSE history"""
    
    def __init__(self, scraping_service, price_service, db_service, notification_checker, nepse_history_service):
        self.scraping_service = scraping_service
        self.price_service = price_service
        self.db_service = db_service
        self.notification_checker = notification_checker
        self.nepse_history_service = nepse_history_service
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
            data_for_hash = []
            for stock in stocks_data[:50]:
                data_for_hash.append({
                    'symbol': stock.get('symbol', ''),
                    'ltp': stock.get('ltp', 0),
                    'change': stock.get('change', 0),
                    'volume': stock.get('qty', 0)
                })
            
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
            
            market_detected_closed = False
            if scrape_info['scrape_count'] >= 2 and scrape_info['no_change_count'] >= 2:
                market_detected_closed = True
            
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
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
            
            self.market_closed_today = market_detected_closed
            
        except Exception as e:
            logger.error(f"Failed to record scrape result: {e}")
    
    def should_scrape_now(self):
        """Determine if scraping should happen now based on intelligent rules"""
        now = self._get_current_nepal_time()
        
        if not self._is_market_open(now):
            logger.info(f"Skipping scrape - outside market hours or not a market day")
            return False
        
        scrape_info = self._get_today_scrape_info()
        
        if scrape_info['market_closed']:
            logger.info(f"Skipping scrape - market detected as closed today")
            return False
        
        if scrape_info.get('market_confirmed_open'):
            logger.info(f"Market confirmed open - allowing scrape")
            return True
        
        if scrape_info['scrape_count'] < 2:
            logger.info(f"Allowing scrape for market detection (scrape #{scrape_info['scrape_count'] + 1})")
            return True
        
        if scrape_info['no_change_count'] < 2:
            logger.info(f"Allowing scrape - market appears active")
            return True
        
        logger.info(f"Skipping scrape - market appears closed today")
        return False
    
    def scheduled_scrape(self):
        """Execute scheduled scraping with intelligent logic"""
        try:
            logger.info("=== Scheduled Scrape Started ===")
            
            if not self.should_scrape_now():
                return
            
            current_stocks = self.price_service.get_all_stocks()
            current_hash = self._calculate_data_hash(current_stocks)
            
            logger.info("Performing scheduled stock data scrape...")
            stock_count = self.scraping_service.scrape_all_sources(force=True)
            
            updated_stocks = self.price_service.get_all_stocks()
            new_hash = self._calculate_data_hash(updated_stocks)
            
            data_changed = current_hash != new_hash
            
            self._record_scrape_result(data_changed, new_hash)
            
            scrape_info = self._get_today_scrape_info()
            logger.info(f"Scheduled scrape completed: {stock_count} stocks processed")
            logger.info(f"Data changed: {data_changed}")
            logger.info(f"Today's stats: {scrape_info['scrape_count']} scrapes, {scrape_info['no_change_count']} no-change")
            
            if self.market_closed_today:
                logger.info("Market detected as closed - future scrapes will be skipped today")
            
        except Exception as e:
            logger.error(f"Scheduled scrape failed: {e}")
    
    def scheduled_ipo_check(self):
        """Execute scheduled IPO notification check"""
        try:
            logger.info("=== Scheduled IPO Notification Check Started ===")
            
            if not self._is_market_day():
                logger.info("Skipping IPO check - not a market day")
                return
            
            result = self.notification_checker.check_and_notify()
            
            if result['success']:
                logger.info(f"IPO check completed successfully")
                if result.get('new_ipos', 0) > 0:
                    logger.info(f"Sent notifications for {result.get('new_ipos')} IPOs to {result.get('notified', 0)} devices")
                else:
                    logger.info("No new IPOs to notify")
            else:
                logger.error(f"IPO check failed: {result.get('error')}")
                
        except Exception as e:
            logger.error(f"Scheduled IPO check failed: {e}")
    
    def scheduled_nepse_history_scrape(self):
        """Execute scheduled NEPSE history scraping"""
        try:
            logger.info("=== Scheduled NEPSE History Scrape Started ===")
            
            if not self._is_market_day():
                logger.info("Skipping NEPSE history scrape - not a market day")
                return
            
            # Scrape all periods (weekly, monthly, yearly)
            results = self.nepse_history_service.scrape_all_periods(force=False)
            
            logger.info(f"NEPSE history scrape completed: {results}")
            
            # Clean old data
            self.nepse_history_service.clean_old_data()
            logger.info("Old NEPSE history data cleaned")
            
        except Exception as e:
            logger.error(f"Scheduled NEPSE history scrape failed: {e}")
    
    def start(self):
        """Start the intelligent scheduler with all jobs"""
        try:
            # Stock scraping job - every 5 minutes during market hours
            self.scheduler.add_job(
                func=self.scheduled_scrape,
                trigger=CronTrigger(
                    day_of_week='sun,mon,tue,wed,thu',
                    hour='11-14',
                    minute='*/5',
                    timezone=self.nepal_tz
                ),
                id='market_scraper',
                name='Intelligent Market Data Scraper',
                max_instances=1,
                replace_existing=True
            )
            
            # IPO notification check job - every 30 minutes during market hours
            self.scheduler.add_job(
                func=self.scheduled_ipo_check,
                trigger=CronTrigger(
                    day_of_week='sun,mon,tue,wed,thu',
                    hour='11-14',
                    minute='*/30',
                    timezone=self.nepal_tz
                ),
                id='ipo_notification_checker',
                name='IPO Notification Checker',
                max_instances=1,
                replace_existing=True
            )
            
            # NEPSE history scraping job - once daily at 4 PM (after market close) on market days
            self.scheduler.add_job(
                func=self.scheduled_nepse_history_scrape,
                trigger=CronTrigger(
                    day_of_week='sun,mon,tue,wed,thu',  # Sunday to Thursday (market days)
                    hour='16',  # 4:00 PM Nepal Time
                    minute='0',
                    timezone=self.nepal_tz
                ),
                id='nepse_history_scraper',
                name='NEPSE History Scraper (4 PM on Market Days)',
                max_instances=1,
                replace_existing=True
            )
            
            self.scheduler.start()
            logger.info("Intelligent scheduler started successfully")
            logger.info("Stock scrapes: Every 5 minutes during market hours (11 AM - 3 PM, Sun-Thu)")
            logger.info("IPO checks: Every 30 minutes during market hours (11 AM - 3 PM, Sun-Thu)")
            logger.info("NEPSE history: Daily at 4:00 PM (Sun-Thu)")
            
            next_scrape = self.scheduler.get_job('market_scraper').next_run_time
            next_ipo = self.scheduler.get_job('ipo_notification_checker').next_run_time
            next_history = self.scheduler.get_job('nepse_history_scraper').next_run_time
            
            if next_scrape:
                logger.info(f"Next stock scrape: {next_scrape.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            if next_ipo:
                logger.info(f"Next IPO check: {next_ipo.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            if next_history:
                logger.info(f"Next NEPSE history scrape: {next_history.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
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
                'next_stock_scrape': None,
                'next_ipo_check': None,
                'next_nepse_history_scrape': None,
                'current_nepal_time': self._get_current_nepal_time().isoformat(),
                'market_currently_open': self._is_market_open(),
                'today_scrape_info': self._get_today_scrape_info(),
                'market_detected_closed_today': self.market_closed_today
            }
            
            if self.scheduler.running:
                stock_job = self.scheduler.get_job('market_scraper')
                ipo_job = self.scheduler.get_job('ipo_notification_checker')
                history_job = self.scheduler.get_job('nepse_history_scraper')
                
                if stock_job and stock_job.next_run_time:
                    status['next_stock_scrape'] = stock_job.next_run_time.isoformat()
                if ipo_job and ipo_job.next_run_time:
                    status['next_ipo_check'] = ipo_job.next_run_time.isoformat()
                if history_job and history_job.next_run_time:
                    status['next_nepse_history_scrape'] = history_job.next_run_time.isoformat()
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {'error': str(e)}