# scraping_service.py - Web Scraping Service

import requests
from bs4 import BeautifulSoup
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import time
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ScrapingService:
    """Handle all web scraping operations for stock data"""
    
    def __init__(self, price_service):
        self.price_service = price_service
        self.last_scrape_time = None
        self.scrape_lock = threading.Lock()
        
        # Data sources configuration
        self.sources = [
            {
                'name': 'ShareSansar Live',
                'url': 'https://www.sharesansar.com/live-trading',
                'parser': self._parse_sharesansar
            },
            {
                'name': 'ShareSansar Today',
                'url': 'https://www.sharesansar.com/today-share-price',
                'parser': self._parse_sharesansar
            },
            {
                'name': 'MeroLagani',
                'url': 'https://merolagani.com/LatestMarket.aspx',
                'parser': self._parse_merolagani
            }
        ]
        
        # HTTP session configuration
        self.session = self._create_session()
        
    def _create_session(self):
        """Create configured HTTP session"""
        session = requests.Session()
        
        # Disable SSL warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Set headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Configure retries
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def scrape_all_sources(self, force=False):
        """Scrape stock data from all available sources"""
        with self.scrape_lock:
            logger.info("Starting stock data scraping from all sources...")
            
            successful_scrapes = []
            total_stocks = 0
            
            for source in self.sources:
                try:
                    logger.info(f"Scraping from: {source['name']}")
                    stocks = self._scrape_source(source)
                    
                    if stocks and len(stocks) >= 50:  # Minimum threshold
                        count = self.price_service.save_stock_prices(stocks, source['name'])
                        if count > 0:
                            successful_scrapes.append({
                                'source': source['name'],
                                'count': count
                            })
                            total_stocks = max(total_stocks, count)
                            logger.info(f"Successfully scraped {count} stocks from {source['name']}")
                            break  # Use first successful source
                    else:
                        logger.warning(f"Insufficient data from {source['name']}: {len(stocks) if stocks else 0} stocks")
                
                except Exception as e:
                    logger.error(f"Error scraping {source['name']}: {e}")
                    continue
            
            if successful_scrapes:
                self.last_scrape_time = datetime.now()
                logger.info(f"Scraping completed successfully. {total_stocks} stocks updated.")
                return total_stocks
            else:
                logger.warning("All scraping sources failed, using sample data")
                return self._populate_sample_data()
    
    def _scrape_source(self, source):
        """Scrape data from a single source"""
        stocks = []
        
        # Try with SSL verification first, then without
        for verify_ssl in [True, False]:
            try:
                response = self.session.get(
                    source['url'], 
                    timeout=30,
                    verify=verify_ssl
                )
                response.raise_for_status()
                
                if response.status_code == 200 and len(response.content) > 1000:
                    stocks = source['parser'](response.content, source['url'])
                    if stocks:
                        return stocks
                
                break  # Break verify_ssl loop if successful
                
            except requests.exceptions.SSLError:
                if verify_ssl:
                    logger.warning(f"SSL error for {source['url']}, trying without verification")
                    continue
                else:
                    logger.error(f"SSL error even without verification for {source['url']}")
                    break
            except Exception as e:
                logger.warning(f"Error with {source['url']} (SSL verify: {verify_ssl}): {e}")
                break
        
        return stocks
    
    def _parse_sharesansar(self, content, url):
        """Parse ShareSansar website data"""
        soup = BeautifulSoup(content, 'html.parser')
        stocks_data = []
        
        try:
            # Look for tables containing stock data
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 10:  # Skip small tables
                    continue
                
                # Check if this looks like a stock table
                header_row = rows[0] if rows else None
                if not header_row:
                    continue
                
                header_text = header_row.get_text().lower()
                if not any(keyword in header_text for keyword in ['symbol', 'ltp', 'price', 'change']):
                    continue
                
                logger.info("Found stock table in ShareSansar")
                
                # Get column indices
                header_cells = header_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True).lower() for cell in header_cells]
                
                symbol_idx = self._find_column_index(headers, ['symbol', 'stock', 'scrip'])
                ltp_idx = self._find_column_index(headers, ['ltp', 'price', 'last'])
                change_idx = self._find_column_index(headers, ['change', 'diff'])
                qty_idx = self._find_column_index(headers, ['qty', 'volume'])
                
                if symbol_idx < 0 or ltp_idx < 0:
                    continue
                
                # Parse data rows
                for row in rows[1:]:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) <= max(symbol_idx, ltp_idx):
                        continue
                    
                    try:
                        symbol = DataValidator.clean_symbol(cols[symbol_idx].get_text(strip=True))
                        ltp = DataValidator.safe_float(cols[ltp_idx].get_text(strip=True))
                        
                        if not DataValidator.is_valid_symbol(symbol) or not DataValidator.is_valid_price(ltp):
                            continue
                        
                        change = 0.0
                        if change_idx >= 0 and len(cols) > change_idx:
                            change = DataValidator.safe_float(cols[change_idx].get_text(strip=True))
                        
                        qty = 1000
                        if qty_idx >= 0 and len(cols) > qty_idx:
                            qty = DataValidator.safe_int(cols[qty_idx].get_text(strip=True))
                            if qty <= 0:
                                qty = 1000
                        
                        # Build stock data
                        stock_data = self._build_stock_data(symbol, ltp, change, qty, url)
                        stocks_data.append(stock_data)
                        
                    except Exception as e:
                        logger.debug(f"Error parsing ShareSansar row: {e}")
                        continue
                
                if stocks_data:
                    logger.info(f"ShareSansar parsing found {len(stocks_data)} stocks")
                    return stocks_data
        
        except Exception as e:
            logger.error(f"Error in ShareSansar parsing: {e}")
        
        return stocks_data
    
    def _parse_merolagani(self, content, url):
        """Parse MeroLagani website data"""
        soup = BeautifulSoup(content, 'html.parser')
        stocks_data = []
        
        try:
            # Look for main market data table
            table = soup.find('table', {'id': 'headtable'}) or soup.find('table', class_='table')
            
            if not table:
                # Try all tables
                tables = soup.find_all('table')
                for t in tables:
                    rows = t.find_all('tr')
                    if len(rows) > 20:  # Large table likely to be stock data
                        table = t
                        break
            
            if table:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    return stocks_data
                
                # Parse rows (skip header)
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) < 3:
                        continue
                    
                    try:
                        # Basic parsing - adjust indices based on MeroLagani structure
                        symbol = DataValidator.clean_symbol(
                            cols[1].get_text(strip=True) if len(cols) > 1 
                            else cols[0].get_text(strip=True)
                        )
                        ltp = DataValidator.safe_float(
                            cols[2].get_text(strip=True) if len(cols) > 2 else 0
                        )
                        change = DataValidator.safe_float(
                            cols[3].get_text(strip=True) if len(cols) > 3 else 0
                        )
                        
                        if not DataValidator.is_valid_symbol(symbol) or not DataValidator.is_valid_price(ltp):
                            continue
                        
                        stock_data = self._build_stock_data(symbol, ltp, change, 1000, url)
                        stocks_data.append(stock_data)
                        
                    except Exception as e:
                        continue
        
        except Exception as e:
            logger.error(f"Error in MeroLagani parsing: {e}")
        
        return stocks_data
    
    def _build_stock_data(self, symbol, ltp, change, qty, source_url):
        """Build standardized stock data dictionary"""
        change_percent = (change / ltp * 100) if ltp > 0 else 0.0
        prev_close = ltp - change if change != 0 else ltp
        
        return {
            'symbol': symbol,
            'company_name': symbol,  # Use symbol as company name by default
            'ltp': ltp,
            'change': change,
            'change_percent': change_percent,
            'high': ltp + abs(change) if change > 0 else ltp,
            'low': ltp - abs(change) if change < 0 else ltp,
            'open_price': prev_close,
            'prev_close': prev_close,
            'qty': qty,
            'turnover': ltp * qty,
            'trades': abs(hash(symbol)) % 100 + 20,  # Generate reasonable fake trades count
            'source': source_url
        }
    
    def _find_column_index(self, headers, possible_names):
        """Find column index by matching possible column names"""
        for i, header in enumerate(headers):
            for name in possible_names:
                if name in header:
                    return i
        return -1
    
    def _populate_sample_data(self):
        """Populate sample data when scraping fails"""
        logger.info("Populating sample stock data...")
        
        sample_stocks = [
            {'symbol': 'NABIL', 'company_name': 'NABIL BANK LIMITED', 'ltp': 1420.0, 'change': 15.0},
            {'symbol': 'ADBL', 'company_name': 'AGRICULTURE DEVELOPMENT BANK LIMITED', 'ltp': 350.0, 'change': -5.0},
            {'symbol': 'EBL', 'company_name': 'EVEREST BANK LIMITED', 'ltp': 720.0, 'change': 12.0},
            {'symbol': 'NBL', 'company_name': 'NEPAL BANK LIMITED', 'ltp': 410.0, 'change': -8.0},
            {'symbol': 'SBI', 'company_name': 'NEPAL SBI BANK LIMITED', 'ltp': 460.0, 'change': 7.0},
            {'symbol': 'KBL', 'company_name': 'KUMARI BANK LIMITED', 'ltp': 310.0, 'change': -3.0},
            {'symbol': 'HBL', 'company_name': 'HIMALAYAN BANK LIMITED', 'ltp': 560.0, 'change': 10.0},
            {'symbol': 'HIDCL', 'company_name': 'HYDROELECTRICITY INVESTMENT AND DEVELOPMENT COMPANY LIMITED', 'ltp': 305.0, 'change': 2.0},
            {'symbol': 'NFS', 'company_name': 'NEPAL FINANCE LTD', 'ltp': 790.0, 'change': 18.0},
            {'symbol': 'CORBL', 'company_name': 'CORPORATE DEVELOPMENT BANK LIMITED', 'ltp': 2250.0, 'change': -25.0},
            {'symbol': 'BOKL', 'company_name': 'BANK OF KATHMANDU LIMITED', 'ltp': 385.0, 'change': 8.0},
            {'symbol': 'NICA', 'company_name': 'NIC ASIA BANK LIMITED', 'ltp': 950.0, 'change': -12.0},
            {'symbol': 'PRVU', 'company_name': 'PRABHU BANK LIMITED', 'ltp': 390.0, 'change': 5.0},
            {'symbol': 'SANIMA', 'company_name': 'SANIMA BANK LIMITED', 'ltp': 365.0, 'change': -2.0},
            {'symbol': 'MBL', 'company_name': 'MACHHAPUCHCHHRE BANK LIMITED', 'ltp': 425.0, 'change': 9.0},
        ]
        
        enriched_stocks = []
        for stock in sample_stocks:
            base_price = stock['ltp']
            change = stock['change']
            high = base_price + abs(change) + 5
            low = base_price - abs(change) - 5
            qty = abs(hash(stock['symbol'])) % 5000 + 1000
            
            enriched_stocks.append({
                'symbol': stock['symbol'],
                'company_name': stock['company_name'],
                'ltp': base_price,
                'change': change,
                'change_percent': (change / base_price) * 100,
                'high': high,
                'low': low,
                'open_price': base_price - change,
                'prev_close': base_price - change,
                'qty': qty,
                'turnover': base_price * qty,
                'trades': abs(hash(stock['symbol'])) % 100 + 20,
                'source': 'Sample Data'
            })
        
        count = self.price_service.save_stock_prices(enriched_stocks, 'Sample Data')
        self.last_scrape_time = datetime.now()
        return count
    
    def get_last_scrape_time(self):
        """Get the timestamp of last successful scrape"""
        return self.last_scrape_time


class DataValidator:
    """Data validation utilities for scraping"""
    
    @staticmethod
    def clean_symbol(symbol_text):
        """Clean and validate symbol text"""
        if not symbol_text:
            return ""
        return re.sub(r'[^\w]', '', symbol_text).upper()
    
    @staticmethod
    def is_valid_symbol(symbol):
        """Check if symbol is valid"""
        if not symbol or len(symbol) < 2 or len(symbol) > 10:
            return False
        if symbol.isdigit():
            return False
        # Exclude obvious non-symbols
        invalid_symbols = {'NO', 'SN', 'SR', 'NAME', 'PRICE', 'CHANGE', 'HIGH', 'LOW', 'QTY', 'VOLUME'}
        return symbol not in invalid_symbols
    
    @staticmethod
    def is_valid_price(price):
        """Check if price is reasonable for Nepal stock market"""
        return 10 <= price <= 5000 if price > 0 else False
    
    @staticmethod
    def safe_float(value):
        """Safely convert string to float"""
        try:
            if value is None:
                return 0.0
            # Remove commas, percentage signs, currency symbols
            cleaned_value = str(value).replace(',', '').replace('%', '').replace('Rs.', '').replace('NPR', '').strip()
            # Handle negative values in parentheses
            if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
                cleaned_value = '-' + cleaned_value[1:-1]
            return float(cleaned_value) if cleaned_value and cleaned_value != '-' else 0.0
        except:
            return 0.0
    
    @staticmethod
    def safe_int(value):
        """Safely convert string to int"""
        try:
            if value is None:
                return 0
            cleaned_value = str(value).replace(',', '').strip()
            return int(float(cleaned_value)) if cleaned_value and cleaned_value != '-' else 0
        except:
            return 0