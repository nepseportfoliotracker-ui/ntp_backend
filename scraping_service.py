# nepali_paisa_scraper_separate_tables.py - Version with separate tables for IPO, FPO, Rights

import requests
from bs4 import BeautifulSoup
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import time
import re
import logging
from datetime import datetime, timedelta
import json
import sqlite3
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class EnhancedScrapingService:
    """Enhanced scraping service with separate tables for IPO, FPO, and Rights"""
    
    def __init__(self, price_service, ipo_service):
        self.price_service = price_service
        self.ipo_service = ipo_service
        self.last_scrape_time = None
        self.last_ipo_scrape_time = None
        self.scrape_lock = threading.Lock()
        
        # Stock data sources (keeping existing stock sources)
        self.stock_sources = [
            {
                'name': 'ShareSansar API',
                'url': 'https://www.sharesansar.com/api/live-trading-data',
                'parser': self._parse_sharesansar_api,
                'headers': {'Accept': 'application/json'}
            },
            {
                'name': 'ShareSansar Live Page',
                'url': 'https://www.sharesansar.com/live-trading',
                'parser': self._parse_sharesansar_stocks
            },
            {
                'name': 'MeroLagani Live',
                'url': 'https://merolagani.com/LatestMarket.aspx',
                'parser': self._parse_merolagani
            }
        ]
        
        # Separate API sources for each type - Only latest 8 items each
        self.ipo_sources = [
            {
                'name': 'Nepali Paisa IPO API',
                'url': 'https://nepalipaisa.com/api/GetIpos',
                'parser': self._parse_nepalipaisa_ipo_api,
                'issue_type': 'IPO',
                'table_name': 'ipos',
                'params': {
                    'stockSymbol': '',
                    'pageNo': 1,
                    'itemsPerPage': 8,  # Only get latest 8
                    'pagePerDisplay': 5
                }
            },
            {
                'name': 'Nepali Paisa FPO API',
                'url': 'https://nepalipaisa.com/api/GetFpos',
                'parser': self._parse_nepalipaisa_fpo_api,
                'issue_type': 'FPO',
                'table_name': 'fpos',
                'params': {
                    'stockSymbol': '',
                    'pageNo': 1,
                    'itemsPerPage': 8,  # Only get latest 8
                    'pagePerDisplay': 5
                }
            },
            {
                'name': 'Nepali Paisa Rights/Dividend API',
                'url': 'https://nepalipaisa.com/api/GetDividendRights',
                'parser': self._parse_nepalipaisa_rights_api,
                'issue_type': 'Rights',
                'table_name': 'rights_dividends',
                'params': {
                    'stockSymbol': '',
                    'pageNo': 1,
                    'itemsPerPage': 8,  # Only get latest 8
                    'pagePerDisplay': 5
                }
            }
        ]
        
        # HTTP session configuration
        self.session = self._create_session()
        
    def _create_session(self):
        """Create configured HTTP session"""
        session = requests.Session()
        
        # Disable SSL warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Set headers to mimic real browser
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://nepalipaisa.com/',
            'X-Requested-With': 'XMLHttpRequest'
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
    
    def scrape_ipo_sources(self, force=False):
        """Scrape IPO/FPO/Rights from Nepali Paisa APIs into separate tables"""
        with self.scrape_lock:
            logger.info("Scraping IPO/FPO/Rights from Nepali Paisa APIs into separate tables...")
            
            total_saved = 0
            successful_scrapes = []
            
            for source in self.ipo_sources:
                try:
                    logger.info(f"Scraping {source['issue_type']} from: {source['name']}")
                    issues = self._scrape_api_source(source)
                    
                    if issues:
                        # Save to specific table for this type
                        saved_count = self.ipo_service.save_issues_to_table(
                            issues, 
                            source['table_name'], 
                            source['issue_type'],
                            source['name']
                        )
                        
                        if saved_count > 0:
                            total_saved += saved_count
                            successful_scrapes.append({
                                'source': source['name'],
                                'type': source['issue_type'],
                                'table': source['table_name'],
                                'count': saved_count
                            })
                            logger.info(f"Successfully saved {saved_count} {source['issue_type']} issues to {source['table_name']} table")
                    else:
                        logger.warning(f"No {source['issue_type']} data found from {source['name']}")
                
                except Exception as e:
                    logger.error(f"Error scraping {source['issue_type']} from {source['name']}: {e}")
                    continue
            
            if total_saved > 0:
                self.last_ipo_scrape_time = datetime.now()
                logger.info(f"IPO scraping completed. Total saved: {total_saved} issues across separate tables")
                for scrape in successful_scrapes:
                    logger.info(f"  {scrape['type']}: {scrape['count']} issues in '{scrape['table']}' table")
                
                return total_saved
            else:
                logger.warning("All IPO/FPO/Rights scraping failed - no data found")
                return 0
    
    def _scrape_api_source(self, source):
        """Scrape data from Nepali Paisa API source"""
        try:
            # Add timestamp to prevent caching
            params = source['params'].copy()
            params['_'] = int(time.time() * 1000)
            
            logger.info(f"Requesting {source['url']} with params: {params}")
            
            response = self.session.get(
                source['url'],
                params=params,
                timeout=30,
                verify=False
            )
            
            response.raise_for_status()
            
            if response.status_code == 200:
                return source['parser'](response, source['url'])
            
        except Exception as e:
            logger.error(f"Error scraping API {source['url']}: {e}")
        
        return []
    
    def _parse_nepalipaisa_ipo_api(self, response, url):
        """Parse Nepali Paisa IPO API response"""
        try:
            data = response.json()
            logger.info(f"IPO API Response structure: {type(data)}")
            
            issues_data = []
            
            # Handle the actual Nepali Paisa API response structure
            if isinstance(data, dict) and 'result' in data:
                result_data = data['result']
                if isinstance(result_data, list):
                    items = result_data
                elif isinstance(result_data, dict) and 'data' in result_data:
                    items = result_data['data']
                else:
                    logger.warning(f"Unexpected result structure in IPO API: {type(result_data)}")
                    return []
            elif isinstance(data, list):
                items = data
            else:
                logger.warning(f"Unexpected IPO API response structure: {type(data)}")
                return []
            
            logger.info(f"Processing {len(items)} IPO items (limiting to 8)")
            
            for item in items[:8]:  # Only process latest 8
                try:
                    company_name = item.get('companyName', '').strip()
                    symbol = item.get('stockSymbol', '').strip()
                    
                    if not company_name:
                        continue
                    
                    if not symbol:
                        symbol = DataValidator.extract_symbol_from_company(company_name)
                    
                    units = DataValidator.safe_int(item.get('units', 0))
                    price = DataValidator.safe_float(item.get('issuePrice', 100))
                    
                    # Parse dates
                    open_date = self._parse_api_date(item.get('openingDate'))
                    close_date = self._parse_api_date(item.get('closingDate'))
                    
                    # Get share type
                    share_type = item.get('shareType', 'Ordinary').strip()
                    if not share_type:
                        share_type = 'Ordinary'
                    
                    # Determine status
                    status = self._determine_status_from_api(item, open_date, close_date)
                    
                    issue_manager = item.get('issueManager', '').strip()
                    
                    issue_data = {
                        'company_name': company_name,
                        'symbol': symbol,
                        'share_type': share_type,
                        'units': units,
                        'price': price,
                        'total_amount': units * price if units and price else 0,
                        'open_date': open_date,
                        'close_date': close_date,
                        'status': status,
                        'issue_manager': issue_manager,
                        'source': url,
                        'scraped_at': datetime.now()
                    }
                    
                    issues_data.append(issue_data)
                    logger.info(f"Parsed IPO: {company_name} ({symbol}) - {share_type}")
                    
                except Exception as e:
                    logger.debug(f"Error parsing IPO API item: {e}")
                    continue
            
            logger.info(f"Nepali Paisa IPO API parsing completed: {len(issues_data)} IPOs")
            return issues_data
        
        except Exception as e:
            logger.error(f"Error parsing Nepali Paisa IPO API: {e}")
            return []
    
    def _parse_nepalipaisa_fpo_api(self, response, url):
        """Parse Nepali Paisa FPO API response"""
        try:
            data = response.json()
            logger.info(f"FPO API Response structure: {type(data)}")
            
            issues_data = []
            
            # Handle the actual Nepali Paisa API response structure
            if isinstance(data, dict) and 'result' in data:
                result_data = data['result']
                if isinstance(result_data, list):
                    items = result_data
                elif isinstance(result_data, dict) and 'data' in result_data:
                    items = result_data['data']
                else:
                    logger.warning(f"Unexpected result structure in FPO API: {type(result_data)}")
                    return []
            elif isinstance(data, list):
                items = data
            else:
                logger.warning(f"Unexpected FPO API response structure: {type(data)}")
                return []
            
            logger.info(f"Processing {len(items)} FPO items (limiting to 8)")
            
            for item in items[:8]:  # Only process latest 8
                try:
                    company_name = item.get('companyName', '').strip()
                    symbol = item.get('stockSymbol', '').strip()
                    
                    if not company_name:
                        continue
                    
                    if not symbol:
                        symbol = DataValidator.extract_symbol_from_company(company_name)
                    
                    units = DataValidator.safe_int(item.get('units', 0))
                    price = DataValidator.safe_float(item.get('issuePrice', 100))
                    
                    # Parse dates
                    open_date = self._parse_api_date(item.get('openingDate'))
                    close_date = self._parse_api_date(item.get('closingDate'))
                    
                    # Get share type
                    share_type = item.get('shareType', 'Ordinary').strip()
                    if not share_type:
                        share_type = 'Ordinary'
                    
                    # Determine status
                    status = self._determine_status_from_api(item, open_date, close_date)
                    
                    issue_manager = item.get('issueManager', '').strip()
                    
                    issue_data = {
                        'company_name': company_name,
                        'symbol': symbol,
                        'share_type': share_type,
                        'units': units,
                        'price': price,
                        'total_amount': units * price if units and price else 0,
                        'open_date': open_date,
                        'close_date': close_date,
                        'status': status,
                        'issue_manager': issue_manager,
                        'source': url,
                        'scraped_at': datetime.now()
                    }
                    
                    issues_data.append(issue_data)
                    logger.info(f"Parsed FPO: {company_name} ({symbol}) - {share_type}")
                    
                except Exception as e:
                    logger.debug(f"Error parsing FPO API item: {e}")
                    continue
            
            logger.info(f"Nepali Paisa FPO API parsing completed: {len(issues_data)} FPOs")
            return issues_data
        
        except Exception as e:
            logger.error(f"Error parsing Nepali Paisa FPO API: {e}")
            return []
    
    def _parse_nepalipaisa_rights_api(self, response, url):
        """Parse Nepali Paisa Rights/Dividend API response"""
        try:
            data = response.json()
            logger.info(f"Rights API Response structure: {type(data)}")
            
            issues_data = []
            
            # Handle the actual Nepali Paisa API response structure
            if isinstance(data, dict) and 'result' in data:
                result_data = data['result']
                if isinstance(result_data, list):
                    items = result_data
                elif isinstance(result_data, dict) and 'data' in result_data:
                    items = result_data['data']
                else:
                    logger.warning(f"Unexpected result structure in Rights API: {type(result_data)}")
                    return []
            elif isinstance(data, list):
                items = data
            else:
                logger.warning(f"Unexpected Rights API response structure: {type(data)}")
                return []
            
            logger.info(f"Processing {len(items)} Rights/Dividend items (limiting to 8)")
            
            for item in items[:8]:  # Only process latest 8
                try:
                    company_name = item.get('companyName', '').strip()
                    symbol = item.get('stockSymbol', '').strip()
                    
                    if not company_name:
                        continue
                    
                    if not symbol:
                        symbol = DataValidator.extract_symbol_from_company(company_name)
                    
                    # Check if this is a rights share issue
                    right_share = item.get('rightShare', '').strip()
                    bonus_share = item.get('bonusShare', '').strip()
                    cash_dividend = item.get('cashDividend', '').strip()
                    
                    # Determine issue type based on available data
                    issue_type = 'Rights'
                    if right_share and right_share not in ['', '0', '0%', 'N/A']:
                        issue_type = 'Rights'
                    elif bonus_share or cash_dividend:
                        issue_type = 'Dividend'
                    
                    # Parse dates
                    book_close_date = self._parse_api_date(item.get('bookCloseDate') or item.get('bonusBookCloseDate') or item.get('rightBookCloseDate'))
                    
                    # For rights/dividend, we use book close date as reference
                    status = 'closed'
                    if book_close_date:
                        current_date = datetime.now().date()
                        if current_date < book_close_date:
                            status = 'coming_soon'
                        elif current_date == book_close_date:
                            status = 'open'
                    
                    fiscal_year = item.get('fiscalYear', '').strip()
                    
                    issue_data = {
                        'company_name': company_name,
                        'symbol': symbol,
                        'issue_type': issue_type,  # Rights or Dividend
                        'rights_ratio': right_share,
                        'bonus_share': bonus_share,
                        'cash_dividend': cash_dividend,
                        'book_close_date': book_close_date,
                        'fiscal_year': fiscal_year,
                        'status': status,
                        'source': url,
                        'scraped_at': datetime.now()
                    }
                    
                    issues_data.append(issue_data)
                    logger.info(f"Parsed Rights/Dividend: {company_name} ({symbol}) - {issue_type}")
                    
                except Exception as e:
                    logger.debug(f"Error parsing Rights API item: {e}")
                    continue
            
            logger.info(f"Nepali Paisa Rights API parsing completed: {len(issues_data)} Rights/Dividend items")
            return issues_data
        
        except Exception as e:
            logger.error(f"Error parsing Nepali Paisa Rights API: {e}")
            return []
    
    def _parse_api_date(self, date_str):
        """Parse date from API response"""
        if not date_str or date_str in ['', 'null', None]:
            return None
        
        try:
            # Common API date formats
            date_formats = [
                '%Y-%m-%d',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%Y/%m/%d'
            ]
            
            date_str = str(date_str).strip()
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.date()
                except ValueError:
                    continue
            
            # If no format matches, try to extract date parts
            date_match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
            if date_match:
                year, month, day = map(int, date_match.groups())
                return datetime(year, month, day).date()
            
            logger.debug(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.debug(f"Error parsing API date '{date_str}': {e}")
            return None
    
    def _determine_status_from_api(self, item, open_date, close_date):
        """Determine status from API item and dates"""
        # First check if API provides status directly
        api_status = item.get('status', '').lower().strip()
        
        if api_status in ['open', 'active', 'ongoing']:
            return 'open'
        elif api_status in ['closed', 'ended', 'finished']:
            return 'closed'
        elif api_status in ['coming', 'upcoming', 'announced']:
            return 'coming_soon'
        
        # Fallback to date-based determination
        return self._determine_status_from_dates(open_date, close_date)
    
    def _determine_status_from_dates(self, open_date, close_date):
        """Determine status from open and close dates"""
        current_date = datetime.now().date()
        
        if open_date and close_date:
            if current_date < open_date:
                return 'coming_soon'
            elif open_date <= current_date <= close_date:
                return 'open'
            else:
                return 'closed'
        elif open_date:
            if current_date < open_date:
                return 'coming_soon'
            else:
                estimated_close = open_date + timedelta(days=7)
                if current_date <= estimated_close:
                    return 'open'
                else:
                    return 'closed'
        else:
            return 'coming_soon'
    
    # Keep existing stock parsing methods unchanged
    def scrape_all_sources(self, force=False):
        """Scrape stock data from all available sources"""
        with self.scrape_lock:
            logger.info("Starting stock data scraping from all sources...")
            
            successful_scrapes = []
            total_stocks = 0
            
            for source in self.stock_sources:
                try:
                    logger.info(f"Scraping stocks from: {source['name']}")
                    stocks = self._scrape_source(source)
                    
                    if stocks and len(stocks) >= 20:
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
                        logger.warning(f"Insufficient stock data from {source['name']}: {len(stocks) if stocks else 0} stocks")
                
                except Exception as e:
                    logger.error(f"Error scraping stocks from {source['name']}: {e}")
                    continue
            
            if successful_scrapes:
                self.last_scrape_time = datetime.now()
                logger.info(f"Stock scraping completed successfully. {total_stocks} stocks updated.")
                return total_stocks
            else:
                logger.warning("All stock scraping sources failed")
                return 0
    
    def _scrape_source(self, source):
        """Scrape data from a single source (for stocks)"""
        data = []
        
        headers = self.session.headers.copy()
        if 'headers' in source:
            headers.update(source['headers'])
        
        for verify_ssl in [True, False]:
            try:
                if 'data_params' in source:
                    response = self.session.post(
                        source['url'], 
                        data=source['data_params'],
                        headers=headers,
                        timeout=30,
                        verify=verify_ssl
                    )
                else:
                    response = self.session.get(
                        source['url'], 
                        headers=headers,
                        timeout=30,
                        verify=verify_ssl
                    )
                
                response.raise_for_status()
                
                if response.status_code == 200:
                    data = source['parser'](response, source['url'])
                    if data:
                        return data
                
                break
                
            except requests.exceptions.SSLError:
                if verify_ssl:
                    logger.warning(f"SSL error for {source['url']}, trying without verification")
                    continue
                else:
                    logger.error(f"SSL error even without verification for {source['url']}")
                    break
            except Exception as e:
                logger.warning(f"Error with {source['url']} (SSL verify: {verify_ssl}): {e}")
                if not verify_ssl:
                    break
        
        return data
    
    def _parse_sharesansar_api(self, response, url):
        """Parse ShareSansar API response (if available)"""
        try:
            if response.headers.get('content-type', '').startswith('application/json'):
                data = response.json()
                stocks_data = []
                
                if isinstance(data, dict):
                    if 'data' in data:
                        stock_list = data['data']
                    elif 'result' in data:
                        stock_list = data['result']
                    else:
                        stock_list = [data]
                elif isinstance(data, list):
                    stock_list = data
                else:
                    logger.warning("Unexpected API response format")
                    return []
                
                for item in stock_list:
                    try:
                        symbol = DataValidator.clean_symbol(item.get('symbol', ''))
                        ltp = DataValidator.safe_float(item.get('ltp', 0))
                        change = DataValidator.safe_float(item.get('change', 0))
                        qty = DataValidator.safe_int(item.get('totalQty', 1000))
                        
                        if DataValidator.is_valid_symbol(symbol) and DataValidator.is_valid_price(ltp):
                            stock_data = self._build_stock_data(symbol, ltp, change, qty, url)
                            stocks_data.append(stock_data)
                    
                    except Exception as e:
                        logger.debug(f"Error parsing API stock item: {e}")
                        continue
                
                logger.info(f"ShareSansar API parsing completed: {len(stocks_data)} stocks")
                return stocks_data
            
            else:
                return self._parse_sharesansar_stocks(response, url)
        
        except Exception as e:
            logger.error(f"Error parsing ShareSansar API: {e}")
            return []
    
    def _parse_sharesansar_stocks(self, response, url):
        """Parse ShareSansar website stock data"""
        soup = BeautifulSoup(response.content, 'html.parser')
        stocks_data = []
        
        try:
            stock_table = None
            
            stock_table = soup.find('table', {'id': re.compile(r'live|stock|trading', re.I)}) or \
                         soup.find('table', {'class': re.compile(r'live|stock|trading', re.I)})
            
            if not stock_table:
                tables = soup.find_all('table')
                if tables:
                    stock_table = max(tables, key=lambda t: len(t.find_all('tr')))
            
            if not stock_table:
                logger.warning("No stock table found in ShareSansar")
                return stocks_data
            
            rows = stock_table.find_all('tr')
            if len(rows) < 10:
                logger.warning(f"Insufficient rows in stock table: {len(rows)}")
                return stocks_data
            
            header_row = rows[0]
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
            
            symbol_idx = self._find_column_index(headers, ['symbol', 'stock', 'scrip', 'company'])
            ltp_idx = self._find_column_index(headers, ['ltp', 'price', 'last', 'current'])
            change_idx = self._find_column_index(headers, ['change', 'diff', '+/-'])
            qty_idx = self._find_column_index(headers, ['qty', 'volume', 'turnover'])
            
            if symbol_idx < 0 or ltp_idx < 0:
                logger.warning(f"Required columns not found. Symbol: {symbol_idx}, LTP: {ltp_idx}")
                return stocks_data
            
            for i, row in enumerate(rows[1:], 1):
                cols = row.find_all(['td', 'th'])
                if len(cols) <= max(symbol_idx, ltp_idx):
                    continue
                
                try:
                    symbol_cell = cols[symbol_idx]
                    symbol_link = symbol_cell.find('a')
                    if symbol_link:
                        symbol = DataValidator.clean_symbol(symbol_link.get_text(strip=True))
                    else:
                        symbol = DataValidator.clean_symbol(symbol_cell.get_text(strip=True))
                    
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
                    
                    stock_data = self._build_stock_data(symbol, ltp, change, qty, url)
                    stocks_data.append(stock_data)
                    
                except Exception as e:
                    logger.debug(f"Error parsing ShareSansar stock row {i}: {e}")
                    continue
            
            logger.info(f"ShareSansar stock parsing completed: {len(stocks_data)} stocks")
            return stocks_data
        
        except Exception as e:
            logger.error(f"Error in ShareSansar stock parsing: {e}")
            return []
    
    def _parse_merolagani(self, response, url):
        """Parse MeroLagani website stock data"""
        soup = BeautifulSoup(response.content, 'html.parser')
        stocks_data = []
        
        try:
            stock_table = soup.find('table', {'id': 'headtable'}) or \
                         soup.find('table', class_='table') or \
                         soup.find('table', class_=re.compile(r'stock|market', re.I))
            
            if not stock_table:
                tables = soup.find_all('table')
                if tables:
                    valid_tables = [t for t in tables if len(t.find_all('tr')) > 10]
                    if valid_tables:
                        stock_table = max(valid_tables, key=lambda t: len(t.find_all('tr')))
            
            if not stock_table:
                logger.warning("No stock table found in MeroLagani")
                return stocks_data
            
            rows = stock_table.find_all('tr')
            if len(rows) < 2:
                logger.warning(f"Insufficient rows in MeroLagani table: {len(rows)}")
                return stocks_data
            
            for i, row in enumerate(rows[1:], 1):
                cols = row.find_all(['td', 'th'])
                if len(cols) < 3:
                    continue
                
                try:
                    symbol_col = 1 if len(cols) > 3 else 0
                    ltp_col = 2 if len(cols) > 3 else 1
                    change_col = 3 if len(cols) > 3 else 2
                    
                    symbol_cell = cols[symbol_col]
                    symbol_link = symbol_cell.find('a')
                    if symbol_link:
                        symbol = DataValidator.clean_symbol(symbol_link.get_text(strip=True))
                    else:
                        symbol = DataValidator.clean_symbol(symbol_cell.get_text(strip=True))
                    
                    ltp = DataValidator.safe_float(cols[ltp_col].get_text(strip=True))
                    change = 0.0
                    if len(cols) > change_col:
                        change = DataValidator.safe_float(cols[change_col].get_text(strip=True))
                    
                    if not DataValidator.is_valid_symbol(symbol) or not DataValidator.is_valid_price(ltp):
                        continue
                    
                    qty = 1000
                    if len(cols) > 4:
                        for col_idx in range(4, min(len(cols), 8)):
                            col_text = cols[col_idx].get_text(strip=True)
                            col_value = DataValidator.safe_int(col_text)
                            if col_value > 100:
                                qty = col_value
                                break
                    
                    stock_data = self._build_stock_data(symbol, ltp, change, qty, url)
                    stocks_data.append(stock_data)
                    
                except Exception as e:
                    logger.debug(f"Error parsing MeroLagani stock row {i}: {e}")
                    continue
            
            logger.info(f"MeroLagani stock parsing completed: {len(stocks_data)} stocks")
            return stocks_data
        
        except Exception as e:
            logger.error(f"Error in MeroLagani stock parsing: {e}")
            return []
    
    def _build_stock_data(self, symbol, ltp, change, qty, source_url, high=None, low=None):
        """Build standardized stock data dictionary"""
        change_percent = (change / ltp * 100) if ltp > 0 else 0.0
        prev_close = ltp - change if change != 0 else ltp
        
        if high is None:
            high = ltp + abs(change) if change > 0 else ltp
        if low is None:
            low = ltp - abs(change) if change < 0 else ltp
        
        return {
            'symbol': symbol,
            'company_name': symbol,
            'ltp': round(ltp, 2),
            'change': round(change, 2),
            'change_percent': round(change_percent, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'open_price': round(prev_close, 2),
            'prev_close': round(prev_close, 2),
            'qty': qty,
            'turnover': round(ltp * qty, 2),
            'trades': abs(hash(symbol)) % 100 + 20,
            'source': source_url,
            'scraped_at': datetime.now()
        }
    
    def _find_column_index(self, headers, possible_names):
        """Find column index by matching possible column names"""
        for i, header in enumerate(headers):
            header_lower = header.lower()
            for name in possible_names:
                if name.lower() in header_lower:
                    return i
        return -1
    
    def get_last_scrape_time(self):
        """Get the timestamp of last successful stock scrape"""
        return self.last_scrape_time
    
    def get_last_ipo_scrape_time(self):
        """Get the timestamp of last successful IPO scrape"""
        return self.last_ipo_scrape_time
    
    def scrape_all_data(self, force=False):
        """Scrape both stock and IPO data"""
        stock_count = self.scrape_all_sources(force=force)
        ipo_count = self.scrape_ipo_sources(force=force)
        
        return {
            'stocks': stock_count,
            'ipos': ipo_count,
            'total': stock_count + ipo_count,
            'last_stock_scrape': self.last_scrape_time,
            'last_ipo_scrape': self.last_ipo_scrape_time
        }


class IPOService:
    """Service for handling IPO/FPO/Rights share data with separate tables"""
    
    def __init__(self, db_service):
        self.db_service = db_service
        self._create_tables()
    
    def _create_tables(self):
        """Create separate tables for IPOs, FPOs, and Rights/Dividends"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # IPO Table
            if self.db_service.db_type == 'mysql':
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ipos (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        symbol VARCHAR(20),
                        share_type VARCHAR(50) DEFAULT 'Ordinary',
                        units BIGINT DEFAULT 0,
                        price DECIMAL(10, 2) DEFAULT 0.00,
                        total_amount DECIMAL(15, 2) DEFAULT 0.00,
                        open_date DATE,
                        close_date DATE,
                        status ENUM('coming_soon', 'open', 'closed') DEFAULT 'coming_soon',
                        issue_manager VARCHAR(255),
                        source VARCHAR(500),
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_symbol (symbol),
                        INDEX idx_share_type (share_type),
                        INDEX idx_status (status),
                        INDEX idx_open_date (open_date),
                        UNIQUE KEY unique_ipo (company_name, open_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                ''')
                
                # FPO Table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fpos (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        symbol VARCHAR(20),
                        share_type VARCHAR(50) DEFAULT 'Ordinary',
                        units BIGINT DEFAULT 0,
                        price DECIMAL(10, 2) DEFAULT 0.00,
                        total_amount DECIMAL(15, 2) DEFAULT 0.00,
                        open_date DATE,
                        close_date DATE,
                        status ENUM('coming_soon', 'open', 'closed') DEFAULT 'coming_soon',
                        issue_manager VARCHAR(255),
                        source VARCHAR(500),
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_symbol (symbol),
                        INDEX idx_share_type (share_type),
                        INDEX idx_status (status),
                        INDEX idx_open_date (open_date),
                        UNIQUE KEY unique_fpo (company_name, open_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                ''')
                
                # Rights/Dividends Table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS rights_dividends (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        symbol VARCHAR(20),
                        issue_type ENUM('Rights', 'Dividend') DEFAULT 'Rights',
                        rights_ratio VARCHAR(20),
                        bonus_share VARCHAR(20),
                        cash_dividend VARCHAR(20),
                        book_close_date DATE,
                        fiscal_year VARCHAR(20),
                        status ENUM('coming_soon', 'open', 'closed') DEFAULT 'coming_soon',
                        source VARCHAR(500),
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_symbol (symbol),
                        INDEX idx_issue_type (issue_type),
                        INDEX idx_status (status),
                        INDEX idx_book_close_date (book_close_date),
                        UNIQUE KEY unique_rights (company_name, fiscal_year, issue_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                ''')
            else:
                # SQLite tables
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ipos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_name TEXT NOT NULL,
                        symbol TEXT,
                        share_type TEXT DEFAULT 'Ordinary',
                        units INTEGER DEFAULT 0,
                        price REAL DEFAULT 0.0,
                        total_amount REAL DEFAULT 0.0,
                        open_date DATE,
                        close_date DATE,
                        status TEXT DEFAULT 'coming_soon' CHECK (status IN ('coming_soon', 'open', 'closed')),
                        issue_manager TEXT,
                        source TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(company_name, open_date)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fpos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_name TEXT NOT NULL,
                        symbol TEXT,
                        share_type TEXT DEFAULT 'Ordinary',
                        units INTEGER DEFAULT 0,
                        price REAL DEFAULT 0.0,
                        total_amount REAL DEFAULT 0.0,
                        open_date DATE,
                        close_date DATE,
                        status TEXT DEFAULT 'coming_soon' CHECK (status IN ('coming_soon', 'open', 'closed')),
                        issue_manager TEXT,
                        source TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(company_name, open_date)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS rights_dividends (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_name TEXT NOT NULL,
                        symbol TEXT,
                        issue_type TEXT DEFAULT 'Rights' CHECK (issue_type IN ('Rights', 'Dividend')),
                        rights_ratio TEXT,
                        bonus_share TEXT,
                        cash_dividend TEXT,
                        book_close_date DATE,
                        fiscal_year TEXT,
                        status TEXT DEFAULT 'coming_soon' CHECK (status IN ('coming_soon', 'open', 'closed')),
                        source TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(company_name, fiscal_year, issue_type)
                    )
                ''')
                
                # Create indexes for SQLite
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipos_symbol ON ipos (symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipos_share_type ON ipos (share_type)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipos_status ON ipos (status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipos_open_date ON ipos (open_date)')
                
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_fpos_symbol ON fpos (symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_fpos_share_type ON fpos (share_type)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_fpos_status ON fpos (status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_fpos_open_date ON fpos (open_date)')
                
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_rights_symbol ON rights_dividends (symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_rights_issue_type ON rights_dividends (issue_type)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_rights_status ON rights_dividends (status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_rights_book_date ON rights_dividends (book_close_date)')
            
            conn.commit()
            logger.info(f"Separate IPO/FPO/Rights tables created successfully for {self.db_service.db_type}")
            
        except Exception as e:
            logger.error(f"Error creating separate tables: {e}")
            raise
        finally:
            try:
                conn.close()
            except:
                pass
    
    def save_issues_to_table(self, issues_data, table_name, issue_type, source_name):
        """Save data to specific table and maintain only 8 latest records"""
        if not issues_data:
            return 0
        
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # Clear existing data to maintain only latest 8 records
            cursor.execute(f"DELETE FROM {table_name}")
            
            saved_count = 0
            
            # Insert new data (limited to 8 records)
            for issue in issues_data[:8]:  # Ensure only 8 records max
                try:
                    if table_name == 'ipos' or table_name == 'fpos':
                        if self.db_service.db_type == 'mysql':
                            cursor.execute(f'''
                                INSERT INTO {table_name} (
                                    company_name, symbol, share_type, units, price, 
                                    total_amount, open_date, close_date, status, 
                                    issue_manager, source, scraped_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', (
                                issue['company_name'],
                                issue.get('symbol'),
                                issue.get('share_type', 'Ordinary'),
                                issue.get('units', 0),
                                issue.get('price', 0.0),
                                issue.get('total_amount', 0.0),
                                issue.get('open_date'),
                                issue.get('close_date'),
                                issue.get('status', 'coming_soon'),
                                issue.get('issue_manager'),
                                issue.get('source'),
                                datetime.now()
                            ))
                        else:
                            cursor.execute(f'''
                                INSERT INTO {table_name} (
                                    company_name, symbol, share_type, units, price, 
                                    total_amount, open_date, close_date, status, 
                                    issue_manager, source, scraped_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                issue['company_name'],
                                issue.get('symbol'),
                                issue.get('share_type', 'Ordinary'),
                                issue.get('units', 0),
                                issue.get('price', 0.0),
                                issue.get('total_amount', 0.0),
                                issue.get('open_date'),
                                issue.get('close_date'),
                                issue.get('status', 'coming_soon'),
                                issue.get('issue_manager'),
                                issue.get('source'),
                                datetime.now(),
                                datetime.now()
                            ))
                    
                    elif table_name == 'rights_dividends':
                        if self.db_service.db_type == 'mysql':
                            cursor.execute(f'''
                                INSERT INTO {table_name} (
                                    company_name, symbol, issue_type, rights_ratio, 
                                    bonus_share, cash_dividend, book_close_date, 
                                    fiscal_year, status, source, scraped_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', (
                                issue['company_name'],
                                issue.get('symbol'),
                                issue.get('issue_type', 'Rights'),
                                issue.get('rights_ratio'),
                                issue.get('bonus_share'),
                                issue.get('cash_dividend'),
                                issue.get('book_close_date'),
                                issue.get('fiscal_year'),
                                issue.get('status', 'coming_soon'),
                                issue.get('source'),
                                datetime.now()
                            ))
                        else:
                            cursor.execute(f'''
                                INSERT INTO {table_name} (
                                    company_name, symbol, issue_type, rights_ratio, 
                                    bonus_share, cash_dividend, book_close_date, 
                                    fiscal_year, status, source, scraped_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                issue['company_name'],
                                issue.get('symbol'),
                                issue.get('issue_type', 'Rights'),
                                issue.get('rights_ratio'),
                                issue.get('bonus_share'),
                                issue.get('cash_dividend'),
                                issue.get('book_close_date'),
                                issue.get('fiscal_year'),
                                issue.get('status', 'coming_soon'),
                                issue.get('source'),
                                datetime.now(),
                                datetime.now()
                            ))
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error saving {issue_type} issue {issue.get('company_name', 'Unknown')}: {e}")
                    continue
            
            conn.commit()
            logger.info(f"Saved {saved_count} {issue_type} issues to {table_name} table from {source_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving {issue_type} issues to {table_name}: {e}")
            return 0
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_all_ipos(self):
        """Get all IPO records"""
        return self._get_table_data('ipos')
    
    def get_all_fpos(self):
        """Get all FPO records"""
        return self._get_table_data('fpos')
    
    def get_all_rights_dividends(self):
        """Get all Rights/Dividend records"""
        return self._get_table_data('rights_dividends')
    
    def _get_table_data(self, table_name):
        """Get all data from specified table"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY scraped_at DESC")
            return self._fetch_results(cursor)
            
        except Exception as e:
            logger.error(f"Error getting data from {table_name}: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _fetch_results(self, cursor):
        """Helper method to fetch results from cursor"""
        if self.db_service.db_type == 'mysql':
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        else:
            cursor.row_factory = sqlite3.Row
            results = [dict(row) for row in cursor.fetchall()]
        
        return results
    
    def get_statistics(self):
        """Get statistics about all tables"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            stats = {}
            
            # Count records in each table
            for table in ['ipos', 'fpos', 'rights_dividends']:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                stats[f'{table}_count'] = result[0] if result else 0
            
            # Count by status in each table
            for table in ['ipos', 'fpos', 'rights_dividends']:
                cursor.execute(f'''
                    SELECT status, COUNT(*) as count 
                    FROM {table} 
                    GROUP BY status
                ''')
                stats[f'{table}_by_status'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
        finally:
            try:
                conn.close()
            except:
                pass


class DataValidator:
    """Data validation utilities for scraping"""
    
    @staticmethod
    def clean_symbol(symbol_text):
        """Clean and validate symbol text"""
        if not symbol_text:
            return ""
        cleaned = re.sub(r'[^\w]', '', str(symbol_text)).upper()
        return cleaned
    
    @staticmethod
    def is_valid_symbol(symbol):
        """Check if symbol is valid"""
        if not symbol or len(symbol) < 2 or len(symbol) > 15:
            return False
        if symbol.isdigit():
            return False
        invalid_symbols = {
            'NO', 'SN', 'SR', 'NAME', 'COMPANY', 'SYMBOL', 'PRICE', 'CHANGE', 
            'HIGH', 'LOW', 'QTY', 'VOLUME', 'LTP', 'PERCENT', 'TURNOVER', 'TRADES',
            'OPEN', 'CLOSE', 'PREV', 'LAST', 'TOTAL', 'VALUE'
        }
        return symbol not in invalid_symbols
    
    @staticmethod
    def is_valid_price(price):
        """Check if price is reasonable for Nepal stock market"""
        if not isinstance(price, (int, float)):
            return False
        return 5 <= price <= 10000
    
    @staticmethod
    def safe_float(value):
        """Safely convert string to float"""
        try:
            if value is None:
                return 0.0
            
            if isinstance(value, str):
                cleaned_value = value.replace(',', '').replace('%', '').replace('Rs.', '').replace('NPR', '').strip()
                
                if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
                    cleaned_value = '-' + cleaned_value[1:-1]
                
                if not cleaned_value or cleaned_value in ['-', 'N/A', 'n/a', '']:
                    return 0.0
                
                return float(cleaned_value)
            
            return float(value)
            
        except (ValueError, TypeError):
            return 0.0
    
    @staticmethod
    def safe_int(value):
        """Safely convert string to int"""
        try:
            if value is None:
                return 0
            
            if isinstance(value, str):
                cleaned_value = value.replace(',', '').replace(' ', '').strip()
                if not cleaned_value or cleaned_value in ['-', 'N/A', 'n/a']:
                    return 0
                return int(float(cleaned_value))
            
            return int(value)
            
        except (ValueError, TypeError):
            return 0
    
    @staticmethod
    def extract_symbol_from_company(company_name):
        """Extract or generate symbol from company name"""
        if not company_name:
            return ""
        
        company_name = company_name.strip().upper()
        
        stop_words = ['LIMITED', 'LTD', 'COMPANY', 'CO', 'PRIVATE', 'PVT', 'PUBLIC', 'PUB']
        words = []
        
        for word in company_name.split():
            if word not in stop_words and len(word) > 1:
                words.append(word)
        
        if not words:
            words = company_name.split()[:2]
        
        symbol = ""
        for word in words[:3]:
            if len(word) <= 3:
                symbol += word
            else:
                symbol += word[0]
        
        symbol = symbol[:8]
        
        return symbol if len(symbol) >= 2 else company_name[:4]


# Test function for separate tables
def test_separate_tables_scraping():
    """Test the separate tables scraping service"""
    
    class MockPriceService:
        def save_stock_prices(self, stocks, source):
            print(f"Mock PriceService: Received {len(stocks)} stocks from {source}")
            return len(stocks)
    
    class MockDBService:
        def __init__(self):
            self.db_type = 'sqlite'
        
        def get_connection(self):
            return sqlite3.connect(':memory:')
    
    print("=== Testing Separate Tables Scraping Service ===\n")
    
    db_service = MockDBService()
    price_service = MockPriceService()
    ipo_service = IPOService(db_service)
    
    scraper = EnhancedScrapingService(price_service, ipo_service)
    
    print("1. Testing separate table creation...")
    print(f"   Tables created successfully\n")
    
    print("2. Testing IPO/FPO/Rights scraping (8 items each)...")
    ipo_result = scraper.scrape_ipo_sources()
    print(f"   Total scraping result: {ipo_result} issues\n")
    
    print("3. Testing individual table queries...")
    try:
        ipos = ipo_service.get_all_ipos()
        fpos = ipo_service.get_all_fpos()
        rights = ipo_service.get_all_rights_dividends()
        
        print(f"   IPOs table: {len(ipos)} records")
        print(f"   FPOs table: {len(fpos)} records")
        print(f"   Rights/Dividends table: {len(rights)} records")
        
        stats = ipo_service.get_statistics()
        print(f"   Statistics: {stats}")
    except Exception as e:
        print(f"   Error testing queries: {e}")
    
    print("\n=== Test completed ===")


# Main execution
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('separate_tables_scraping.log')
        ]
    )
    
    # Run the test
    test_separate_tables_scraping()