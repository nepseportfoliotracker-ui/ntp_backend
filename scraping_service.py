# scraping_service.py - Enhanced version with market indices support

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
    """Enhanced scraping service with stock, IPO, and market indices support"""
    
    def __init__(self, price_service, ipo_service, index_service=None):
        self.price_service = price_service
        self.ipo_service = ipo_service
        self.index_service = index_service
        self.last_scrape_time = None
        self.last_ipo_scrape_time = None
        self.last_index_scrape_time = None
        self.scrape_lock = threading.Lock()
        
        # Stock data sources
        self.stock_sources = [
            {
                'name': 'ShareSansar Live Page',
                'url': 'https://www.sharesansar.com/live-trading',
                'parser': self._parse_sharesansar_stocks
            }
        ]
        
        # Index data sources
        self.index_sources = [
            {
                'name': 'ShareSansar Live Page',
                'url': 'https://www.sharesansar.com/live-trading',
                'parser': self._parse_sharesansar_indices
            }
        ]
        
        # IPO/FPO/Rights sources
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
                    'itemsPerPage': 8,
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
                    'itemsPerPage': 8,
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
                    'itemsPerPage': 8,
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
    
    # ==================== INDEX SCRAPING METHODS ====================
    
    def scrape_market_indices(self, force=False):
        """Scrape market indices from ShareSansar"""
        if not self.index_service:
            logger.warning("Index service not configured, skipping index scraping")
            return 0
        
        with self.scrape_lock:
            logger.info("Starting market indices scraping...")
            
            for source in self.index_sources:
                try:
                    logger.info(f"Scraping indices from: {source['name']}")
                    indices = self._scrape_source_for_indices(source)
                    
                    if indices and len(indices) >= 5:
                        count = self.index_service.save_indices(indices, source['name'])
                        if count > 0:
                            self.last_index_scrape_time = datetime.now()
                            logger.info(f"Successfully scraped {count} market indices")
                            return count
                    else:
                        logger.warning(f"Insufficient index data from {source['name']}: {len(indices) if indices else 0} indices")
                
                except Exception as e:
                    logger.error(f"Error scraping indices from {source['name']}: {e}")
                    continue
            
            logger.warning("All index scraping sources failed")
            return 0
    
    def _scrape_source_for_indices(self, source):
        """Scrape data from a single source for indices"""
        data = []
        
        headers = self.session.headers.copy()
        if 'headers' in source:
            headers.update(source['headers'])
        
        for verify_ssl in [True, False]:
            try:
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
    
    def _parse_sharesansar_indices(self, response, url):
        """Parse ShareSansar market indices from live trading page"""
        soup = BeautifulSoup(response.content, 'html.parser')
        indices_data = []
        
        try:
            # The indices appear before the stock table in a specific section
            # Look for the index data (usually in a table or list format)
            
            # Method 1: Find all text patterns that match index format
            page_text = soup.get_text()
            
            # Known indices to look for
            index_patterns = {
                'NEPSE Index': r'NEPSE\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Banking SubIndex': r'Banking\s+SubIndex\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Development Bank': r'Development\s+Bank\s+\w*\.?\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Finance Index': r'Finance\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Hotels And Tourism': r'Hotels\s+And\s+Tourism\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'HydroPower Index': r'HydroPower\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Investment': r'Investment\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Life Insurance': r'Life\s+Insurance\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Manufacturing And Pr.': r'Manufacturing\s+And\s+Pr\.\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Microfinance Index': r'Microfinance\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Non Life Insurance': r'Non\s+Life\s+Insurance\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Others Index': r'Others\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Float Index': r'Float\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Sensitive Index': r'Sensitive\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Sensitive Float': r'Sensitive\s+Float\s+\w*\.?\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Trading Index': r'Trading\s+Index\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?',
                'Mutual Fund': r'Mutual\s+Fund\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([-+]?\d+\.?\d*)%?'
            }
            
            for index_name, pattern in index_patterns.items():
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    try:
                        turnover = DataValidator.safe_float(match.group(1).replace(',', ''))
                        value = DataValidator.safe_float(match.group(2).replace(',', ''))
                        change_percent = DataValidator.safe_float(match.group(3))
                        
                        # Calculate point change from percentage
                        point_change = (value * change_percent) / 100 if change_percent else 0
                        prev_value = value - point_change
                        
                        index_data = {
                            'index_name': index_name,
                            'index_value': round(value, 2),
                            'point_change': round(point_change, 2),
                            'percent_change': round(change_percent, 2),
                            'turnover': round(turnover, 2),
                            'prev_close': round(prev_value, 2),
                            'source': url,
                            'scraped_at': datetime.now()
                        }
                        
                        indices_data.append(index_data)
                        logger.info(f"Parsed index: {index_name} - Value: {value}, Change: {change_percent}%")
                        
                    except Exception as e:
                        logger.debug(f"Error parsing index {index_name}: {e}")
                        continue
            
            logger.info(f"ShareSansar index parsing completed: {len(indices_data)} indices")
            return indices_data
        
        except Exception as e:
            logger.error(f"Error in ShareSansar index parsing: {e}")
            return []
    
    # ==================== IPO SCRAPING METHODS ====================
    
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
            
            for item in items[:8]:
                try:
                    company_name = item.get('companyName', '').strip()
                    symbol = item.get('stockSymbol', '').strip()
                    
                    if not company_name:
                        continue
                    
                    if not symbol:
                        symbol = DataValidator.extract_symbol_from_company(company_name)
                    
                    units = DataValidator.safe_int(item.get('units', 0))
                    price = DataValidator.safe_float(item.get('issuePrice', 100))
                    
                    open_date = self._parse_api_date(item.get('openingDate'))
                    close_date = self._parse_api_date(item.get('closingDate'))
                    
                    share_type = item.get('shareType', 'Ordinary').strip()
                    if not share_type:
                        share_type = 'Ordinary'
                    
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
                    logger.info(f"Parsed IPO: {company_name} ({symbol}) - {share_type} - Status: {status}")
                    
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
            
            for item in items[:8]:
                try:
                    company_name = item.get('companyName', '').strip()
                    symbol = item.get('stockSymbol', '').strip()
                    
                    if not company_name:
                        continue
                    
                    if not symbol:
                        symbol = DataValidator.extract_symbol_from_company(company_name)
                    
                    units = DataValidator.safe_int(item.get('units', 0))
                    price = DataValidator.safe_float(item.get('issuePrice', 100))
                    
                    open_date = self._parse_api_date(item.get('openingDate'))
                    close_date = self._parse_api_date(item.get('closingDate'))
                    
                    share_type = item.get('shareType', 'Ordinary').strip()
                    if not share_type:
                        share_type = 'Ordinary'
                    
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
                    logger.info(f"Parsed FPO: {company_name} ({symbol}) - {share_type} - Status: {status}")
                    
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
            
            for item in items[:8]:
                try:
                    company_name = item.get('companyName', '').strip()
                    symbol = item.get('stockSymbol', '').strip()
                    
                    if not company_name:
                        continue
                    
                    if not symbol:
                        symbol = DataValidator.extract_symbol_from_company(company_name)
                    
                    right_share = item.get('rightShare', '').strip()
                    bonus_share = item.get('bonusShare', '').strip()
                    cash_dividend = item.get('cashDividend', '').strip()
                    
                    issue_type = 'Rights'
                    if right_share and right_share not in ['', '0', '0%', 'N/A']:
                        issue_type = 'Rights'
                    elif bonus_share or cash_dividend:
                        issue_type = 'Dividend'
                    
                    book_close_date = self._parse_api_date(item.get('bookCloseDate') or item.get('bonusBookCloseDate') or item.get('rightBookCloseDate'))
                    
                    status = self._determine_rights_status(item, book_close_date)
                    
                    fiscal_year = item.get('fiscalYear', '').strip()
                    
                    issue_data = {
                        'company_name': company_name,
                        'symbol': symbol,
                        'issue_type': issue_type,
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
                    logger.info(f"Parsed Rights/Dividend: {company_name} ({symbol}) - {issue_type} - Status: {status}")
                    
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
        api_status = item.get('status', '').lower().strip()
        
        if api_status in ['open', 'active', 'ongoing']:
            return 'open'
        elif api_status in ['closed', 'ended', 'finished', 'completed']:
            return 'closed'
        elif api_status in ['nearing', 'coming', 'upcoming', 'announced', 'coming soon', 'comingsoon']:
            return 'coming_soon'
        
        return self._determine_status_from_dates(open_date, close_date)
    
    def _determine_rights_status(self, item, book_close_date):
        """Determine status for rights/dividend issues"""
        api_status = item.get('status', '').lower().strip()
        
        if api_status in ['open', 'active', 'ongoing']:
            return 'open'
        elif api_status in ['closed', 'ended', 'finished', 'completed']:
            return 'closed'
        elif api_status in ['nearing', 'coming', 'upcoming', 'announced', 'coming soon', 'comingsoon']:
            return 'coming_soon'
        
        if book_close_date:
            current_date = datetime.now().date()
            days_until = (book_close_date - current_date).days
            
            if days_until < -7:
                return 'closed'
            elif days_until <= 0:
                return 'open'
            elif days_until <= 7:
                return 'coming_soon'
            else:
                return 'coming_soon'
        
        return 'coming_soon'
    
    def _determine_status_from_dates(self, open_date, close_date):
        """Determine status from open and close dates"""
        current_date = datetime.now().date()
        
        if open_date and close_date:
            if current_date < open_date:
                days_until = (open_date - current_date).days
                if days_until <= 7:
                    return 'coming_soon'
                else:
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
    
    # ==================== STOCK SCRAPING METHODS ====================
    
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
                            break
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
    
    # ==================== COMBINED SCRAPING ====================
    
    def scrape_all_data(self, force=False):
        """Scrape stocks, indices, and IPO data"""
        stock_count = self.scrape_all_sources(force=force)
        index_count = self.scrape_market_indices(force=force)
        ipo_count = self.scrape_ipo_sources(force=force)
        
        return {
            'stocks': stock_count,
            'indices': index_count,
            'ipos': ipo_count,
            'total': stock_count + index_count + ipo_count,
            'last_stock_scrape': self.last_scrape_time,
            'last_index_scrape': self.last_index_scrape_time,
            'last_ipo_scrape': self.last_ipo_scrape_time
        }
    
    def get_last_scrape_time(self):
        """Get the timestamp of last successful stock scrape"""
        return self.last_scrape_time
    
    def get_last_index_scrape_time(self):
        """Get the timestamp of last successful index scrape"""
        return self.last_index_scrape_time
    
    def get_last_ipo_scrape_time(self):
        """Get the timestamp of last successful IPO scrape"""
        return self.last_ipo_scrape_time


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
        return 5 <= price <= 50000
    
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


# Example Index Service class (you'll need to implement this)
class IndexService:
    """Service for managing market indices"""
    
    def __init__(self, db_service):
        self.db_service = db_service
        self._ensure_indices_table()
    
    def _ensure_indices_table(self):
        """Create indices table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS market_indices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name TEXT NOT NULL,
            index_value REAL NOT NULL,
            point_change REAL,
            percent_change REAL,
            turnover REAL,
            prev_close REAL,
            source TEXT,
            scraped_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(index_name, scraped_at)
        )
        """
        
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error creating indices table: {e}")
    
    def save_indices(self, indices, source_name):
        """Save market indices to database"""
        if not indices:
            return 0
        
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            saved_count = 0
            for index_data in indices:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO market_indices 
                        (index_name, index_value, point_change, percent_change, 
                         turnover, prev_close, source, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        index_data['index_name'],
                        index_data['index_value'],
                        index_data['point_change'],
                        index_data['percent_change'],
                        index_data['turnover'],
                        index_data['prev_close'],
                        index_data['source'],
                        index_data['scraped_at']
                    ))
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving index {index_data.get('index_name')}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully saved {saved_count} indices from {source_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving indices to database: {e}")
            return 0
    
    def get_latest_indices(self):
        """Get the latest market indices"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT index_name, index_value, point_change, percent_change,
                       turnover, prev_close, scraped_at
                FROM market_indices
                WHERE scraped_at = (SELECT MAX(scraped_at) FROM market_indices)
                ORDER BY 
                    CASE 
                        WHEN index_name = 'NEPSE Index' THEN 0
                        ELSE 1
                    END,
                    index_name
            """)
            
            rows = cursor.fetchall()
            conn.close()
            
            indices = []
            for row in rows:
                indices.append({
                    'index_name': row[0],
                    'index_value': row[1],
                    'point_change': row[2],
                    'percent_change': row[3],
                    'turnover': row[4],
                    'prev_close': row[5],
                    'scraped_at': row[6]
                })
            
            return indices
            
        except Exception as e:
            logger.error(f"Error fetching latest indices: {e}")
            return []


# Test function
def test_index_scraping():
    """Test the index scraping functionality"""
    
    class MockPriceService:
        def save_stock_prices(self, stocks, source):
            print(f"Mock PriceService: Received {len(stocks)} stocks from {source}")
            return len(stocks)
    

    class MockDBService:
        def __init__(self):
            self.db_type = 'sqlite'
            self.conn = None
        
        def get_connection(self):
            if self.conn is None:
                self.conn = sqlite3.connect(':memory:')
            return self.conn
    
    print("=== Testing Index Scraping Service ===\n")
    
    db_service = MockDBService()
    price_service = MockPriceService()
    
    # Import IPOService from ipo_service.py if available
    try:
        from ipo_service import IPOService
    except ImportError:
        print("Note: IPOService not imported, using mock")
        class IPOService:
            def __init__(self, db_service):
                self.db_service = db_service
            def save_issues_to_table(self, issues, table, type, source):
                return len(issues)
    
    ipo_service = IPOService(db_service)
    index_service = IndexService(db_service)
    
    scraper = EnhancedScrapingService(price_service, ipo_service, index_service)
    
    print("1. Configuration:")
    print(f"   Stock sources: {len(scraper.stock_sources)}")
    print(f"   Index sources: {len(scraper.index_sources)}")
    print(f"   IPO sources: {len(scraper.ipo_sources)}\n")
    
    print("2. Testing index scraping...")
    index_count = scraper.scrape_market_indices()
    print(f"   Scraped {index_count} market indices\n")
    
    if index_count > 0:
        print("3. Fetching latest indices...")
        latest_indices = index_service.get_latest_indices()
        print(f"   Found {len(latest_indices)} indices:")
        for idx in latest_indices[:5]:
            print(f"   - {idx['index_name']}: {idx['index_value']} ({idx['percent_change']:+.2f}%)")
    
    print("\n=== Test completed ===")


# Main execution
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('scraping_service.log')
        ]
    )
    
    # Run the test
    test_index_scraping()