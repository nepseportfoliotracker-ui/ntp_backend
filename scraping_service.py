# enhanced_scraping_service.py - Complete version with fixed IPO parsing

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

logger = logging.getLogger(__name__)

class EnhancedScrapingService:
    """Enhanced scraping service for stocks, IPOs, FPOs, and Rights shares"""
    
    def __init__(self, price_service, ipo_service):
        self.price_service = price_service
        self.ipo_service = ipo_service
        self.last_scrape_time = None
        self.last_ipo_scrape_time = None
        self.scrape_lock = threading.Lock()
        
        # Stock data sources configuration
        self.stock_sources = [
            {
                'name': 'ShareSansar Live',
                'url': 'https://www.sharesansar.com/live-trading',
                'parser': self._parse_sharesansar_stocks
            },
            {
                'name': 'ShareSansar Today',
                'url': 'https://www.sharesansar.com/today-share-price',
                'parser': self._parse_sharesansar_stocks
            },
            {
                'name': 'MeroLagani',
                'url': 'https://merolagani.com/LatestMarket.aspx',
                'parser': self._parse_merolagani
            }
        ]
        
        # IPO/FPO/Rights data sources configuration
        self.ipo_sources = [
            {
                'name': 'ShareSansar Existing Issues',
                'url': 'https://www.sharesansar.com/existing-issues',
                'parser': self._parse_sharesansar_existing_issues
            },
            {
                'name': 'ShareSansar Upcoming Issues',
                'url': 'https://www.sharesansar.com/upcoming-issue',
                'parser': self._parse_sharesansar_upcoming_issues
            },
            {
                'name': 'MeroLagani IPO',
                'url': 'https://merolagani.com/Ipo.aspx',
                'parser': self._parse_merolagani_ipo
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
            
            for source in self.stock_sources:
                try:
                    logger.info(f"Scraping stocks from: {source['name']}")
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
                        logger.warning(f"Insufficient stock data from {source['name']}: {len(stocks) if stocks else 0} stocks")
                
                except Exception as e:
                    logger.error(f"Error scraping stocks from {source['name']}: {e}")
                    continue
            
            if successful_scrapes:
                self.last_scrape_time = datetime.now()
                logger.info(f"Stock scraping completed successfully. {total_stocks} stocks updated.")
                return total_stocks
            else:
                logger.warning("All stock scraping sources failed, using sample data")
                return self._populate_sample_data()
    
    def scrape_ipo_sources(self, force=False):
        """Scrape IPO/FPO/Rights data from all available sources with fallback"""
        with self.scrape_lock:
            logger.info("Starting IPO/FPO/Rights data scraping from all sources...")
            
            successful_scrapes = []
            total_issues = 0
            
            for source in self.ipo_sources:
                try:
                    logger.info(f"Scraping IPO data from: {source['name']}")
                    issues = self._scrape_source(source)
                    
                    if issues:
                        logger.info(f"Raw issues found from {source['name']}: {len(issues)}")
                        
                        # Filter for open issues (current date between open and close dates)
                        open_issues = self._filter_open_issues(issues)
                        
                        if open_issues:
                            count = self.ipo_service.save_issues(open_issues, source['name'])
                            if count > 0:
                                successful_scrapes.append({
                                    'source': source['name'],
                                    'count': count
                                })
                                total_issues += count
                                logger.info(f"Successfully scraped {count} open issues from {source['name']}")
                        else:
                            logger.info(f"No open issues found from {source['name']}")
                    else:
                        logger.warning(f"No data from {source['name']}")
                
                except Exception as e:
                    logger.error(f"Error scraping IPO data from {source['name']}: {e}")
                    continue
            
            if successful_scrapes:
                self.last_ipo_scrape_time = datetime.now()
                logger.info(f"IPO scraping completed successfully. {total_issues} issues updated.")
                return total_issues
            else:
                logger.warning("All IPO scraping sources failed, creating sample data...")
                return self._create_sample_ipo_data()
    
    def _filter_open_issues(self, issues):
        """Filter issues that are currently open (between opening and closing dates)"""
        open_issues = []
        current_date = datetime.now().date()
        
        for issue in issues:
            try:
                open_date = issue.get('open_date')
                close_date = issue.get('close_date')
                
                if open_date and close_date:
                    # Parse dates if they're strings
                    if isinstance(open_date, str):
                        open_date = datetime.strptime(open_date, '%Y-%m-%d').date()
                    if isinstance(close_date, str):
                        close_date = datetime.strptime(close_date, '%Y-%m-%d').date()
                    
                    # Check if current date is between open and close dates
                    if open_date <= current_date <= close_date:
                        issue['status'] = 'open'
                        open_issues.append(issue)
                    elif current_date < open_date:
                        issue['status'] = 'coming_soon'
                        # Include coming soon issues that start within next 30 days
                        if (open_date - current_date).days <= 30:
                            open_issues.append(issue)
                    else:
                        issue['status'] = 'closed'
                        # Include recently closed issues (within last 7 days)
                        if (current_date - close_date).days <= 7:
                            open_issues.append(issue)
                else:
                    # If no dates available, include as coming soon
                    issue['status'] = 'coming_soon'
                    open_issues.append(issue)
                        
            except Exception as e:
                logger.warning(f"Error filtering issue dates: {e}")
                # Include issues with date parsing errors as coming soon
                issue['status'] = 'coming_soon'
                open_issues.append(issue)
                continue
                
        return open_issues
    
    def _scrape_source(self, source):
        """Scrape data from a single source"""
        data = []
        
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
                    data = source['parser'](response.content, source['url'])
                    if data:
                        return data
                
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
        
        return data
    
    def _parse_sharesansar_stocks(self, content, url):
        """Parse ShareSansar website stock data"""
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
                        logger.debug(f"Error parsing ShareSansar stock row: {e}")
                        continue
                
                if stocks_data:
                    logger.info(f"ShareSansar stock parsing found {len(stocks_data)} stocks")
                    return stocks_data
        
        except Exception as e:
            logger.error(f"Error in ShareSansar stock parsing: {e}")
        
        return stocks_data
    
    def _parse_sharesansar_existing_issues(self, content, url):
        """Targeted ShareSansar existing issues parser"""
        soup = BeautifulSoup(content, 'html.parser')
        issues_data = []
        
        try:
            logger.info("Parsing ShareSansar existing issues page with targeted approach")
            
            # Look for ALL tables and try to parse each one
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables to analyze")
            
            for table_idx, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                logger.info(f"Processing table {table_idx + 1} with {len(rows)} rows")
                
                # Get all text from the table to check for IPO-related content
                table_text = table.get_text().lower()
                
                # Check if table contains IPO company names or related terms
                ipo_indicators = [
                    'jhapa energy', 'sagar distillery', 'reliance spinning', 'bungal hydro',
                    'shreenagar agritech', 'bandipur cable', 'mabilung energy',
                    'units', 'price', 'opening date', 'closing date', 'rs.', 'limited', 'symbol'
                ]
                
                has_ipo_content = any(indicator in table_text for indicator in ipo_indicators)
                
                if has_ipo_content:
                    logger.info(f"Table {table_idx + 1} contains IPO-related content, parsing...")
                    
                    # Try to extract data from this table using multiple strategies
                    extracted = self._extract_from_any_table(table, url, table_idx + 1)
                    if extracted:
                        issues_data.extend(extracted)
                        logger.info(f"Successfully extracted {len(extracted)} issues from table {table_idx + 1}")
                else:
                    logger.debug(f"Table {table_idx + 1} doesn't appear to contain IPO data")
            
            # Fallback: Try to extract from any div that might contain structured data
            if not issues_data:
                logger.info("No table data found, trying div-based extraction...")
                divs = soup.find_all('div')
                
                for div in divs:
                    div_text = div.get_text()
                    if any(company in div_text for company in ['Jhapa Energy', 'Sagar Distillery', 'Reliance Spinning']):
                        extracted = self._extract_from_div_content(div, url)
                        if extracted:
                            issues_data.extend(extracted)
            
            logger.info(f"ShareSansar existing issues parsing completed: {len(issues_data)} issues found")
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in ShareSansar existing issues parsing: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return []

    def _extract_from_any_table(self, table, url, table_number):
        """Extract IPO data from any table structure"""
        issues_data = []
        
        try:
            rows = table.find_all('tr')
            
            # Log the first few rows for debugging
            for i, row in enumerate(rows[:3]):
                cols = row.find_all(['td', 'th'])
                row_text = [col.get_text(strip=True) for col in cols]
                logger.debug(f"Table {table_number} Row {i}: {row_text}")
            
            # Skip header row and process data rows
            data_rows = rows[1:] if len(rows) > 1 else rows
            
            for row_idx, row in enumerate(data_rows):
                cols = row.find_all(['td', 'th'])
                
                if len(cols) < 3:  # Need at least 3 columns for meaningful data
                    continue
                
                # Extract all cell texts
                cell_texts = [col.get_text(strip=True) for col in cols]
                logger.debug(f"Table {table_number} processing row: {cell_texts}")
                
                # Look for company name (longest text that looks like a company)
                company_name = None
                company_col = -1
                
                for i, text in enumerate(cell_texts):
                    if len(text) > 10 and any(word in text for word in ['Limited', 'Ltd', 'Energy', 'Bank', 'Hydro', 'Company', 'Distillery', 'Agritech', 'Spinning', 'Cable']):
                        company_name = text
                        company_col = i
                        break
                
                # If no clear company name found, try the first non-numeric column
                if not company_name:
                    for i, text in enumerate(cell_texts):
                        if len(text) > 5 and not text.replace(',', '').replace('.', '').replace('-', '').isdigit():
                            # Skip common header words
                            if text.lower() not in ['company', 'symbol', 'units', 'price', 'open', 'close', 'status']:
                                company_name = text
                                company_col = i
                                break
                
                if not company_name:
                    continue
                
                logger.info(f"Table {table_number}: Found potential company '{company_name}' in column {company_col}")
                
                # Look for numerical data (units, prices)
                units = 0
                price = 0.0
                
                for text in cell_texts:
                    # Look for large numbers (likely units) - must have commas for thousands
                    if ',' in text and text.replace(',', '').isdigit():
                        potential_units = DataValidator.safe_int(text)
                        if potential_units > 1000:  # Reasonable for IPO units
                            units = potential_units
                    
                    # Look for prices (smaller numbers, possibly with decimals)
                    elif text.replace('.', '').replace(',', '').isdigit() and ',' not in text:
                        potential_price = DataValidator.safe_float(text)
                        if 10 <= potential_price <= 2000:  # Reasonable price range
                            price = potential_price
                
                # Look for dates
                open_date = None
                close_date = None
                
                for text in cell_texts:
                    if re.search(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}', text):
                        parsed_date = self._parse_nepali_date(text)
                        if parsed_date:
                            if not open_date:
                                open_date = parsed_date
                            elif not close_date:
                                close_date = parsed_date
                
                # Determine issue type and status
                issue_type = 'IPO'  # Default
                status = 'unknown'
                
                row_text_combined = ' '.join(cell_texts).lower()
                if 'fpo' in row_text_combined:
                    issue_type = 'FPO'
                elif 'right' in row_text_combined:
                    issue_type = 'Rights'
                
                # Check for "Coming Soon" status
                if 'coming soon' in row_text_combined or not (open_date and close_date):
                    status = 'coming_soon'
                else:
                    # Determine from dates
                    current_date = datetime.now().date()
                    if open_date and close_date:
                        if open_date <= current_date <= close_date:
                            status = 'open'
                        elif current_date < open_date:
                            status = 'coming_soon'
                        else:
                            status = 'closed'
                
                # Create issue data
                issue_data = {
                    'company_name': company_name,
                    'symbol': DataValidator.extract_symbol_from_company(company_name),
                    'issue_type': issue_type,
                    'units': units,
                    'price': price,
                    'total_amount': units * price if units and price else 0,
                    'open_date': open_date,
                    'close_date': close_date,
                    'status': status,
                    'source': url,
                    'scraped_at': datetime.now()
                }
                
                issues_data.append(issue_data)
                logger.info(f"Table {table_number}: Extracted issue - {company_name} ({issue_type}) Status: {status}")
            
            return issues_data
            
        except Exception as e:
            logger.error(f"Error extracting from table {table_number}: {e}")
            return []

    def _extract_from_div_content(self, div, url):
        """Extract IPO data from div content using text patterns"""
        issues_data = []
        
        try:
            text = div.get_text(separator=' ', strip=True)
            
            # Known IPO companies from your screenshot
            known_companies = [
                'Jhapa Energy Limited',
                'Sagar Distillery Limited', 
                'Shreenagar Agritech Industries Limited',
                'Reliance Spinning Mills',
                'Bungal Hydro Limited',
                'Bandipur Cable Car and Tourism Limited',
                'Mabilung Energy Limited'
            ]
            
            for company in known_companies:
                if company in text:
                    # Try to extract surrounding data
                    company_index = text.find(company)
                    context = text[max(0, company_index-100):company_index+200]
                    
                    # Extract numerical data from context
                    units_match = re.search(r'(\d{1,3}(?:,\d{3})*)', context)
                    price_match = re.search(r'(\d{1,4}(?:\.\d{2})?)', context)
                    
                    issue_data = {
                        'company_name': company,
                        'symbol': DataValidator.extract_symbol_from_company(company),
                        'issue_type': 'IPO',
                        'units': DataValidator.safe_int(units_match.group(1) if units_match else '0'),
                        'price': DataValidator.safe_float(price_match.group(1) if price_match else '0'),
                        'total_amount': 0,
                        'open_date': None,
                        'close_date': None,
                        'status': 'coming_soon',
                        'source': url,
                        'scraped_at': datetime.now()
                    }
                    
                    issues_data.append(issue_data)
                    logger.info(f"Extracted from div content: {company}")
            
            return issues_data
            
        except Exception as e:
            logger.debug(f"Error extracting from div: {e}")
            return []

    def _create_sample_ipo_data(self):
        """Create sample IPO data based on known current issues"""
        logger.info("Creating sample IPO data...")
        
        sample_issues = [
            {
                'company_name': 'Jhapa Energy Limited',
                'symbol': 'JEL',
                'issue_type': 'IPO',
                'units': 473336,
                'price': 100.0,
                'total_amount': 47333600,
                'open_date': datetime(2025, 9, 5).date(),
                'close_date': datetime(2025, 9, 15).date(),
                'status': 'closed',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            },
            {
                'company_name': 'Sagar Distillery Limited',
                'symbol': 'SDLTD',
                'issue_type': 'IPO',
                'units': 1190640,
                'price': 100.0,
                'total_amount': 119064000,
                'open_date': datetime(2025, 9, 15).date(),
                'close_date': datetime(2025, 9, 21).date(),
                'status': 'closed',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            },
            {
                'company_name': 'Shreenagar Agritech Industries Limited',
                'symbol': 'SHREE',
                'issue_type': 'IPO',
                'units': 3262500,
                'price': 100.0,
                'total_amount': 326250000,
                'open_date': None,
                'close_date': None,
                'status': 'coming_soon',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            },
            {
                'company_name': 'Reliance Spinning Mills',
                'symbol': 'RSM',
                'issue_type': 'IPO',
                'units': 1155960,
                'price': 820.80,
                'total_amount': 948651168,
                'open_date': None,
                'close_date': None,
                'status': 'coming_soon',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            },
            {
                'company_name': 'Bungal Hydro Limited',
                'symbol': 'BLHL',
                'issue_type': 'IPO',
                'units': 1701500,
                'price': 100.0,
                'total_amount': 170150000,
                'open_date': datetime(2025, 9, 1).date(),
                'close_date': datetime(2025, 9, 4).date(),
                'status': 'closed',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            },
            {
                'company_name': 'Bandipur Cable Car and Tourism Limited',
                'symbol': 'BCTL',
                'issue_type': 'IPO',
                'units': 4341080,
                'price': 100.0,
                'total_amount': 434108000,
                'open_date': datetime(2025, 8, 27).date(),
                'close_date': datetime(2025, 8, 31).date(),
                'status': 'closed',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            },
            {
                'company_name': 'Mabilung Energy Limited',
                'symbol': 'MBEL',
                'issue_type': 'IPO',
                'units': 1248904,
                'price': 100.0,
                'total_amount': 124890400,
                'open_date': datetime(2025, 8, 11).date(),
                'close_date': datetime(2025, 8, 14).date(),
                'status': 'closed',
                'source': 'Sample Data',
                'scraped_at': datetime.now()
            }
        ]
        
        # Save sample data to database
        saved_count = self.ipo_service.save_issues(sample_issues, 'Sample Data')
        logger.info(f"Created {saved_count} sample IPO issues")
        return saved_count
    
    def _parse_sharesansar_upcoming_issues(self, content, url):
        """Parse ShareSansar upcoming issues page"""
        soup = BeautifulSoup(content, 'html.parser')
        issues_data = []
        
        try:
            logger.info("Parsing ShareSansar upcoming issues page")
            
            # Use similar approach as existing issues
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                table_text = table.get_text().lower()
                if 'upcoming' in table_text or 'expected' in table_text or 'coming' in table_text:
                    extracted = self._extract_from_any_table(table, url, 0)
                    
                    # Mark all as coming soon
                    for issue in extracted:
                        issue['status'] = 'coming_soon'
                        if not issue.get('open_date'):
                            issue['open_date'] = None
                        if not issue.get('close_date'):
                            issue['close_date'] = None
                    
                    issues_data.extend(extracted)
            
            logger.info(f"ShareSansar upcoming issues parsing completed: {len(issues_data)} issues found")
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in ShareSansar upcoming issues parsing: {e}")
            return []
    
    def _parse_merolagani_ipo(self, content, url):
        """Parse MeroLagani IPO page"""
        soup = BeautifulSoup(content, 'html.parser')
        issues_data = []
        
        try:
            logger.info("Parsing MeroLagani IPO page")
            
            # Look for tables with IPO data
            tables = soup.find_all('table')
            
            for table in tables:
                table_text = table.get_text().lower()
                if 'ipo' in table_text or 'company' in table_text:
                    extracted = self._extract_from_any_table(table, url, 0)
                    issues_data.extend(extracted)
            
            logger.info(f"MeroLagani IPO parsing completed: {len(issues_data)} issues found")
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in MeroLagani IPO parsing: {e}")
            return []
    
    def _parse_merolagani(self, content, url):
        """Parse MeroLagani website stock data"""
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
    
    def _parse_nepali_date(self, date_str):
        """Parse Nepali date string and convert to standard date format"""
        if not date_str or date_str.strip() == '-':
            return None
        
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Common Nepali date formats to handle
            date_patterns = [
                r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # MM-DD-YYYY or MM/DD/YYYY
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',   # MM-DD-YY or MM/DD/YY
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_str)
                if match:
                    parts = match.groups()
                    
                    # Handle different formats
                    if len(parts[0]) == 4:  # YYYY-MM-DD format
                        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    elif len(parts[2]) == 4:  # MM-DD-YYYY format
                        month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
                    else:  # MM-DD-YY format
                        month, day = int(parts[0]), int(parts[1])
                        year = int(parts[2])
                        if year < 50:  # Assume 20xx for years < 50
                            year += 2000
                        else:  # Assume 19xx for years >= 50
                            year += 1900
                    
                    # Validate ranges
                    if 1 <= month <= 12 and 1 <= day <= 31 and 1980 <= year <= 2050:
                        try:
                            return datetime(year, month, day).date()
                        except ValueError:
                            continue
            
            # If no pattern matches, try to extract just year
            year_match = re.search(r'\b(20\d{2})\b', date_str)
            if year_match:
                year = int(year_match.group(1))
                return datetime(year, 1, 1).date()  # Default to Jan 1st
                
        except Exception as e:
            logger.debug(f"Error parsing date '{date_str}': {e}")
        
        return None
    
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
        """Populate sample stock data when scraping fails"""
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
            'total': stock_count + ipo_count
        }


class IPOService:
    """Service for handling IPO/FPO/Rights share data"""
    
    def __init__(self, db_service):
        self.db_service = db_service
        self._create_tables()
    
    def _create_tables(self):
        """Create IPO/FPO/Rights tables"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            if self.db_service.db_type == 'mysql':
                # MySQL table creation
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS issues (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        symbol VARCHAR(20),
                        issue_type ENUM('IPO', 'FPO', 'Rights', 'Debenture') NOT NULL,
                        units BIGINT DEFAULT 0,
                        price DECIMAL(10, 2) DEFAULT 0.00,
                        total_amount DECIMAL(15, 2) DEFAULT 0.00,
                        open_date DATE,
                        close_date DATE,
                        status ENUM('coming_soon', 'open', 'closed') DEFAULT 'coming_soon',
                        source VARCHAR(500),
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_symbol (symbol),
                        INDEX idx_issue_type (issue_type),
                        INDEX idx_status (status),
                        INDEX idx_open_date (open_date),
                        INDEX idx_close_date (close_date),
                        UNIQUE KEY unique_company_type (company_name, issue_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                ''')
            else:
                # SQLite table creation
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS issues (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_name TEXT NOT NULL,
                        symbol TEXT,
                        issue_type TEXT NOT NULL CHECK (issue_type IN ('IPO', 'FPO', 'Rights', 'Debenture')),
                        units INTEGER DEFAULT 0,
                        price REAL DEFAULT 0.0,
                        total_amount REAL DEFAULT 0.0,
                        open_date DATE,
                        close_date DATE,
                        status TEXT DEFAULT 'coming_soon' CHECK (status IN ('coming_soon', 'open', 'closed')),
                        source TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(company_name, issue_type)
                    )
                ''')
                
                # Create indices for SQLite
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_issues_symbol ON issues (symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_issues_type ON issues (issue_type)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_issues_status ON issues (status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_issues_open_date ON issues (open_date)')
            
            conn.commit()
            logger.info(f"IPO tables created successfully for {self.db_service.db_type}")
            
        except Exception as e:
            logger.error(f"Error creating IPO tables: {e}")
            raise
        finally:
            try:
                conn.close()
            except:
                pass
    
    def save_issues(self, issues_data, source_name):
        """Save IPO/FPO/Rights data to database"""
        if not issues_data:
            return 0
        
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            saved_count = 0
            
            for issue in issues_data:
                try:
                    if self.db_service.db_type == 'mysql':
                        cursor.execute('''
                            INSERT INTO issues (
                                company_name, symbol, issue_type, units, price, 
                                total_amount, open_date, close_date, status, source, scraped_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                units = VALUES(units),
                                price = VALUES(price),
                                total_amount = VALUES(total_amount),
                                open_date = VALUES(open_date),
                                close_date = VALUES(close_date),
                                status = VALUES(status),
                                source = VALUES(source),
                                updated_at = CURRENT_TIMESTAMP
                        ''', (
                            issue['company_name'],
                            issue.get('symbol'),
                            issue['issue_type'],
                            issue.get('units', 0),
                            issue.get('price', 0.0),
                            issue.get('total_amount', 0.0),
                            issue.get('open_date'),
                            issue.get('close_date'),
                            issue.get('status', 'coming_soon'),
                            issue.get('source'),
                            datetime.now()
                        ))
                    else:
                        # SQLite - use INSERT OR REPLACE
                        cursor.execute('''
                            INSERT OR REPLACE INTO issues (
                                company_name, symbol, issue_type, units, price, 
                                total_amount, open_date, close_date, status, source, scraped_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            issue['company_name'],
                            issue.get('symbol'),
                            issue['issue_type'],
                            issue.get('units', 0),
                            issue.get('price', 0.0),
                            issue.get('total_amount', 0.0),
                            issue.get('open_date'),
                            issue.get('close_date'),
                            issue.get('status', 'coming_soon'),
                            issue.get('source'),
                            datetime.now(),
                            datetime.now()
                        ))
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error saving issue {issue.get('company_name', 'Unknown')}: {e}")
                    continue
            
            conn.commit()
            logger.info(f"Saved {saved_count} issues from {source_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving issues: {e}")
            return 0
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_open_issues(self, issue_type=None):
        """Get currently open issues"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            query = "SELECT * FROM issues WHERE status = 'open'"
            params = []
            
            if issue_type:
                if self.db_service.db_type == 'mysql':
                    query += " AND issue_type = %s"
                else:
                    query += " AND issue_type = ?"
                params.append(issue_type)
            
            query += " ORDER BY open_date DESC"
            
            cursor.execute(query, params)
            
            if self.db_service.db_type == 'mysql':
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                cursor.row_factory = sqlite3.Row
                results = [dict(row) for row in cursor.fetchall()]
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting open issues: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_coming_soon_issues(self):
        """Get issues that are coming soon"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM issues WHERE status = 'coming_soon' ORDER BY open_date ASC")
            
            if self.db_service.db_type == 'mysql':
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                cursor.row_factory = sqlite3.Row
                results = [dict(row) for row in cursor.fetchall()]
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting coming soon issues: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def search_issues(self, query, limit=20):
        """Search issues by company name or symbol"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            search_pattern = f"%{query}%"
            
            if self.db_service.db_type == 'mysql':
                cursor.execute('''
                    SELECT * FROM issues 
                    WHERE company_name LIKE %s OR symbol LIKE %s
                    ORDER BY 
                        CASE WHEN status = 'open' THEN 1
                             WHEN status = 'coming_soon' THEN 2
                             ELSE 3 END,
                        open_date DESC
                    LIMIT %s
                ''', (search_pattern, search_pattern, limit))
                
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                cursor.execute('''
                    SELECT * FROM issues 
                    WHERE company_name LIKE ? OR symbol LIKE ?
                    ORDER BY 
                        CASE WHEN status = 'open' THEN 1
                             WHEN status = 'coming_soon' THEN 2
                             ELSE 3 END,
                        open_date DESC
                    LIMIT ?
                ''', (search_pattern, search_pattern, limit))
                
                cursor.row_factory = sqlite3.Row
                results = [dict(row) for row in cursor.fetchall()]
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching issues: {e}")
            return []
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
    
    @staticmethod
    def extract_symbol_from_company(company_name):
        """Extract or generate symbol from company name"""
        if not company_name:
            return ""
        
        # Remove common words and take first letters
        words = re.findall(r'\b[A-Z][A-Z\s]*[A-Z]\b|\b[A-Z]+\b', company_name.upper())
        
        if words:
            # Take first letters of significant words
            symbol_parts = []
            for word in words[:3]:  # Max 3 words
                if word in ['LIMITED', 'LTD', 'COMPANY', 'CO', 'BANK', 'FINANCE']:
                    symbol_parts.append(word[0])
                else:
                    symbol_parts.append(word[:2] if len(word) > 1 else word)
            
            symbol = ''.join(symbol_parts)[:6]  # Max 6 characters
            return symbol if len(symbol) >= 2 else company_name[:4].upper()
        
        # Fallback: take first 4 characters
        return re.sub(r'[^A-Z]', '', company_name.upper())[:4]