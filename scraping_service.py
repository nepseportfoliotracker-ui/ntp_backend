# enhanced_scraping_service.py - Web Scraping Service with IPO/FPO/Rights Support

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
        """Scrape IPO/FPO/Rights data from all available sources"""
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
                logger.warning("All IPO scraping sources failed")
                return 0
    
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
                        # Include coming soon issues that start within next 7 days
                        if (open_date - current_date).days <= 7:
                            open_issues.append(issue)
                    else:
                        issue['status'] = 'closed'
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
        """Enhanced ShareSansar existing issues parser with multiple fallback strategies"""
        soup = BeautifulSoup(content, 'html.parser')
        issues_data = []
        
        try:
            logger.info("Parsing ShareSansar existing issues page")
            
            # Strategy 1: Look for div containers with IPO data (more common in modern sites)
            ipo_containers = soup.find_all('div', class_=re.compile(r'.*(?:ipo|issue|stock).*', re.I))
            
            if ipo_containers:
                logger.info(f"Found {len(ipo_containers)} potential IPO containers")
                for container in ipo_containers:
                    try:
                        # Extract text and look for IPO patterns
                        text = container.get_text(strip=True)
                        if any(keyword in text.lower() for keyword in ['ipo', 'fpo', 'rights', 'debenture', 'issue']):
                            # Try to extract structured data from container
                            issue_data = self._extract_issue_from_container(container, url)
                            if issue_data:
                                issues_data.append(issue_data)
                    except Exception as e:
                        logger.debug(f"Error parsing IPO container: {e}")
                        continue
            
            # Strategy 2: Look for tables with more flexible header matching
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables to analyze")
            
            for table_idx, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                logger.info(f"Analyzing table {table_idx + 1} with {len(rows)} rows")
                
                # More flexible header detection
                header_row = rows[0]
                header_text = header_row.get_text().lower()
                
                # Check for any IPO-related keywords in headers
                ipo_keywords = ['company', 'symbol', 'issue', 'ipo', 'fpo', 'rights', 'units', 'price', 'open', 'close', 'date']
                header_match_count = sum(1 for keyword in ipo_keywords if keyword in header_text)
                
                if header_match_count >= 3:  # At least 3 IPO-related keywords
                    logger.info(f"Table {table_idx + 1} looks like IPO table (matched {header_match_count} keywords)")
                    
                    # Parse this table
                    parsed_issues = self._parse_ipo_table_flexible(table, url)
                    if parsed_issues:
                        issues_data.extend(parsed_issues)
                        logger.info(f"Extracted {len(parsed_issues)} issues from table {table_idx + 1}")
            
            # Strategy 3: Look for script tags with JSON data (modern websites often embed data)
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    try:
                        # Look for JSON-like data containing IPO information
                        if any(keyword in script.string.lower() for keyword in ['ipo', 'issue', 'company']):
                            # Try to extract JSON data
                            json_data = self._extract_json_from_script(script.string)
                            if json_data:
                                script_issues = self._parse_json_issues(json_data, url)
                                issues_data.extend(script_issues)
                    except Exception as e:
                        continue
            
            logger.info(f"ShareSansar existing issues parsing completed: {len(issues_data)} issues found")
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in ShareSansar existing issues parsing: {e}")
            return []

    def _parse_ipo_table_flexible(self, table, url):
        """Flexible IPO table parser that adapts to different table structures"""
        issues_data = []
        
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return issues_data
            
            # Get headers
            header_row = rows[0]
            header_cells = header_row.find_all(['th', 'td'])
            headers = [cell.get_text(strip=True).lower() for cell in header_cells]
            
            logger.info(f"Table headers: {headers}")
            
            # Flexible column mapping - try multiple possible names for each field
            column_mapping = {
                'company': self._find_flexible_column_index(headers, ['company', 'name', 'issuer', 'corporation']),
                'symbol': self._find_flexible_column_index(headers, ['symbol', 'scrip', 'stock', 'code']),
                'type': self._find_flexible_column_index(headers, ['type', 'issue type', 'category', 'kind']),
                'units': self._find_flexible_column_index(headers, ['units', 'shares', 'quantity', 'no. of shares', 'unit']),
                'price': self._find_flexible_column_index(headers, ['price', 'rate', 'amount', 'rs.', 'npr']),
                'open_date': self._find_flexible_column_index(headers, ['open', 'opening', 'start', 'from']),
                'close_date': self._find_flexible_column_index(headers, ['close', 'closing', 'end', 'to', 'last']),
                'status': self._find_flexible_column_index(headers, ['status', 'state', 'condition'])
            }
            
            logger.info(f"Column mapping: {column_mapping}")
            
            # Parse data rows
            for row_idx, row in enumerate(rows[1:], 1):
                cols = row.find_all(['td', 'th'])
                if len(cols) < 2:
                    continue
                
                try:
                    # Extract company name (required field)
                    company_name = None
                    if column_mapping['company'] >= 0:
                        company_name = cols[column_mapping['company']].get_text(strip=True)
                    else:
                        # Fallback: look for the longest text in first few columns
                        for i in range(min(3, len(cols))):
                            text = cols[i].get_text(strip=True)
                            if len(text) > 5 and not text.replace(',', '').replace('.', '').isdigit():
                                company_name = text
                                break
                    
                    if not company_name or len(company_name) < 3:
                        continue
                    
                    # Extract other fields with fallbacks
                    symbol = self._extract_cell_value(cols, column_mapping['symbol'], company_name[:4])
                    
                    issue_type = 'IPO'  # Default
                    if column_mapping['type'] >= 0:
                        type_text = cols[column_mapping['type']].get_text(strip=True).upper()
                        if 'FPO' in type_text:
                            issue_type = 'FPO'
                        elif 'RIGHT' in type_text:
                            issue_type = 'Rights'
                        elif 'DEBENTURE' in type_text:
                            issue_type = 'Debenture'
                    
                    units = self._extract_numeric_value(cols, column_mapping['units'], 0)
                    price = self._extract_numeric_value(cols, column_mapping['price'], 0.0)
                    
                    open_date = self._extract_date_value(cols, column_mapping['open_date'])
                    close_date = self._extract_date_value(cols, column_mapping['close_date'])
                    
                    # Determine status
                    status = 'unknown'
                    if column_mapping['status'] >= 0:
                        status_text = cols[column_mapping['status']].get_text(strip=True).lower()
                        if 'open' in status_text:
                            status = 'open'
                        elif 'close' in status_text or 'end' in status_text:
                            status = 'closed'
                        elif 'coming' in status_text or 'soon' in status_text:
                            status = 'coming_soon'
                    else:
                        # Determine status from dates
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
                        'symbol': DataValidator.extract_symbol_from_company(company_name) if not symbol else symbol,
                        'issue_type': issue_type,
                        'units': int(units) if units else 0,
                        'price': float(price) if price else 0.0,
                        'total_amount': int(units) * float(price) if units and price else 0,
                        'open_date': open_date,
                        'close_date': close_date,
                        'status': status,
                        'source': url,
                        'scraped_at': datetime.now()
                    }
                    
                    issues_data.append(issue_data)
                    logger.debug(f"Parsed issue: {company_name} ({issue_type})")
                    
                except Exception as e:
                    logger.debug(f"Error parsing row {row_idx}: {e}")
                    continue
            
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in flexible IPO table parsing: {e}")
            return []

    def _find_flexible_column_index(self, headers, possible_names):
        """Find column index with flexible matching"""
        for i, header in enumerate(headers):
            for name in possible_names:
                if name in header or header in name:
                    return i
        return -1

    def _extract_cell_value(self, cols, col_index, default_value=""):
        """Safely extract cell value"""
        if col_index >= 0 and col_index < len(cols):
            return cols[col_index].get_text(strip=True)
        return default_value

    def _extract_numeric_value(self, cols, col_index, default_value=0):
        """Safely extract and convert numeric value"""
        if col_index >= 0 and col_index < len(cols):
            text = cols[col_index].get_text(strip=True)
            return DataValidator.safe_float(text) if default_value == 0.0 else DataValidator.safe_int(text)
        return default_value

    def _extract_date_value(self, cols, col_index):
        """Safely extract and parse date value"""
        if col_index >= 0 and col_index < len(cols):
            text = cols[col_index].get_text(strip=True)
            return self._parse_nepali_date(text)
        return None

    def _extract_issue_from_container(self, container, url):
        """Extract issue data from a div container"""
        try:
            text = container.get_text(separator=' ', strip=True)
            
            # Use regex patterns to extract structured data
            patterns = {
                'company': r'([A-Z][a-zA-Z\s&]+(?:Limited|Ltd|Bank|Finance|Company))',
                'symbol': r'([A-Z]{2,8})',
                'units': r'(\d{1,3}(?:,\d{3})*)\s*(?:units|shares)',
                'price': r'(?:Rs\.?\s*)?(\d{1,4}(?:\.\d{2})?)',
                'date': r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})'
            }
            
            extracted = {}
            for key, pattern in patterns.items():
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    extracted[key] = match.group(1)
            
            if extracted.get('company'):
                return {
                    'company_name': extracted['company'].strip(),
                    'symbol': extracted.get('symbol', ''),
                    'issue_type': 'IPO',  # Default
                    'units': DataValidator.safe_int(extracted.get('units', '0')),
                    'price': DataValidator.safe_float(extracted.get('price', '0')),
                    'total_amount': 0,
                    'open_date': self._parse_nepali_date(extracted.get('date', '')),
                    'close_date': None,
                    'status': 'unknown',
                    'source': url,
                    'scraped_at': datetime.now()
                }
        
        except Exception as e:
            logger.debug(f"Error extracting from container: {e}")
        
        return None

    def _extract_json_from_script(self, script_content):
        """Try to extract JSON data from script content"""
        try:
            # Look for JSON-like structures
            json_patterns = [
                r'var\s+\w+\s*=\s*(\{.*?\});',
                r'data:\s*(\[.*?\])',
                r'issues:\s*(\[.*?\])',
                r'(\{.*?"company".*?\})'
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, script_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, (list, dict)):
                            return data
                    except:
                        continue
        except:
            pass
        
        return None

    def _parse_json_issues(self, json_data, url):
        """Parse issues from JSON data"""
        issues = []
        
        try:
            if isinstance(json_data, dict):
                json_data = [json_data]
            
            for item in json_data:
                if isinstance(item, dict):
                    # Map JSON fields to our issue structure
                    issue_data = {
                        'company_name': item.get('company', item.get('name', '')),
                        'symbol': item.get('symbol', item.get('code', '')),
                        'issue_type': item.get('type', 'IPO'),
                        'units': DataValidator.safe_int(item.get('units', 0)),
                        'price': DataValidator.safe_float(item.get('price', 0)),
                        'total_amount': 0,
                        'open_date': self._parse_nepali_date(item.get('open_date', '')),
                        'close_date': self._parse_nepali_date(item.get('close_date', '')),
                        'status': item.get('status', 'unknown'),
                        'source': url,
                        'scraped_at': datetime.now()
                    }
                    
                    if issue_data['company_name']:
                        issues.append(issue_data)
        
        except Exception as e:
            logger.debug(f"Error parsing JSON issues: {e}")
        
        return issues
    
    def _parse_sharesansar_upcoming_issues(self, content, url):
        """Enhanced ShareSansar upcoming issues parser"""
        soup = BeautifulSoup(content, 'html.parser')
        issues_data = []
        
        try:
            logger.info("Parsing ShareSansar upcoming issues page")
            
            # Use the same flexible parsing strategies as existing issues
            # but mark all found issues as 'coming_soon'
            
            # Look for tables
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                header_text = rows[0].get_text().lower()
                if not any(keyword in header_text for keyword in ['company', 'issue', 'expected', 'upcoming']):
                    continue
                
                parsed_issues = self._parse_ipo_table_flexible(table, url)
                
                # Mark all as coming soon and adjust data
                for issue in parsed_issues:
                    issue['status'] = 'coming_soon'
                    # For upcoming issues, dates might not be specified
                    if not issue.get('open_date') and not issue.get('close_date'):
                        issue['open_date'] = None
                        issue['close_date'] = None
                
                issues_data.extend(parsed_issues)
            
            logger.info(f"ShareSansar upcoming issues parsing completed: {len(issues_data)} issues found")
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in ShareSansar upcoming issues parsing: {e}")
            return []
    
    def _parse_merolagani_ipo(self, content, url):
        """Enhanced MeroLagani IPO parser with multiple strategies"""
        soup = BeautifulSoup(content, 'html.parser')
        issues_data = []
        
        try:
            logger.info("Parsing MeroLagani IPO page")
            
            # Strategy 1: Look for tables with flexible matching
            tables = soup.find_all('table')
            
            for table_idx, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Check if table contains IPO-like data
                table_text = table.get_text().lower()
                if not any(keyword in table_text for keyword in ['company', 'ipo', 'issue', 'share']):
                    continue
                
                logger.info(f"Analyzing MeroLagani table {table_idx + 1}")
                
                # Try to parse as IPO table
                parsed_issues = self._parse_ipo_table_flexible(table, url)
                if parsed_issues:
                    issues_data.extend(parsed_issues)
            
            # Strategy 2: Look for specific MeroLagani structures
            ipo_sections = soup.find_all('div', class_=re.compile(r'.*(?:ipo|stock|market).*', re.I))
            
            for section in ipo_sections:
                try:
                    section_issues = self._extract_issue_from_container(section, url)
                    if section_issues:
                        issues_data.append(section_issues)
                except Exception as e:
                    continue
            
            logger.info(f"MeroLagani IPO parsing completed: {len(issues_data)} issues found")
            return issues_data
            
        except Exception as e:
            logger.error(f"Error in MeroLagani IPO parsing: {e}")
            return []
    
    def _parse_merolagani(self, content, url):
        """Parse MeroLagani website stock data (unchanged from original)"""
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
        """Build standardized stock data dictionary (unchanged from original)"""
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
        """Populate sample stock data when scraping fails (unchanged from original)"""
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
                        INDEX idx_close_date (close_date)
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
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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