# ipo_service.py - Simplified SQLite-only version

import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

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
            
            # FPO Table
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
            
            # Rights/Dividends Table
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
            
            # Create indexes
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
            logger.info("Separate IPO/FPO/Rights tables created successfully (SQLite)")
            
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
            
            # Clear existing data
            cursor.execute(f"DELETE FROM {table_name}")
            
            saved_count = 0
            
            # Insert new data (limited to 8 records)
            for issue in issues_data[:8]:
                try:
                    if table_name in ['ipos', 'fpos']:
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
        return self._get_table_data_formatted('ipos', 'IPO')
    
    def get_all_fpos(self):
        """Get all FPO records"""
        return self._get_table_data_formatted('fpos', 'FPO')
    
    def get_all_rights_dividends(self):
        """Get all Rights/Dividend records"""
        return self._get_table_data_formatted('rights_dividends', 'Rights')
    
    def get_open_issues(self, issue_type=None):
        """Get currently open issues from all tables"""
        open_issues = []
        
        if not issue_type or issue_type.upper() == 'IPO':
            ipos = self._get_issues_by_status('ipos', 'open', 'IPO')
            open_issues.extend(ipos)
        
        if not issue_type or issue_type.upper() == 'FPO':
            fpos = self._get_issues_by_status('fpos', 'open', 'FPO')
            open_issues.extend(fpos)
        
        if not issue_type or issue_type.upper() in ['RIGHTS', 'DIVIDEND']:
            rights = self._get_issues_by_status('rights_dividends', 'open', 'Rights')
            open_issues.extend(rights)
        
        open_issues.sort(key=lambda x: x.get('open_date') or x.get('book_close_date') or '', reverse=True)
        
        return open_issues
    
    def get_coming_soon_issues(self):
        """Get coming soon issues from all tables"""
        coming_soon = []
        
        coming_soon.extend(self._get_issues_by_status('ipos', 'coming_soon', 'IPO'))
        coming_soon.extend(self._get_issues_by_status('fpos', 'coming_soon', 'FPO'))
        coming_soon.extend(self._get_issues_by_status('rights_dividends', 'coming_soon', 'Rights'))
        
        coming_soon.sort(key=lambda x: x.get('open_date') or x.get('book_close_date') or '', reverse=False)
        
        return coming_soon
    
    def search_issues(self, query, limit=20):
        """Search issues across all tables"""
        results = []
        search_term = f"%{query.upper()}%"
        
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            # Search IPOs
            cursor.execute('''
                SELECT * FROM ipos 
                WHERE (UPPER(company_name) LIKE ? OR UPPER(symbol) LIKE ?)
                ORDER BY scraped_at DESC
                LIMIT ?
            ''', (search_term, search_term, limit))
            
            ipos = self._format_table_results(cursor.fetchall(), cursor.description, 'IPO')
            results.extend(ipos)
            
            # Search FPOs
            cursor.execute('''
                SELECT * FROM fpos 
                WHERE (UPPER(company_name) LIKE ? OR UPPER(symbol) LIKE ?)
                ORDER BY scraped_at DESC
                LIMIT ?
            ''', (search_term, search_term, limit))
            
            fpos = self._format_table_results(cursor.fetchall(), cursor.description, 'FPO')
            results.extend(fpos)
            
            # Search Rights/Dividends
            cursor.execute('''
                SELECT * FROM rights_dividends 
                WHERE (UPPER(company_name) LIKE ? OR UPPER(symbol) LIKE ?)
                ORDER BY scraped_at DESC
                LIMIT ?
            ''', (search_term, search_term, limit))
            
            rights = self._format_table_results(cursor.fetchall(), cursor.description, 'Rights')
            results.extend(rights)
            
            results.sort(key=lambda x: x.get('scraped_at', ''), reverse=True)
            
            return results[:limit]
            
        except Exception as e:
            logger.error(f"Error searching issues: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _get_table_data_formatted(self, table_name, issue_category):
        """Get data from table with formatting"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY scraped_at DESC")
            rows = cursor.fetchall()
            
            return self._format_table_results(rows, cursor.description, issue_category)
            
        except Exception as e:
            logger.error(f"Error getting data from {table_name}: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _get_issues_by_status(self, table_name, status, issue_category):
        """Get issues by status from specific table"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT * FROM {table_name} 
                WHERE status = ? 
                ORDER BY scraped_at DESC
            ''', (status,))
            
            rows = cursor.fetchall()
            return self._format_table_results(rows, cursor.description, issue_category)
            
        except Exception as e:
            logger.error(f"Error getting {status} issues from {table_name}: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _format_table_results(self, rows, description, issue_category):
        """Format database results for consumption"""
        if not rows:
            return []
        
        columns = [desc[0] for desc in description]
        
        results = []
        for row in rows:
            issue_dict = dict(zip(columns, row))
            issue_dict['issue_category'] = issue_category
            
            # Format dates
            for date_field in ['open_date', 'close_date', 'book_close_date', 'scraped_at', 'updated_at']:
                if date_field in issue_dict and issue_dict[date_field]:
                    try:
                        if isinstance(issue_dict[date_field], str):
                            parsed_date = datetime.fromisoformat(issue_dict[date_field].replace('Z', '+00:00'))
                            issue_dict[date_field] = parsed_date.isoformat()
                        elif hasattr(issue_dict[date_field], 'isoformat'):
                            issue_dict[date_field] = issue_dict[date_field].isoformat()
                    except Exception as e:
                        logger.debug(f"Error formatting date {date_field}: {e}")
            
            # Ensure numeric fields are properly formatted
            for numeric_field in ['units', 'price', 'total_amount']:
                if numeric_field in issue_dict and issue_dict[numeric_field] is not None:
                    try:
                        issue_dict[numeric_field] = float(issue_dict[numeric_field])
                    except (ValueError, TypeError):
                        issue_dict[numeric_field] = 0.0
            
            # Add display fields
            issue_dict['display_title'] = f"{issue_dict.get('company_name', 'Unknown')} ({issue_category})"
            issue_dict['display_symbol'] = issue_dict.get('symbol', 'N/A')
            issue_dict['display_status'] = issue_dict.get('status', 'unknown').replace('_', ' ').title()
            
            results.append(issue_dict)
        
        return results
    
    def get_statistics(self):
        """Get statistics about all tables"""
        try:
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            
            stats = {
                'tables': {},
                'summary': {
                    'total_issues': 0,
                    'open_issues': 0,
                    'coming_soon_issues': 0,
                    'closed_issues': 0
                },
                'by_category': {}
            }
            
            for table, category in [('ipos', 'IPO'), ('fpos', 'FPO'), ('rights_dividends', 'Rights')]:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                total_count = result[0] if result else 0
                
                cursor.execute(f'''
                    SELECT status, COUNT(*) as count 
                    FROM {table} 
                    GROUP BY status
                ''')
                
                status_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                stats['tables'][table] = {
                    'total': total_count,
                    'by_status': status_counts
                }
                
                stats['by_category'][category] = {
                    'total': total_count,
                    'open': status_counts.get('open', 0),
                    'coming_soon': status_counts.get('coming_soon', 0),
                    'closed': status_counts.get('closed', 0)
                }
                
                stats['summary']['total_issues'] += total_count
                stats['summary']['open_issues'] += status_counts.get('open', 0)
                stats['summary']['coming_soon_issues'] += status_counts.get('coming_soon', 0)
                stats['summary']['closed_issues'] += status_counts.get('closed', 0)
            
            stats['last_updated'] = datetime.now().isoformat()
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {
                'tables': {},
                'summary': {'total_issues': 0, 'open_issues': 0, 'coming_soon_issues': 0, 'closed_issues': 0},
                'by_category': {},
                'error': str(e)
            }
        finally:
            try:
                conn.close()
            except:
                pass