# index_service.py - Service for managing NEPSE market indices

import logging
from datetime import datetime, timedelta
import sqlite3

logger = logging.getLogger(__name__)


class IndexService:
    """Service for managing NEPSE market indices data"""
    
    def __init__(self, db_service):
        self.db_service = db_service
        self._ensure_indices_table()
        logger.info("IndexService initialized with market_indices table")
    
    def _ensure_indices_table(self):
        """Create market indices table if it doesn't exist"""
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
        
        # Create index for faster queries
        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_market_indices_scraped_at 
        ON market_indices(scraped_at DESC)
        """
        
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            cursor.execute(create_index_sql)
            conn.commit()
            conn.close()
            logger.info("market_indices table and indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating market_indices table: {e}")
            raise
    
    def save_indices(self, indices, source_name):
        """
        Save market indices to database
        
        Args:
            indices: List of index dictionaries
            source_name: Name of the data source
            
        Returns:
            Number of indices saved
        """
        if not indices:
            logger.warning("No indices provided to save")
            return 0
        
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            saved_count = 0
            scrape_time = datetime.now()
            
            for index_data in indices:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO market_indices 
                        (index_name, index_value, point_change, percent_change, 
                         turnover, prev_close, source, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        index_data.get('index_name'),
                        index_data.get('index_value'),
                        index_data.get('point_change', 0),
                        index_data.get('percent_change', 0),
                        index_data.get('turnover', 0),
                        index_data.get('prev_close', 0),
                        source_name,
                        scrape_time
                    ))
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving index {index_data.get('index_name')}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully saved {saved_count}/{len(indices)} indices from {source_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving indices to database: {e}")
            return 0
    
    def get_latest_indices(self, limit=None):
        """
        Get the latest market indices
        
        Args:
            limit: Optional limit on number of indices to return
            
        Returns:
            List of index dictionaries
        """
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            # Get the most recent scrape time
            cursor.execute("SELECT MAX(scraped_at) FROM market_indices")
            latest_scrape = cursor.fetchone()[0]
            
            if not latest_scrape:
                conn.close()
                logger.info("No market indices found in database")
                return []
            
            # Get all indices from that scrape time
            query = """
                SELECT index_name, index_value, point_change, percent_change,
                       turnover, prev_close, scraped_at, source
                FROM market_indices
                WHERE scraped_at = ?
                ORDER BY 
                    CASE 
                        WHEN index_name = 'NEPSE Index' THEN 0
                        WHEN index_name = 'Sensitive Index' THEN 1
                        WHEN index_name = 'Float Index' THEN 2
                        ELSE 3
                    END,
                    index_name
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, (latest_scrape,))
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
                    'scraped_at': row[6],
                    'source': row[7]
                })
            
            logger.info(f"Retrieved {len(indices)} latest market indices")
            return indices
            
        except Exception as e:
            logger.error(f"Error fetching latest indices: {e}")
            return []
    
    def get_index_by_name(self, index_name):
        """
        Get the latest value for a specific index
        
        Args:
            index_name: Name of the index (e.g., 'NEPSE Index')
            
        Returns:
            Index dictionary or None
        """
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT index_name, index_value, point_change, percent_change,
                       turnover, prev_close, scraped_at, source
                FROM market_indices
                WHERE index_name = ?
                ORDER BY scraped_at DESC
                LIMIT 1
            """, (index_name,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'index_name': row[0],
                    'index_value': row[1],
                    'point_change': row[2],
                    'percent_change': row[3],
                    'turnover': row[4],
                    'prev_close': row[5],
                    'scraped_at': row[6],
                    'source': row[7]
                }
            
            logger.info(f"Index '{index_name}' not found")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching index '{index_name}': {e}")
            return None
    
    def get_index_history(self, index_name, days=30):
        """
        Get historical data for a specific index
        
        Args:
            index_name: Name of the index
            days: Number of days of history to retrieve
            
        Returns:
            List of historical index data
        """
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            cursor.execute("""
                SELECT index_name, index_value, point_change, percent_change,
                       turnover, prev_close, scraped_at, source
                FROM market_indices
                WHERE index_name = ? AND scraped_at >= ?
                ORDER BY scraped_at DESC
            """, (index_name, cutoff_date))
            
            rows = cursor.fetchall()
            conn.close()
            
            history = []
            for row in rows:
                history.append({
                    'index_name': row[0],
                    'index_value': row[1],
                    'point_change': row[2],
                    'percent_change': row[3],
                    'turnover': row[4],
                    'prev_close': row[5],
                    'scraped_at': row[6],
                    'source': row[7]
                })
            
            logger.info(f"Retrieved {len(history)} historical records for '{index_name}'")
            return history
            
        except Exception as e:
            logger.error(f"Error fetching history for index '{index_name}': {e}")
            return []
    
    def get_all_index_names(self):
        """
        Get list of all unique index names in the database
        
        Returns:
            List of index names
        """
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT index_name
                FROM market_indices
                ORDER BY 
                    CASE 
                        WHEN index_name = 'NEPSE Index' THEN 0
                        ELSE 1
                    END,
                    index_name
            """)
            
            rows = cursor.fetchall()
            conn.close()
            
            index_names = [row[0] for row in rows]
            logger.info(f"Found {len(index_names)} unique index names")
            return index_names
            
        except Exception as e:
            logger.error(f"Error fetching index names: {e}")
            return []
    
    def get_indices_summary(self):
        """
        Get summary statistics for all indices
        
        Returns:
            Dictionary with summary information
        """
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            # Get latest scrape time
            cursor.execute("SELECT MAX(scraped_at) FROM market_indices")
            latest_scrape = cursor.fetchone()[0]
            
            if not latest_scrape:
                return {
                    'total_indices': 0,
                    'last_update': None,
                    'indices': []
                }
            
            # Get count and gainers/losers
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) as gainers,
                    SUM(CASE WHEN percent_change < 0 THEN 1 ELSE 0 END) as losers,
                    SUM(CASE WHEN percent_change = 0 THEN 1 ELSE 0 END) as unchanged
                FROM market_indices
                WHERE scraped_at = ?
            """, (latest_scrape,))
            
            stats = cursor.fetchone()
            conn.close()
            
            return {
                'total_indices': stats[0],
                'gainers': stats[1],
                'losers': stats[2],
                'unchanged': stats[3],
                'last_update': latest_scrape
            }
            
        except Exception as e:
            logger.error(f"Error getting indices summary: {e}")
            return {
                'total_indices': 0,
                'gainers': 0,
                'losers': 0,
                'unchanged': 0,
                'last_update': None
            }
    
    def cleanup_old_data(self, days_to_keep=90):
        """
        Remove index data older than specified days
        
        Args:
            days_to_keep: Number of days of data to retain
            
        Returns:
            Number of records deleted
        """
        try:
            conn = self.db_service.get_connection('data')
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            cursor.execute("""
                DELETE FROM market_indices
                WHERE scraped_at < ?
            """, (cutoff_date,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Cleaned up {deleted_count} old index records (older than {days_to_keep} days)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old index data: {e}")
            return 0


# Test function
def test_index_service():
    """Test the IndexService functionality"""
    
    class MockDBService:
        def __init__(self):
            self.db_type = 'sqlite'
            self.conn = None
        
        def get_connection(self, db_type='data'):
            if self.conn is None:
                self.conn = sqlite3.connect(':memory:')
            return self.conn
    
    print("=== Testing IndexService ===\n")
    
    db_service = MockDBService()
    index_service = IndexService(db_service)
    
    # Test saving indices
    test_indices = [
        {
            'index_name': 'NEPSE Index',
            'index_value': 2650.50,
            'point_change': 15.30,
            'percent_change': 0.58,
            'turnover': 5000000,
            'prev_close': 2635.20,
            'source': 'Test Source',
            'scraped_at': datetime.now()
        },
        {
            'index_name': 'Banking SubIndex',
            'index_value': 1850.25,
            'point_change': -8.50,
            'percent_change': -0.46,
            'turnover': 2000000,
            'prev_close': 1858.75,
            'source': 'Test Source',
            'scraped_at': datetime.now()
        }
    ]
    
    print("1. Saving test indices...")
    saved = index_service.save_indices(test_indices, "Test Source")
    print(f"   Saved {saved} indices\n")
    
    print("2. Getting latest indices...")
    latest = index_service.get_latest_indices()
    print(f"   Found {len(latest)} indices:")
    for idx in latest:
        print(f"   - {idx['index_name']}: {idx['index_value']} ({idx['percent_change']:+.2f}%)")
    print()
    
    print("3. Getting NEPSE Index by name...")
    nepse = index_service.get_index_by_name('NEPSE Index')
    if nepse:
        print(f"   {nepse['index_name']}: {nepse['index_value']}")
    print()
    
    print("4. Getting indices summary...")
    summary = index_service.get_indices_summary()
    print(f"   Total: {summary['total_indices']}")
    print(f"   Gainers: {summary['gainers']}, Losers: {summary['losers']}")
    print()
    
    print("=== Test completed ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    test_index_service()