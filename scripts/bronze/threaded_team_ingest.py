#!/usr/bin/env python3
"""
Optimized concurrent team ingestion using requests with ThreadPoolExecutor.
This approach avoids SSL/async issues while still providing concurrency.
"""

import requests
import psycopg2
from dotenv import load_dotenv
import os
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging

# Setup logging without emojis for Windows compatibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('team_ingestion_threaded.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ThreadedTeamIngester:
    def __init__(self, max_workers=5, rate_limit_per_minute=20):  # Reduced from 25/24
        self.max_workers = max_workers
        self.rate_limit_per_minute = rate_limit_per_minute
        self.request_times = []
        self.rate_lock = threading.Lock()
        
    def get_db_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            host=os.getenv('PGHOST'),
            port=os.getenv('PGPORT'),
            database=os.getenv('PGDATABASE'),
            user=os.getenv('PGUSER'),
            password=os.getenv('PGPASSWORD')
        )
    
    def check_rate_limit(self):
        """Check if we can make a request without exceeding rate limit."""
        with self.rate_lock:
            current_time = time.time()
            # Remove requests older than 60 seconds
            self.request_times = [t for t in self.request_times if current_time - t < 60]
            
            if len(self.request_times) >= self.rate_limit_per_minute:
                # Calculate how long to wait
                oldest_request = min(self.request_times)
                wait_time = 61 - (current_time - oldest_request)
                if wait_time > 0:
                    logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    # Recheck after wait
                    current_time = time.time()
                    self.request_times = [t for t in self.request_times if current_time - t < 60]
            
            # Record this request
            self.request_times.append(current_time)
    
    def fetch_team(self, team_id):
        """Fetch a single team with rate limiting and error handling."""
        self.check_rate_limit()
        
        url = f"https://virtual.sssfonline.com/api/teams/{team_id}"
        headers = {
            'User-Agent': 'SASP-ETL-Threaded/1.0',
            'Accept': 'application/json'
        }
        
        try:
            # Add small delay to prevent overwhelming the server
            time.sleep(0.1)
            response = requests.get(url, headers=headers, timeout=30, verify=True)
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"SUCCESS Team {team_id}: {data.get('name', 'Unknown')}")
                return team_id, data, None
            elif response.status_code == 404:
                logger.debug(f"NOT_FOUND Team {team_id}")
                return team_id, None, "NOT_FOUND"
            elif response.status_code == 429:
                logger.warning(f"RATE_LIMITED Team {team_id}")
                return team_id, None, "RATE_LIMITED"
            else:
                logger.warning(f"HTTP_ERROR Team {team_id}: {response.status_code}")
                return team_id, None, f"HTTP_{response.status_code}"
                
        except requests.exceptions.Timeout:
            logger.warning(f"TIMEOUT Team {team_id}")
            return team_id, None, "TIMEOUT"
        except Exception as e:
            logger.error(f"ERROR Team {team_id}: {str(e)}")
            return team_id, None, f"ERROR_{str(e)}"
    
    def insert_teams_batch(self, teams_data):
        """Insert multiple teams in a single transaction."""
        if not teams_data:
            return 0
            
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                insert_sql = """
                    INSERT INTO raw_teams (payload, source, source_hash, ingested_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_hash) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        ingested_at = EXCLUDED.ingested_at;
                """
                
                batch_data = []
                for team_id, team_data in teams_data:
                    source = f"teams_api_{team_id}"
                    source_hash = f"team_{team_id}"
                    batch_data.append((
                        json.dumps(team_data),
                        source,
                        source_hash,
                        datetime.now()
                    ))
                
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                
                logger.info(f"BATCH_INSERT: {len(batch_data)} teams")
                return len(batch_data)
                
        except Exception as e:
            logger.error(f"BATCH_INSERT_ERROR: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def process_team_range(self, start_id, end_id, batch_size=100):
        """Process a range of team IDs with threaded fetching and batch insertion."""
        logger.info(f"STARTING: Teams {start_id} to {end_id} (workers={self.max_workers}, rate_limit={self.rate_limit_per_minute}/min)")
        
        total_processed = 0
        total_successful = 0
        total_not_found = 0
        total_errors = 0
        
        # Process in chunks for batch inserts
        for chunk_start in range(start_id, end_id + 1, batch_size):
            chunk_end = min(chunk_start + batch_size - 1, end_id)
            chunk_ids = list(range(chunk_start, chunk_end + 1))
            
            logger.info(f"PROCESSING_CHUNK: {chunk_start}-{chunk_end} ({len(chunk_ids)} teams)")
            
            chunk_start_time = time.time()
            successful_teams = []
            chunk_not_found = 0
            chunk_errors = 0
            
            # Use ThreadPoolExecutor for concurrent requests
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_team = {executor.submit(self.fetch_team, team_id): team_id 
                                 for team_id in chunk_ids}
                
                # Process completed tasks
                for future in as_completed(future_to_team):
                    team_id = future_to_team[future]
                    try:
                        result = future.result()
                        team_id, team_data, error = result
                        
                        if error is None and team_data:
                            successful_teams.append((team_id, team_data))
                        elif error == "NOT_FOUND":
                            chunk_not_found += 1
                        else:
                            chunk_errors += 1
                            
                    except Exception as e:
                        logger.error(f"FUTURE_ERROR Team {team_id}: {e}")
                        chunk_errors += 1
            
            # Batch insert successful teams
            if successful_teams:
                inserted = self.insert_teams_batch(successful_teams)
                total_successful += inserted
            
            total_processed += len(chunk_ids)
            total_not_found += chunk_not_found
            total_errors += chunk_errors
            
            chunk_duration = time.time() - chunk_start_time
            rate = len(chunk_ids) / chunk_duration if chunk_duration > 0 else 0
            logger.info(f"CHUNK_COMPLETE: {len(successful_teams)} inserted, {chunk_not_found} not found, {chunk_errors} errors ({rate:.1f} req/sec)")
            
            # Brief pause between chunks
            time.sleep(1)
        
        logger.info(f"""
INGESTION_COMPLETE:
Total processed: {total_processed}
Successfully inserted: {total_successful}
Not found: {total_not_found}
Errors: {total_errors}
Success rate: {(total_successful/total_processed*100):.1f}%
        """)
        
        return {
            'processed': total_processed,
            'successful': total_successful,
            'not_found': total_not_found,
            'errors': total_errors
        }

def main():
    """Main execution function."""
    # Configuration
    START_ID = 1
    END_ID = 4000
    MAX_WORKERS = 5      # Reduced concurrent threads to prevent connection resets
    RATE_LIMIT = 20      # Reduced requests per minute
    BATCH_SIZE = 50      # Smaller batches
    
    ingester = ThreadedTeamIngester(
        max_workers=MAX_WORKERS,
        rate_limit_per_minute=RATE_LIMIT
    )
    
    start_time = time.time()
    results = ingester.process_team_range(
        START_ID, 
        END_ID, 
        batch_size=BATCH_SIZE
    )
    duration = time.time() - start_time
    
    logger.info(f"TOTAL_TIME: {duration:.1f} seconds")
    logger.info(f"AVERAGE_RATE: {results['processed']/duration:.1f} teams/second")

if __name__ == "__main__":
    main()