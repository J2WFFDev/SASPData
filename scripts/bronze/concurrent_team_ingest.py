#!/usr/bin/env python3
"""
Optimized concurrent team ingestion with rate limiting.
Handles 25-50 concurrent requests with 60-second rate limit windows.
"""

import asyncio
import aiohttp
import psycopg2
from dotenv import load_dotenv
import os
import json
import time
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('team_ingestion_concurrent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ConcurrentTeamIngester:
    def __init__(self, max_concurrent=25, rate_limit_per_minute=24):
        self.max_concurrent = max_concurrent
        self.rate_limit_per_minute = rate_limit_per_minute
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = asyncio.Semaphore(rate_limit_per_minute)
        self.request_times = []
        
    def get_db_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            host=os.getenv('PGHOST'),
            port=os.getenv('PGPORT'),
            database=os.getenv('PGDATABASE'),
            user=os.getenv('PGUSER'),
            password=os.getenv('PGPASSWORD')
        )
    
    async def rate_limit_reset(self):
        """Reset rate limiter every 60 seconds."""
        while True:
            await asyncio.sleep(60)
            # Reset semaphore by releasing all permits and recreating
            current_time = time.time()
            self.request_times = [t for t in self.request_times if current_time - t < 60]
            
            # Calculate how many permits to restore
            used_permits = len(self.request_times)
            available_permits = max(0, self.rate_limit_per_minute - used_permits)
            
            # Recreate semaphore with available permits
            self.rate_limiter = asyncio.Semaphore(available_permits)
            logger.info(f"🔄 Rate limiter reset: {available_permits}/{self.rate_limit_per_minute} permits available")
    
    async def fetch_team_async(self, session, team_id):
        """Fetch a single team with rate limiting and error handling."""
        async with self.semaphore:  # Limit concurrent requests
            async with self.rate_limiter:  # Limit requests per minute
                self.request_times.append(time.time())
                
                url = f"https://virtual.sssfonline.com/api/teams/{team_id}"
                headers = {
                    'User-Agent': 'SASP-ETL-Concurrent/1.0',
                    'Accept': 'application/json'
                }
                
                try:
                    async with session.get(url, headers=headers, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            logger.debug(f"✓ Team {team_id}: {data.get('name', 'Unknown')}")
                            return team_id, data, None
                        elif response.status == 404:
                            logger.debug(f"⚬ Team {team_id}: Not found")
                            return team_id, None, "NOT_FOUND"
                        elif response.status == 429:
                            logger.warning(f"⚠️ Team {team_id}: Rate limited")
                            return team_id, None, "RATE_LIMITED"
                        else:
                            logger.warning(f"⚠️ Team {team_id}: HTTP {response.status}")
                            return team_id, None, f"HTTP_{response.status}"
                            
                except asyncio.TimeoutError:
                    logger.warning(f"⚠️ Team {team_id}: Timeout")
                    return team_id, None, "TIMEOUT"
                except Exception as e:
                    logger.error(f"❌ Team {team_id}: {str(e)}")
                    return team_id, None, f"ERROR_{str(e)}"
    
    def insert_teams_batch(self, teams_data):
        """Insert multiple teams in a single transaction."""
        if not teams_data:
            return 0
            
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Prepare batch insert
                insert_sql = """
                    INSERT INTO raw_teams (id, payload, ingested_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        ingested_at = EXCLUDED.ingested_at;
                """
                
                batch_data = []
                for team_id, team_data in teams_data:
                    batch_data.append((
                        team_id,
                        json.dumps(team_data),
                        datetime.now()
                    ))
                
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                
                logger.info(f"📦 Batch inserted {len(batch_data)} teams")
                return len(batch_data)
                
        except Exception as e:
            logger.error(f"❌ Batch insert failed: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    async def process_team_range(self, start_id, end_id, batch_size=100):
        """Process a range of team IDs with concurrent fetching and batch insertion."""
        logger.info(f"🚀 Processing teams {start_id} to {end_id} (concurrent={self.max_concurrent}, rate_limit={self.rate_limit_per_minute}/min)")
        
        # Start rate limiter reset task
        reset_task = asyncio.create_task(self.rate_limit_reset())
        
        total_processed = 0
        total_successful = 0
        total_not_found = 0
        total_errors = 0
        
        try:
            async with aiohttp.ClientSession() as session:
                
                # Process in chunks to manage memory and batch inserts
                for chunk_start in range(start_id, end_id + 1, batch_size):
                    chunk_end = min(chunk_start + batch_size - 1, end_id)
                    chunk_ids = list(range(chunk_start, chunk_end + 1))
                    
                    logger.info(f"📊 Processing chunk: {chunk_start}-{chunk_end} ({len(chunk_ids)} teams)")
                    
                    # Create tasks for concurrent fetching
                    tasks = [
                        self.fetch_team_async(session, team_id)
                        for team_id in chunk_ids
                    ]
                    
                    # Wait for all tasks in this chunk to complete
                    chunk_start_time = time.time()
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    chunk_duration = time.time() - chunk_start_time
                    
                    # Process results
                    successful_teams = []
                    chunk_not_found = 0
                    chunk_errors = 0
                    
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"❌ Task exception: {result}")
                            chunk_errors += 1
                            continue
                            
                        team_id, team_data, error = result
                        
                        if error is None and team_data:
                            successful_teams.append((team_id, team_data))
                        elif error == "NOT_FOUND":
                            chunk_not_found += 1
                        else:
                            chunk_errors += 1
                    
                    # Batch insert successful teams
                    if successful_teams:
                        inserted = self.insert_teams_batch(successful_teams)
                        total_successful += inserted
                    
                    total_processed += len(chunk_ids)
                    total_not_found += chunk_not_found
                    total_errors += chunk_errors
                    
                    # Progress report
                    rate = len(chunk_ids) / chunk_duration if chunk_duration > 0 else 0
                    logger.info(f"✅ Chunk complete: {len(successful_teams)} inserted, {chunk_not_found} not found, {chunk_errors} errors ({rate:.1f} req/sec)")
                    
                    # Brief pause between chunks to respect rate limits
                    await asyncio.sleep(1)
        
        finally:
            reset_task.cancel()
            
        logger.info(f"""
🎯 CONCURRENT INGESTION COMPLETE
📊 Total processed: {total_processed}
✅ Successfully inserted: {total_successful}
⚬ Not found: {total_not_found}
❌ Errors: {total_errors}
📈 Success rate: {(total_successful/total_processed*100):.1f}%
        """)
        
        return {
            'processed': total_processed,
            'successful': total_successful,
            'not_found': total_not_found,
            'errors': total_errors
        }

async def main():
    """Main execution function."""
    # Configuration
    START_ID = 1
    END_ID = 4000
    MAX_CONCURRENT = 25  # Concurrent requests
    RATE_LIMIT = 24      # Requests per minute
    BATCH_SIZE = 100     # Teams per batch insert
    
    ingester = ConcurrentTeamIngester(
        max_concurrent=MAX_CONCURRENT,
        rate_limit_per_minute=RATE_LIMIT
    )
    
    start_time = time.time()
    results = await ingester.process_team_range(
        START_ID, 
        END_ID, 
        batch_size=BATCH_SIZE
    )
    duration = time.time() - start_time
    
    logger.info(f"🏁 Total execution time: {duration:.1f} seconds")
    logger.info(f"📊 Average rate: {results['processed']/duration:.1f} teams/second")

if __name__ == "__main__":
    asyncio.run(main())