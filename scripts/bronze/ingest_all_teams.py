#!/usr/bin/env python3
"""
Complete team ingestion script with proper rate limiting and retry logic.
Loads ALL teams from team_id 1 to MAX_TEAM_ID with robust error handling.
"""

import os
import sys
import time
import json
import hashlib
import logging
import requests
import psycopg2
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Load environment
load_dotenv()

# Configuration
MIN_TEAM_ID = 1     # Start from team ID 1
MAX_TEAM_ID = 4000  # Go up to team ID 4000 to get complete dataset
RATE_LIMIT_DELAY = 2.5  # 2.5 seconds between requests = 24 requests/minute (safe margin)
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0  # Exponential backoff multiplier
BATCH_COMMIT_SIZE = 10  # Commit every N successful inserts

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('team_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using environment variables"""
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        port=int(os.getenv('PGPORT', '5432')),
        dbname=os.getenv('PGDATABASE', 'saspd'),
        user=os.getenv('PGUSER', 'sasp_dba'),
        password=os.getenv('PGPASSWORD', '')
    )

def team_exists(conn, team_id: int) -> bool:
    """Check if team already exists in database"""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM raw_teams WHERE id = %s", (team_id,))
        return cur.fetchone() is not None

def fetch_team_with_retry(team_id: int, session: requests.Session) -> Optional[Dict[Any, Any]]:
    """Fetch team data with exponential backoff retry logic"""
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Attempting to fetch team {team_id} (attempt {attempt + 1})")
            
            response = session.get(
                f"https://virtual.sssfonline.com/api/teams/{team_id}",
                timeout=30
            )
            
            # Handle different response codes
            if response.status_code == 200:
                data = response.json()
                # API returns team data directly, not wrapped in 'data' field
                if data and isinstance(data, dict) and data.get('id'):
                    logger.debug(f"Successfully fetched team {team_id}")
                    return data
                else:
                    logger.debug(f"Team {team_id} has no valid data or missing ID")
                    return None
                    
            elif response.status_code == 404:
                logger.debug(f"Team {team_id} not found (404)")
                return None
                
            elif response.status_code == 429:  # Rate limited
                wait_time = BACKOFF_FACTOR ** attempt * 60  # Start with 1 minute
                logger.warning(f"Rate limited on team {team_id}, waiting {wait_time:.1f} seconds")
                time.sleep(wait_time)
                continue
                
            elif response.status_code >= 500:  # Server error
                wait_time = BACKOFF_FACTOR ** attempt * 5  # Start with 5 seconds
                logger.warning(f"Server error {response.status_code} for team {team_id}, waiting {wait_time:.1f} seconds")
                time.sleep(wait_time)
                continue
                
            else:
                logger.warning(f"Unexpected status {response.status_code} for team {team_id}")
                return None
                
        except requests.exceptions.Timeout:
            wait_time = BACKOFF_FACTOR ** attempt * 10
            logger.warning(f"Timeout for team {team_id}, waiting {wait_time:.1f} seconds")
            time.sleep(wait_time)
            
        except requests.exceptions.RequestException as e:
            wait_time = BACKOFF_FACTOR ** attempt * 5
            logger.warning(f"Request error for team {team_id}: {e}, waiting {wait_time:.1f} seconds")
            time.sleep(wait_time)
            
    logger.error(f"Failed to fetch team {team_id} after {MAX_RETRIES} attempts")
    return None

def insert_team(conn, api_team_id: int, team_data: Dict[Any, Any]) -> bool:
    """Insert team data into raw_teams table using correct table structure"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_teams (id, payload, source, source_hash, ingested_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    source = EXCLUDED.source,
                    source_hash = EXCLUDED.source_hash,
                    ingested_at = NOW()
                """,
                (api_team_id, json.dumps(team_data), f'teams/{api_team_id}', 
                 hashlib.md5(json.dumps(team_data, sort_keys=True).encode()).hexdigest())
            )
        return True
        
    except Exception as e:
        logger.error(f"Failed to insert team {api_team_id}: {e}")
        return False

def get_progress(conn) -> tuple[int, int]:
    """Get current progress - returns (teams_loaded, max_team_id)"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COALESCE(MAX(id), 0) FROM raw_teams")
        count, max_id = cur.fetchone()
        return count, max_id

def main():
    """Main ingestion process"""
    logger.info("Starting complete team ingestion process")
    logger.info(f"Target: Load teams {MIN_TEAM_ID} to {MAX_TEAM_ID}")
    logger.info(f"Rate limit: {RATE_LIMIT_DELAY}s delay = {60/RATE_LIMIT_DELAY:.1f} requests/minute")
    
    # Connect to database
    conn = get_db_connection()
    
    # Get current progress
    teams_loaded, current_max = get_progress(conn)
    logger.info(f"Current status: {teams_loaded} teams loaded, max ID: {current_max}")
    
    # Create requests session for connection reuse
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'SASP-ETL/1.0',
        'Accept': 'application/json'
    })
    
    # Counters
    successful_loads = 0
    failed_loads = 0
    skipped_existing = 0
    not_found = 0
    batch_count = 0
    
    start_time = time.time()
    last_commit_time = time.time()
    
    try:
        # Process each team ID in the actual API range
        for api_team_id in range(MIN_TEAM_ID, MAX_TEAM_ID + 1):
            
            # Check if team with this API ID already exists
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM raw_teams WHERE (payload->>'id')::int = %s", (api_team_id,))
                if cur.fetchone():
                    skipped_existing += 1
                    if api_team_id % 100 == 0:
                        logger.info(f"Progress: {api_team_id}/{MAX_TEAM_ID} - Team {api_team_id} already exists")
                    continue
            
            # Rate limiting delay
            time.sleep(RATE_LIMIT_DELAY)
            
            # Fetch team data
            team_data = fetch_team_with_retry(api_team_id, session)
            
            if team_data is None:
                not_found += 1
                if api_team_id % 100 == 0:
                    logger.info(f"Progress: {api_team_id}/{MAX_TEAM_ID} - Team {api_team_id} not found")
                continue
            
            # Insert team data using API ID as the database ID
            if insert_team(conn, api_team_id, team_data):
                successful_loads += 1
                batch_count += 1
                logger.info(f"✓ Loaded team {api_team_id}: {team_data.get('name', 'Unknown')}")
                
                # Commit in batches for performance and recovery
                if batch_count >= BATCH_COMMIT_SIZE:
                    conn.commit()
                    batch_count = 0
                    elapsed = time.time() - last_commit_time
                    logger.info(f"Batch committed - {successful_loads} teams loaded in {elapsed:.1f}s")
                    last_commit_time = time.time()
                    
            else:
                failed_loads += 1
                logger.error(f"✗ Failed to load team {api_team_id}")
            
            # Progress reporting
            if api_team_id % 100 == 0:
                elapsed = time.time() - start_time
                rate = successful_loads / elapsed * 60 if elapsed > 0 else 0
                remaining = MAX_TEAM_ID - api_team_id
                eta_minutes = remaining / rate if rate > 0 else 0
                
                logger.info(f"""
Progress Report - Team {api_team_id}/{MAX_TEAM_ID}:
  Successful loads: {successful_loads}
  Failed loads: {failed_loads}
  Already existed: {skipped_existing}
  Not found: {not_found}
  Rate: {rate:.1f} teams/minute
  ETA: {eta_minutes:.1f} minutes
                """)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user - committing current progress")
        
    finally:
        # Final commit
        conn.commit()
        conn.close()
        
        # Final statistics
        total_time = time.time() - start_time
        logger.info(f"""
=== TEAM INGESTION COMPLETE ===
Total time: {total_time/60:.1f} minutes
Successful loads: {successful_loads}
Failed loads: {failed_loads}
Already existed: {skipped_existing}
Not found: {not_found}
Average rate: {successful_loads/(total_time/60):.1f} teams/minute
        """)

if __name__ == "__main__":
    main()