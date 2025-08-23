#!/usr/bin/env python3
"""
Concurrent scoreboard ingestion for all discovered competitions.
Fetches scoreboard data for all competition IDs with proper rate limiting.
"""

import os
import sys
import time
import json
import hashlib
import logging
import requests
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Load environment
load_dotenv()

# Configuration
BASE_URL = "https://virtual.sssfonline.com/api/shot/sasp-scoreboard"
MAX_WORKERS = 5  # Conservative concurrency for API rate limiting
RATE_LIMIT_DELAY = 3.0  # 3 seconds between requests = 20/minute (safe)
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0
BATCH_COMMIT_SIZE = 50  # Commit every N successful inserts

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scoreboard_ingestion.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create database connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "saspd"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def get_competition_ids() -> List[int]:
    """
    Extract all competition IDs from the raw_competition table.
    
    Returns:
        List of competition IDs for scoreboard fetching
    """
    conn = get_db_connection()
    competition_ids = []
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT (comp->>'id')::int as competition_id
                FROM raw_competition rc,
                     jsonb_array_elements(rc.payload->'data') AS comp
                WHERE comp->>'id' IS NOT NULL
                ORDER BY (comp->>'id')::int
            """)
            
            competition_ids = [row[0] for row in cur.fetchall()]
            logger.info(f"📊 Found {len(competition_ids)} competitions for scoreboard ingestion")
            
    except Exception as e:
        logger.error(f"❌ Failed to extract competition IDs: {e}")
    finally:
        conn.close()
    
    return competition_ids


def check_existing_scoreboards() -> set:
    """
    Check which scoreboards we already have to avoid unnecessary API calls.
    
    Returns:
        Set of competition IDs we already have scoreboard data for
    """
    conn = get_db_connection()
    existing_ids = set()
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT match_number FROM raw_scoreboard WHERE match_number IS NOT NULL")
            existing_ids = {row[0] for row in cur.fetchall()}
            logger.info(f"📋 Found existing scoreboard data for {len(existing_ids)} competitions")
            
    except Exception as e:
        logger.error(f"❌ Failed to check existing scoreboards: {e}")
    finally:
        conn.close()
    
    return existing_ids


def fetch_scoreboard(competition_id: int, session: requests.Session) -> Optional[Dict[str, Any]]:
    """
    Fetch scoreboard data for a single competition with retry logic.
    
    Args:
        competition_id: Competition ID to fetch
        session: Requests session with headers configured
        
    Returns:
        Scoreboard data dict or None on failure
    """
    url = f"{BASE_URL}/{competition_id}"
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Fetching scoreboard {competition_id}, attempt {attempt + 1}")
            
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                entries_count = len(data.get('EntryData', [])) if isinstance(data, dict) else 0
                logger.info(f"✓ Competition {competition_id}: {entries_count} entries")
                return data
                
            elif response.status_code == 404:
                logger.debug(f"⚬ Competition {competition_id}: No scoreboard data")
                return None
                
            else:
                logger.warning(f"⚠ Competition {competition_id}: HTTP {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Competition {competition_id}, attempt {attempt + 1}: {e}")
            
        # Exponential backoff
        if attempt < MAX_RETRIES - 1:
            delay = RATE_LIMIT_DELAY * (BACKOFF_FACTOR ** attempt)
            logger.debug(f"Retrying competition {competition_id} in {delay:.1f}s...")
            time.sleep(delay)
    
    logger.error(f"❌ Failed to fetch competition {competition_id} after {MAX_RETRIES} attempts")
    return None


def insert_scoreboard_data(conn: psycopg2.extensions.connection, competition_id: int, scoreboard_data: Dict[str, Any]) -> bool:
    """
    Insert scoreboard data into raw_scoreboard table.
    
    Args:
        conn: Database connection
        competition_id: Competition ID
        scoreboard_data: Scoreboard data from API
        
    Returns:
        True if inserted successfully, False if duplicate or error
    """
    canonical = json.dumps(scoreboard_data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    source_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    source_url = f"{BASE_URL}/{competition_id}"
    
    insert_sql = """
    INSERT INTO raw_scoreboard (match_number, payload, source, source_hash)
    VALUES (%s, %s::jsonb, %s, %s)
    ON CONFLICT (source_hash) DO NOTHING
    RETURNING id;
    """
    
    with conn.cursor() as cur:
        try:
            cur.execute(insert_sql, (competition_id, json.dumps(scoreboard_data), source_url, source_hash))
            row = cur.fetchone()
            
            if row:
                logger.debug(f"✓ Inserted scoreboard {competition_id} (id={row[0]})")
                return True
            else:
                logger.debug(f"⚬ Scoreboard {competition_id} already exists")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to insert scoreboard {competition_id}: {e}")
            return False


def process_scoreboard_batch(competition_ids: List[int], session: requests.Session, conn: psycopg2.extensions.connection) -> Dict[str, int]:
    """
    Process a batch of competition IDs for scoreboard fetching.
    
    Args:
        competition_ids: List of competition IDs to process
        session: Requests session
        conn: Database connection
        
    Returns:
        Dict with counts of successful, failed, not_found, and duplicate operations
    """
    results = {
        'successful': 0,
        'failed': 0,
        'not_found': 0,
        'duplicate': 0
    }
    
    batch_start_time = time.time()
    
    for competition_id in competition_ids:
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY / MAX_WORKERS)
        
        scoreboard_data = fetch_scoreboard(competition_id, session)
        
        if scoreboard_data:
            if insert_scoreboard_data(conn, competition_id, scoreboard_data):
                results['successful'] += 1
            else:
                results['duplicate'] += 1
        else:
            results['not_found'] += 1
    
    batch_duration = time.time() - batch_start_time
    rate = len(competition_ids) / batch_duration if batch_duration > 0 else 0
    
    logger.info(f"📦 Batch complete: {len(competition_ids)} competitions in {batch_duration:.1f}s ({rate:.1f} req/sec)")
    
    return results


def main():
    """Main concurrent scoreboard ingestion process."""
    logger.info("🏆 Starting concurrent scoreboard ingestion...")
    
    start_time = time.time()
    
    # Get all competition IDs
    all_competition_ids = get_competition_ids()
    if not all_competition_ids:
        logger.error("❌ No competition IDs found, aborting")
        return
    
    # Check existing scoreboards to avoid duplicates
    existing_scoreboards = check_existing_scoreboards()
    
    # Filter to only new competitions
    new_competition_ids = [cid for cid in all_competition_ids if cid not in existing_scoreboards]
    
    logger.info(f"📋 Processing {len(new_competition_ids)} new competitions (skipping {len(existing_scoreboards)} existing)")
    
    if not new_competition_ids:
        logger.info("✅ All scoreboards already exist, nothing to do")
        return
    
    # Create session with headers
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'SASP-Scoreboard-Ingest/1.0',
        'Accept': 'application/json'
    })
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        # Split into batches for processing
        batch_size = MAX_WORKERS * 10  # Process in chunks
        total_results = {
            'successful': 0,
            'failed': 0,
            'not_found': 0,
            'duplicate': 0
        }
        
        for i in range(0, len(new_competition_ids), batch_size):
            batch = new_competition_ids[i:i + batch_size]
            logger.info(f"🚀 Processing batch {i//batch_size + 1}: competitions {batch[0]} to {batch[-1]}")
            
            # Process this batch with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Split batch among workers
                worker_batch_size = max(1, len(batch) // MAX_WORKERS)
                
                futures = []
                for worker_start in range(0, len(batch), worker_batch_size):
                    worker_batch = batch[worker_start:worker_start + worker_batch_size]
                    future = executor.submit(process_scoreboard_batch, worker_batch, session, conn)
                    futures.append(future)
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        batch_results = future.result()
                        for key, value in batch_results.items():
                            total_results[key] += value
                    except Exception as e:
                        logger.error(f"❌ Batch processing failed: {e}")
                        total_results['failed'] += 1
            
            # Commit batch
            conn.commit()
            logger.info(f"✅ Batch {i//batch_size + 1} committed to database")
        
        # Final statistics
        total_time = time.time() - start_time
        total_processed = sum(total_results.values())
        
        logger.info(f"""
=== SCOREBOARD INGESTION COMPLETE ===
Total time: {total_time/60:.1f} minutes
Total processed: {total_processed}
✅ Successful: {total_results['successful']}
⚬ Not found: {total_results['not_found']}
🔄 Duplicates: {total_results['duplicate']}
❌ Failed: {total_results['failed']}
📈 Rate: {(total_processed / total_time * 60):.1f} competitions/minute

🎯 Scoreboards now available for silver layer processing
        """)
        
    except KeyboardInterrupt:
        logger.info("❌ Interrupted by user")
    finally:
        conn.close()


if __name__ == "__main__":
    main()