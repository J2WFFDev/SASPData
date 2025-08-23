#!/usr/bin/env python3
"""
Concurrent competition ingestion with pagination support.
Discovers ALL competitions across all pages and extracts match IDs for further processing.
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
BASE_URL = "https://virtual.sssfonline.com/api/shot/SASP/competitions"
MAX_WORKERS = 5  # Conservative concurrency for API rate limiting
RATE_LIMIT_DELAY = 3.0  # 3 seconds between requests = 20/minute (safe)
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('competition_ingestion.log', encoding='utf-8'),
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


def fetch_competition_page(page_num: int, session: requests.Session) -> Optional[Dict[str, Any]]:
    """
    Fetch a single page of competition data with retry logic.
    
    Args:
        page_num: Page number to fetch (1-based)
        session: Requests session with headers configured
        
    Returns:
        Competition data dict or None on failure
    """
    url = f"{BASE_URL}?type=S&page={page_num}"
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Fetching page {page_num}, attempt {attempt + 1}")
            
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                competitions_count = len(data.get('data', []))
                logger.info(f"✓ Page {page_num}: {competitions_count} competitions")
                return data
                
            elif response.status_code == 404:
                logger.info(f"⚬ Page {page_num}: Not found (end of data)")
                return None
                
            else:
                logger.warning(f"⚠ Page {page_num}: HTTP {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Page {page_num}, attempt {attempt + 1}: {e}")
            
        # Exponential backoff
        if attempt < MAX_RETRIES - 1:
            delay = RATE_LIMIT_DELAY * (BACKOFF_FACTOR ** attempt)
            logger.debug(f"Retrying page {page_num} in {delay:.1f}s...")
            time.sleep(delay)
    
    logger.error(f"❌ Failed to fetch page {page_num} after {MAX_RETRIES} attempts")
    return None


def insert_competition_data(conn: psycopg2.extensions.connection, page_data: Dict[str, Any]) -> int:
    """
    Insert competition page data into raw_competition table.
    
    Args:
        conn: Database connection
        page_data: Competition page data from API
        
    Returns:
        Number of records inserted (0 if duplicate)
    """
    canonical = json.dumps(page_data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    source_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    
    # Extract page number from meta if available
    page_num = page_data.get('meta', {}).get('current_page', 1)
    source_url = f"{BASE_URL}?type=S&page={page_num}"
    
    insert_sql = """
    INSERT INTO raw_competition (match_number, payload, source, source_hash)
    VALUES (%s, %s::jsonb, %s, %s)
    ON CONFLICT (source_hash) DO NOTHING
    RETURNING id;
    """
    
    with conn.cursor() as cur:
        try:
            cur.execute(insert_sql, (page_num, json.dumps(page_data), source_url, source_hash))
            row = cur.fetchone()
            
            if row:
                logger.info(f"✓ Inserted competition page {page_num} (id={row[0]})")
                return 1
            else:
                logger.debug(f"⚬ Competition page {page_num} already exists")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Failed to insert page {page_num}: {e}")
            return 0


def discover_total_pages() -> int:
    """
    Discover the total number of pages by fetching page 1.
    
    Returns:
        Total number of pages available
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'SASP-Competition-Discovery/1.0',
        'Accept': 'application/json'
    })
    
    try:
        response = session.get(f"{BASE_URL}?type=S&page=1", timeout=30)
        if response.status_code == 200:
            data = response.json()
            total_pages = data.get('meta', {}).get('last_page', 1)
            total_competitions = data.get('meta', {}).get('total', 0)
            logger.info(f"🔍 Discovery: {total_pages} pages containing {total_competitions} total competitions")
            return total_pages
        else:
            logger.error(f"❌ Discovery failed: HTTP {response.status_code}")
            return 1
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}")
        return 1


def extract_match_ids_from_competitions() -> List[int]:
    """
    Extract all competition IDs from the raw_competition table for scoreboard ingestion.
    
    Returns:
        List of competition IDs that can be used for scoreboard/schedule fetching
    """
    conn = get_db_connection()
    match_ids = []
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT (comp->>'id')::int as competition_id
                FROM raw_competition rc,
                     jsonb_array_elements(rc.payload->'data') AS comp
                WHERE comp->>'id' IS NOT NULL
                ORDER BY (comp->>'id')::int
            """)
            
            match_ids = [row[0] for row in cur.fetchall()]
            logger.info(f"📊 Extracted {len(match_ids)} competition IDs for scoreboard ingestion")
            
    except Exception as e:
        logger.error(f"❌ Failed to extract competition IDs: {e}")
    finally:
        conn.close()
    
    return match_ids


def main():
    """Main concurrent competition ingestion process."""
    logger.info("🚀 Starting concurrent competition ingestion...")
    
    start_time = time.time()
    
    # Discover total pages
    total_pages = discover_total_pages()
    if total_pages <= 0:
        logger.error("❌ Could not determine total pages, aborting")
        return
    
    # Create session with headers
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'SASP-Competition-Ingest/1.0',
        'Accept': 'application/json'
    })
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        # Process pages concurrently
        successful_pages = 0
        failed_pages = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all page fetch tasks
            future_to_page = {
                executor.submit(fetch_competition_page, page_num, session): page_num 
                for page_num in range(1, total_pages + 1)
            }
            
            # Process completed tasks
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                
                try:
                    page_data = future.result()
                    
                    if page_data:
                        # Insert into database
                        inserted = insert_competition_data(conn, page_data)
                        successful_pages += 1  # Count as successful even if duplicate
                        
                        # Rate limiting between inserts
                        time.sleep(RATE_LIMIT_DELAY / MAX_WORKERS)
                        
                    else:
                        failed_pages += 1
                        
                except Exception as e:
                    logger.error(f"❌ Task failed for page {page_num}: {e}")
                    failed_pages += 1
        
        # Commit all changes
        conn.commit()
        
        # Extract competition IDs for next phase
        competition_ids = extract_match_ids_from_competitions()
        
        # Final statistics
        total_time = time.time() - start_time
        logger.info(f"""
=== COMPETITION INGESTION COMPLETE ===
Total time: {total_time:.1f} seconds
Successful pages: {successful_pages}
Failed pages: {failed_pages}
Total competitions discovered: {len(competition_ids)}
Rate: {(successful_pages / total_time * 60):.1f} pages/minute

🎯 Ready for phase 2: Scoreboard ingestion for {len(competition_ids)} competitions
        """)
        
    except KeyboardInterrupt:
        logger.info("❌ Interrupted by user")
    finally:
        conn.close()


if __name__ == "__main__":
    main()