#!/usr/bin/env python3
"""
Script to ingest missing teams based on team IDs referenced in scoreboard data.
This ensures we have complete team data for silver transformations.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add src to path so we can import ingest functions
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from ingest import get_db_conn, ingest_endpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("complete_teams")

load_dotenv()

def find_missing_team_ids():
    """Find team IDs referenced in scoreboard but missing from raw_teams"""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                WITH scoreboard_teams AS (
                    SELECT DISTINCT 
                        (jsonb_array_elements(payload->'teams')->'disciplines'->0->'athletes'->0->>'ent_id')::int as team_id
                    FROM raw_scoreboard 
                    WHERE payload->'teams' IS NOT NULL
                ),
                existing_teams AS (
                    SELECT (payload->>'id')::int as team_id
                    FROM raw_teams 
                    WHERE payload->>'id' IS NOT NULL
                )
                SELECT st.team_id
                FROM scoreboard_teams st
                LEFT JOIN existing_teams et ON st.team_id = et.team_id
                WHERE et.team_id IS NULL AND st.team_id > 0
                ORDER BY st.team_id
            ''')
            
            missing_ids = [row[0] for row in cur.fetchall()]
            logger.info(f"Found {len(missing_ids)} missing team IDs: {missing_ids}")
            return missing_ids
    finally:
        conn.close()

def ingest_teams_by_ids(team_ids):
    """Ingest specific team IDs from the API"""
    conn = get_db_conn()
    try:
        success_count = 0
        for team_id in team_ids:
            url = f"https://virtual.sssfonline.com/api/teams/{team_id}"
            logger.info(f"Ingesting team ID {team_id} from {url}")
            
            if ingest_endpoint(conn, "raw_teams", url):
                success_count += 1
                logger.info(f"Successfully ingested team {team_id}")
            else:
                logger.warning(f"Failed to ingest team {team_id}")
        
        logger.info(f"Team ingestion complete: {success_count}/{len(team_ids)} successful")
        return success_count
    finally:
        conn.close()

def main():
    logger.info("Starting complete teams ingestion...")
    
    # Find missing teams
    missing_ids = find_missing_team_ids()
    
    if not missing_ids:
        logger.info("No missing teams found - all referenced teams are already ingested")
        return
    
    logger.info(f"Need to ingest {len(missing_ids)} missing teams")
    
    # Ingest missing teams
    success_count = ingest_teams_by_ids(missing_ids)
    
    # Verify completion
    remaining_missing = find_missing_team_ids()
    if remaining_missing:
        logger.warning(f"Still missing {len(remaining_missing)} teams: {remaining_missing}")
    else:
        logger.info("All teams successfully ingested!")

if __name__ == "__main__":
    main()