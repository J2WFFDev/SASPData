#!/usr/bin/env python3
"""
Silver Layer ETL Orchestrator
Runs the complete transformation pipeline from raw JSON to silver dimensional model.
"""

import os
import sys
import logging
import argparse
from datetime import datetime

# Add silver directory to path
sys.path.append(os.path.dirname(__file__))

from transform_utils import get_db_conn, truncate_silver_tables, logger
from transform_dimensions import main as transform_dimensions
from transform_facts import main as transform_facts

def create_silver_tables(conn):
    """Create silver tables using DDL scripts"""
    logger.info("Creating silver tables...")
    
    script_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Execute dimension DDL
    ddl_dims = os.path.join(script_dir, 'sql', '01_dims.sql')
    if os.path.exists(ddl_dims):
        with open(ddl_dims, 'r', encoding='utf-8') as f:
            ddl_sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
        logger.info("Dimension tables created/updated")
    
    # Execute fact DDL  
    ddl_facts = os.path.join(script_dir, 'sql', '02_facts.sql')
    if os.path.exists(ddl_facts):
        with open(ddl_facts, 'r', encoding='utf-8') as f:
            ddl_sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
        logger.info("Fact tables created/updated")
    
    conn.commit()

def validate_raw_data(conn):
    """Validate that required raw data exists"""
    logger.info("Validating raw data...")
    
    required_tables = ['raw_competition', 'raw_teams', 'raw_schedule', 'raw_scoreboard']
    
    with conn.cursor() as cur:
        for table in required_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            
            if count == 0:
                raise ValueError(f"No data found in {table} - please run ingestion first")
            
            logger.info(f"{table}: {count} records")

def run_data_quality_checks(conn):
    """Run basic data quality checks on silver tables"""
    logger.info("Running data quality checks...")
    
    checks = [
        ("Competitions loaded", "SELECT COUNT(*) FROM dim_competition"),
        ("Teams loaded", "SELECT COUNT(*) FROM dim_team"),
        ("Athletes loaded", "SELECT COUNT(*) FROM dim_athlete"),
        ("Entries loaded", "SELECT COUNT(*) FROM fact_entry"),
        ("Entry strings loaded", "SELECT COUNT(*) FROM fact_entry_strings"),
        ("Valid entries %", """
            SELECT ROUND(
                100.0 * COUNT(CASE WHEN is_valid THEN 1 END) / COUNT(*), 2
            ) FROM fact_entry
        """),
        ("DQ rate %", """
            SELECT ROUND(
                100.0 * COUNT(CASE WHEN dq_tag THEN 1 END) / COUNT(*), 2
            ) FROM fact_entry
        """),
        ("Entries with missing dimensions", """
            SELECT COUNT(*) FROM fact_entry 
            WHERE competition_key IS NULL OR team_key IS NULL 
               OR discipline_key IS NULL OR athlete_key IS NULL
        """)
    ]
    
    with conn.cursor() as cur:
        logger.info("=== DATA QUALITY REPORT ===")
        for check_name, sql in checks:
            cur.execute(sql)
            result = cur.fetchone()[0]
            logger.info(f"{check_name}: {result}")

def main():
    parser = argparse.ArgumentParser(description='SASP Silver Layer ETL')
    parser.add_argument('--rebuild', action='store_true', 
                       help='Truncate existing silver tables before transform')
    parser.add_argument('--skip-ddl', action='store_true',
                       help='Skip DDL creation (tables already exist)')
    parser.add_argument('--dims-only', action='store_true',
                       help='Only transform dimensions')
    parser.add_argument('--facts-only', action='store_true',
                       help='Only transform facts (requires dimensions)')
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info(f"Starting silver ETL at {start_time}")
    
    conn = get_db_conn()
    conn.autocommit = False
    
    try:
        # Validate raw data
        validate_raw_data(conn)
        
        # Create tables if needed
        if not args.skip_ddl:
            create_silver_tables(conn)
        
        # Truncate if rebuilding
        if args.rebuild:
            logger.info("Rebuilding - truncating silver tables...")
            truncate_silver_tables(conn)
        
        # Transform dimensions
        if not args.facts_only:
            logger.info("=== TRANSFORMING DIMENSIONS ===")
            transform_dimensions()
        
        # Transform facts
        if not args.dims_only:
            logger.info("=== TRANSFORMING FACTS ===")
            transform_facts()
        
        # Data quality checks
        run_data_quality_checks(conn)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Silver ETL completed successfully in {duration}")
        
    except Exception as e:
        logger.error(f"Silver ETL failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()