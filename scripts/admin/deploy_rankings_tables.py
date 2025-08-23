#!/usr/bin/env python3
"""
Deploy Rankings Tables
Purpose: Deploy rankings_individual and rankings_squad tables
Usage: python deploy_rankings_tables.py
"""

import sys
import os
import logging
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def deploy_rankings_tables():
    """Deploy rankings tables"""
    logger = setup_logging()
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        db = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        db.autocommit = True
        cursor = db.cursor()
        
        print("=== DEPLOYING RANKINGS TABLES ===\n")
        
        # Deploy individual rankings table
        print("✓ Deploying rankings_individual...")
        sql_file = os.path.join(os.path.dirname(__file__), '..', '..', 'sql', '10_rankings_individual.sql')
        with open(sql_file, 'r') as f:
            sql = f.read()
        cursor.execute(sql)
        print("✅ rankings_individual created successfully")
        
        # Deploy squad rankings table  
        print("\n✓ Deploying rankings_squad...")
        sql_file = os.path.join(os.path.dirname(__file__), '..', '..', 'sql', '11_rankings_squad.sql')
        with open(sql_file, 'r') as f:
            sql = f.read()
        cursor.execute(sql)
        print("✅ rankings_squad created successfully")
        
        # Verify table structures
        print("\n📊 Rankings Tables Summary:")
        
        # Check individual rankings
        cursor.execute("""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'rankings_individual'
        """)
        individual_cols = cursor.fetchone()[0]
        print(f"✓ rankings_individual: {individual_cols} columns")
        
        # Check squad rankings
        cursor.execute("""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'rankings_squad'
        """)
        squad_cols = cursor.fetchone()[0]
        print(f"✓ rankings_squad: {squad_cols} columns")
        
        print(f"\n🎯 All rankings tables ready for ETL!")
        print(f"📈 Analytics ready: Performance → Rankings → Awards")
        
        return True
        
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        print(f"\n❌ Error: {e}")
        return False
    
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    success = deploy_rankings_tables()
    sys.exit(0 if success else 1)