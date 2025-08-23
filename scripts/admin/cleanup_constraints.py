#!/usr/bin/env python3
"""Clean up and fix all constraints in fact_stage_performance table."""

import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection
conn = psycopg2.connect(
    host=os.getenv('PGHOST'),
    port=os.getenv('PGPORT'),
    database=os.getenv('PGDATABASE'),
    user=os.getenv('PGUSER'),
    password=os.getenv('PGPASSWORD')
)

try:
    with conn.cursor() as cur:
        print('🔧 Cleaning up fact_stage_performance constraints...')
        
        # Drop all existing check constraints
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            DROP CONSTRAINT IF EXISTS fact_stage_performance_dropped_string_number_check;
        """)
        
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            DROP CONSTRAINT IF EXISTS fact_stage_performance_strings_count_check;
        """)
        
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            DROP CONSTRAINT IF EXISTS chk_dropped_string_valid;
        """)
        
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            DROP CONSTRAINT IF EXISTS chk_strings_count_valid;
        """)
        
        # Add the corrected constraints
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            ADD CONSTRAINT chk_strings_count_valid 
            CHECK (strings_count BETWEEN 1 AND 4);
        """)
        
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            ADD CONSTRAINT chk_dropped_string_valid 
            CHECK (dropped_string_number IS NULL OR dropped_string_number BETWEEN 1 AND 5);
        """)
        
        conn.commit()
        print('✅ All constraints cleaned up and fixed successfully!')
        
except Exception as e:
    print(f'❌ Error: {e}')
    conn.rollback()
finally:
    conn.close()