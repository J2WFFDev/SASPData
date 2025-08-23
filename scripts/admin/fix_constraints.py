#!/usr/bin/env python3
"""Fix the constraint issue in fact_stage_performance table."""

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
        print('🔧 Fixing fact_stage_performance constraints...')
        
        # Drop the problematic constraint
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            DROP CONSTRAINT IF EXISTS fact_stage_performance_dropped_string_number_check;
        """)
        
        # Drop the strings_count constraint too
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            DROP CONSTRAINT IF EXISTS fact_stage_performance_strings_count_check;
        """)
        
        # Add a new constraint that allows NULL
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            ADD CONSTRAINT chk_dropped_string_valid 
            CHECK (dropped_string_number IS NULL OR dropped_string_number BETWEEN 1 AND 5);
        """)
        
        # Add a new constraint for strings_count that allows 1-4
        cur.execute("""
            ALTER TABLE fact_stage_performance 
            ADD CONSTRAINT chk_strings_count_valid 
            CHECK (strings_count BETWEEN 1 AND 4);
        """)
        
        conn.commit()
        print('✅ Constraints fixed successfully!')
        
except Exception as e:
    print(f'❌ Error: {e}')
    conn.rollback()
finally:
    conn.close()