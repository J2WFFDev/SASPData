#!/usr/bin/env python3
"""Enhance dim_athlete table with classification columns and functions."""

import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

try:
    print('=== ENHANCING DIM_ATHLETE TABLE ===')
    
    # Read and execute the enhancement SQL
    with open('sql/09_enhance_dim_athlete.sql', 'r') as f:
        sql_content = f.read()
    
    with conn.cursor() as cur:
        print('✓ Adding classification columns to dim_athlete...')
        cur.execute(sql_content)
        conn.commit()
        
        # Verify the new columns exist
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'dim_athlete' 
            AND column_name IN ('classification_key', 'division_name', 'class_name', 'is_ghost_athlete')
            ORDER BY column_name;
        """)
        
        columns = cur.fetchall()
        if len(columns) >= 4:
            print('✅ All classification columns added successfully:')
            for col_name, data_type, nullable in columns:
                print(f'   • {col_name}: {data_type} (nullable: {nullable})')
        else:
            print(f'⚠️  Only {len(columns)}/4 columns found')
        
        # Test the classification functions
        print('\n🧪 Testing classification functions:')
        
        # Test the classify_athlete function with a sample
        cur.execute("""
            SELECT athlete_key, fname, lname 
            FROM dim_athlete 
            LIMIT 1;
        """)
        
        sample_athlete = cur.fetchone()
        if sample_athlete:
            athlete_key, fname, lname = sample_athlete
            print(f'✓ Testing with sample athlete: {fname} {lname} (key: {athlete_key})')
            
            # Test the function
            cur.execute("""
                SELECT classify_athlete(%s, 'Test Team', 'Test Discipline');
            """, (athlete_key,))
            
            result = cur.fetchone()[0]
            print(f'✓ classify_athlete() returned: {result}')
        
        # Check total athlete count
        cur.execute("SELECT COUNT(*) FROM dim_athlete;")
        total_athletes = cur.fetchone()[0]
        
        # Check how many have classifications
        cur.execute("SELECT COUNT(*) FROM dim_athlete WHERE classification_key IS NOT NULL;")
        classified_athletes = cur.fetchone()[0]
        
        print(f'\n📊 Athlete Classification Status:')
        print(f'   • Total athletes: {total_athletes:,}')
        print(f'   • Classified: {classified_athletes:,}')
        print(f'   • Unclassified: {total_athletes - classified_athletes:,}')
        
        if total_athletes > 0:
            print(f'   • Classification rate: {(classified_athletes/total_athletes)*100:.1f}%')
        
        print('\n✅ dim_athlete enhancement completed successfully!')
        print('🎯 Ready for athlete classification and ETL aggregation pipeline!')
        
except Exception as e:
    print(f'❌ Enhancement failed: {e}')
    import traceback
    traceback.print_exc()
finally:
    conn.close()