import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('PGHOST'),
        port=int(os.getenv('PGPORT')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )
    
    with conn.cursor() as cur:
        print('=== TESTING 8-STAGE MAPPING AGAINST YOUR DATA ===')
        
        # Get all unique stage names from raw_scoreboard using your Power BI approach
        print('\n🔍 EXTRACTING ALL STAGE NAMES FROM RAW_SCOREBOARD:')
        cur.execute('''
            SELECT DISTINCT 
                payload->>'stage_one' as stage_one,
                payload->>'stage_two' as stage_two, 
                payload->>'stage_three' as stage_three,
                payload->>'stage_four' as stage_four
            FROM raw_scoreboard
            WHERE payload->>'stage_one' IS NOT NULL
            LIMIT 10
        ''')
        
        print('Sample stage combinations found:')
        stage_names_found = set()
        for row in cur.fetchall():
            stage_one, stage_two, stage_three, stage_four = row
            print(f'  {stage_one} | {stage_two} | {stage_three} | {stage_four}')
            for stage in [stage_one, stage_two, stage_three, stage_four]:
                if stage:
                    stage_names_found.add(stage.strip().lower())
        
        print(f'\n📊 UNIQUE STAGE NAMES FOUND: {len(stage_names_found)}')
        for i, stage in enumerate(sorted(stage_names_found), 1):
            print(f'  {i:2}. "{stage}"')
        
        # Check how many use 'go fast' as stage_one
        print('\n🎯 VERIFYING STAGE 1 = "GO FAST" PATTERN:')
        cur.execute('''
            SELECT 
                payload->>'stage_one' as stage_one,
                COUNT(*) as count
            FROM raw_scoreboard
            WHERE payload->>'stage_one' IS NOT NULL
            GROUP BY payload->>'stage_one'
            ORDER BY COUNT(*) DESC
        ''')
        
        print('Stage 1 (stage_one) frequency:')
        for stage_name, count in cur.fetchall():
            print(f'  "{stage_name}": {count} times')
    
    conn.close()
    
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()