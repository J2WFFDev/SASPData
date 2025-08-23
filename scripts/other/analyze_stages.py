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
        print('=== COMPREHENSIVE STAGE NAMES ANALYSIS ===')
        
        print('\n🔍 STAGE NAMES FROM dim_slot:')
        cur.execute('SELECT DISTINCT stage FROM dim_slot WHERE stage IS NOT NULL ORDER BY stage')
        
        stage_names = []
        for (stage,) in cur.fetchall():
            stage_names.append(stage)
            print(f'  "{stage}"')
        
        print(f'\nFound {len(stage_names)} unique stage names in dim_slot')
        
        print('\n🔍 CHECKING RAW DATA SOURCES:')
        
        # Check raw_schedule for stage names
        print('\nStage names in raw_schedule:')
        cur.execute('''
            SELECT DISTINCT payload->>'name' as stage_name
            FROM raw_schedule 
            WHERE payload->>'name' IS NOT NULL 
            AND payload->>'name' != ''
            ORDER BY payload->>'name'
        ''')
        
        raw_schedule_stages = []
        for (stage,) in cur.fetchall():
            if stage and stage.strip():
                raw_schedule_stages.append(stage)
                print(f'  "{stage}"')
        
        print(f'Found {len(raw_schedule_stages)} stage names in raw_schedule')
        
        # Check raw_scoreboard for stage-related fields
        print('\nChecking raw_scoreboard for stage info:')
        cur.execute('''
            SELECT payload 
            FROM raw_scoreboard 
            LIMIT 1
        ''')
        
        sample = cur.fetchone()
        if sample:
            payload = sample[0]
            stage_keys = [k for k in payload.keys() if 'stage' in k.lower()]
            print(f'  Stage-related keys in raw_scoreboard: {stage_keys}')
            
            # Look for specific stage patterns
            for key in payload.keys():
                if any(pattern in key.lower() for pattern in ['stage', 'popquiz', 'exclamation', 'focus', 'speed']):
                    print(f'  Potential stage field: {key} = {payload.get(key)}')
        
        print('\n🔍 ALL UNIQUE STAGE NAMES FOUND:')
        all_stages = set(stage_names + raw_schedule_stages)
        for i, stage in enumerate(sorted(all_stages), 1):
            print(f'  {i:2}. "{stage}"')
        
        print(f'\nTotal unique stage names: {len(all_stages)}')
        
        # Look for stage names that might contain the ones you mentioned
        print('\n🔍 SEARCHING FOR SPECIFIC PATTERNS:')
        search_terms = ['popquiz', 'pop quiz', 'exclamation', '!', 'focus', 'speed', 'victory', 'fast', 'out', 'in']
        
        for term in search_terms:
            matching_stages = [s for s in all_stages if term.lower() in s.lower()]
            if matching_stages:
                print(f'  Stages containing "{term}": {matching_stages}')
    
    conn.close()
    
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()