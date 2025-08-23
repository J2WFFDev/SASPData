import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

print('=== DEPLOYING DIM_STAGE TABLE ===')

try:
    conn = psycopg2.connect(
        host=os.getenv('PGHOST'),
        port=int(os.getenv('PGPORT')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )
    
    with conn.cursor() as cur:
        # Read and execute the dim_stage SQL
        with open('sql/04_dim_stage.sql', 'r') as f:
            sql_content = f.read()
        
        print('✓ Executing dim_stage DDL...')
        cur.execute(sql_content)
        conn.commit()
        
        # Test the table was created
        cur.execute('SELECT COUNT(*) FROM dim_stage')
        count = cur.fetchone()[0]
        print(f'✓ dim_stage table created with {count} records')
        
        # Test the lookup function
        cur.execute("SELECT get_stage_key('go fast')")
        stage_key = cur.fetchone()[0]
        print(f'✓ Lookup function test: get_stage_key("go fast") = {stage_key}')
        
        # Show all stages
        cur.execute('''
            SELECT stage_number, stage_name_standard, stage_short_code, stage_type 
            FROM dim_stage 
            ORDER BY stage_number
        ''')
        
        print('\n📋 All stages deployed:')
        for num, name, code, stype in cur.fetchall():
            print(f'  Stage {num}: {name} ({code}) - {stype}')
    
    conn.close()
    print('\n🚀 dim_stage deployment SUCCESSFUL!')
    
except Exception as e:
    print(f'❌ Deployment failed: {e}')
    import traceback
    traceback.print_exc()