import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

print('=== DEPLOYING DIM_CLASSIFICATION TABLE ===')

try:
    conn = psycopg2.connect(
        host=os.getenv('PGHOST'),
        port=int(os.getenv('PGPORT')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )
    
    with conn.cursor() as cur:
        # Read and execute the dim_classification SQL
        with open('sql/05_dim_classification.sql', 'r') as f:
            sql_content = f.read()
        
        print('✓ Executing dim_classification DDL...')
        cur.execute(sql_content)
        conn.commit()
        
        # Test the table was created
        cur.execute('SELECT COUNT(*) FROM dim_classification')
        count = cur.fetchone()[0]
        print(f'✓ dim_classification table created with {count} records')
        
        # Test the lookup function
        cur.execute("SELECT get_classification_key_by_division('Rookie')")
        class_key = cur.fetchone()[0]
        print(f'✓ Lookup function test: get_classification_key_by_division("Rookie") = {class_key}')
        
        # Show all divisions
        cur.execute('''
            SELECT 
                division_id, 
                division_name, 
                class_name,
                allows_ghost_athletes,
                is_open_division
            FROM dim_classification 
            ORDER BY division_order, display_order
        ''')
        
        print('\n📋 All classifications deployed:')
        print('ID | Division        | Class              | Ghost | Open')
        print('---|-----------------|--------------------|---------|---------')
        for div_id, div_name, class_name, ghost, open_div in cur.fetchall():
            div_id_str = str(div_id) if div_id is not None else 'N/A'
            ghost_str = 'Yes' if ghost else 'No'
            open_str = 'Yes' if open_div else 'No'
            print(f'{div_id_str:>3} | {div_name:15} | {class_name:18} | {ghost_str:7} | {open_str}')
        
        # Test squad division logic
        print('\n🧪 Testing squad division logic:')
        test_cases = [
            ('Rookie', 'Rookie', 'Rookie', 'Rookie'),
            ('Senior', 'Senior', 'Intermediate', 'Senior'),
            ('Collegiate', 'Collegiate', 'Collegiate', 'Collegiate')
        ]
        
        for case in test_cases:
            cur.execute('SELECT get_squad_division(%s, %s, %s, %s)', case)
            result = cur.fetchone()[0]
            print(f'  Squad with {case} → {result}')
    
    conn.close()
    print('\n🚀 dim_classification deployment SUCCESSFUL!')
    
except Exception as e:
    print(f'❌ Deployment failed: {e}')
    import traceback
    traceback.print_exc()