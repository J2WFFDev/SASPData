#!/usr/bin/env python3
"""Deploy fact performance tables for analytics aggregation."""

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

def deploy_table(sql_file, table_name):
    """Deploy a single table from SQL file."""
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        with conn.cursor() as cur:
            print(f'✓ Deploying {table_name}...')
            cur.execute(sql_content)
            conn.commit()
            
            # Check table creation
            cur.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name.lower()}';
            """)
            
            if cur.fetchone()[0] > 0:
                print(f'✅ {table_name} created successfully')
                return True
            else:
                print(f'❌ {table_name} not found after deployment')
                return False
                
    except Exception as e:
        print(f'❌ Error deploying {table_name}: {e}')
        return False

try:
    print('=== DEPLOYING PERFORMANCE TABLES ===\n')
    
    tables = [
        ('sql/06_fact_stage_performance.sql', 'fact_stage_performance'),
        ('sql/07_fact_match_performance.sql', 'fact_match_performance'), 
        ('sql/08_fact_squad_performance.sql', 'fact_squad_performance')
    ]
    
    success_count = 0
    for sql_file, table_name in tables:
        if deploy_table(sql_file, table_name):
            success_count += 1
        print()
    
    print(f'📊 Performance Tables Summary:')
    print(f'✓ {success_count}/{len(tables)} tables deployed successfully')
    
    if success_count == len(tables):
        print('\n🎯 All performance tables ready for ETL aggregation!')
        print('📈 Analytics hierarchy: fact_entry_strings → fact_stage_performance → fact_match_performance → fact_squad_performance')
    else:
        print(f'\n⚠️  {len(tables) - success_count} table(s) failed deployment')
        
except Exception as e:
    print(f'❌ Deployment failed: {e}')
finally:
    conn.close()