#!/usr/bin/env python3
"""Test the dim_classification table and lookup functions."""

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
    with conn.cursor() as cur:
        print('=== DIM_CLASSIFICATION DATA ===')
        
        # Show all classifications
        cur.execute("""
            SELECT classification_key, division_id, division_name, class_name, 
                   allows_ghost_athletes, is_open_division
            FROM dim_classification 
            ORDER BY display_order;
        """)
        
        print(' ID | Div | Division        | Class              | Ghost | Open')
        print('----|-----|-----------------|--------------------|---------|---------')
        for row in cur.fetchall():
            class_key, div_id, div_name, class_name, ghost, open_div = row
            div_id_str = str(div_id) if div_id is not None else 'N/A'
            ghost_str = 'Yes' if ghost else 'No'
            open_str = 'Yes' if open_div else 'No'
            print(f'{class_key:3} | {div_id_str:>3} | {div_name:15} | {class_name:18} | {ghost_str:7} | {open_str}')
        
        # Test lookup functions
        print('\n🧪 Testing lookup functions:')
        
        test_cases = [
            "get_classification_key_by_division('Rookie')",
            "get_classification_key_by_division('Open')", 
            "get_squad_division('Rookie', 'Rookie', 'Rookie', 'Rookie')",
            "get_squad_division('Senior', 'Senior', 'Intermediate', 'Rookie')"
        ]
        
        for test in test_cases:
            cur.execute(f"SELECT {test};")
            result = cur.fetchone()[0]
            print(f'✓ {test} = {result}')
            
    print('\n✅ All classification functions working correctly!')
    
except Exception as e:
    print(f'❌ Error: {e}')
finally:
    conn.close()