#!/usr/bin/env python3
"""Check raw_teams table structure."""

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('PGHOST'),
    port=os.getenv('PGPORT'),
    database=os.getenv('PGDATABASE'),
    user=os.getenv('PGUSER'),
    password=os.getenv('PGPASSWORD')
)

with conn.cursor() as cur:
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default 
        FROM information_schema.columns 
        WHERE table_name = 'raw_teams' 
        ORDER BY ordinal_position;
    """)
    
    print('raw_teams table structure:')
    print('Column Name      Data Type            Nullable  Default')
    print('-' * 65)
    for row in cur.fetchall():
        col_name, data_type, nullable, default = row
        default_str = str(default) if default else 'None'
        print(f'{col_name:15} {data_type:20} {nullable:8} {default_str}')

conn.close()