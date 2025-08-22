import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=os.getenv('PGPORT','5432'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS raw_competition (
    id bigserial PRIMARY KEY,
    match_number int NULL,
    payload jsonb NOT NULL,
    source text NOT NULL,
    source_hash text NOT NULL UNIQUE,
    ingested_at timestamptz DEFAULT now()
)
''')
conn.commit()
cur.close()
conn.close()
print('Created raw_competition (if not exists)')
