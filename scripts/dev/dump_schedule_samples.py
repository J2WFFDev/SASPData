import os, json
import psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=os.getenv('PGPORT','5432'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
cur = conn.cursor()
out = {}
for tbl in ['silver_schedule','silver_schedule_slot','silver_slot_flight','silver_slot_lineup']:
    cur.execute(f"SELECT * FROM {tbl} LIMIT 10")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    out[tbl] = rows
with open('tmp_silver_schedule_samples.json','w', encoding='utf-8') as f:
    json.dump(out, f, default=str, indent=2)
print('Wrote tmp_silver_schedule_samples.json')
cur.close(); conn.close()
