import os, json, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(host=os.getenv('PGHOST'), port=os.getenv('PGPORT'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
cur = conn.cursor()
cur.execute("SELECT id, payload, source FROM raw_competition ORDER BY id LIMIT 10")
rows = cur.fetchall()
out = []
for r in rows:
    out.append({'id': r[0], 'source': r[2], 'payload': r[1]})
open('tmp_raw_competition_rows.json','w', encoding='utf-8').write(json.dumps(out, default=str, indent=2))
print('Wrote tmp_raw_competition_rows.json')
cur.close(); conn.close()
