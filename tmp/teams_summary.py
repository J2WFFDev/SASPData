import os, json, traceback
import psycopg2

try:
    conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=os.getenv('PGPORT','5432'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_teams")
    total = cur.fetchone()[0]
    print(f"raw_teams count: {total}")
    cur.execute("SELECT id, match_number, source, source_hash, payload FROM raw_teams ORDER BY id DESC LIMIT 10")
    rows = cur.fetchall()
    for r in rows:
        rid, match_number, source, source_hash, payload = r
        try:
            p = payload if isinstance(payload, dict) else json.loads(payload)
            preview = {k: p[k] for k in list(p.keys())[:5]} if isinstance(p, dict) else str(p)[:200]
        except Exception:
            preview = str(payload)[:200]
        print('\n---')
        print('id=', rid, 'match_number=', match_number, 'source=', source)
        print('source_hash=', source_hash)
        print('preview keys/values:', json.dumps(preview, ensure_ascii=False, indent=2))
    cur.close(); conn.close()
except Exception:
    traceback.print_exc()
    print('Failed to query raw_teams')
