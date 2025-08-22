import os
import json
import traceback
from pprint import pprint
try:
    import psycopg2
except Exception as e:
    print('psycopg2 import failed:', e)
    raise

def get_conn():
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=os.getenv('PGPORT','5432'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))

try:
    conn = get_conn()
    cur = conn.cursor()
    tables = ['raw_scoreboard','raw_schedule','raw_teams','raw_competition','dim_team']
    print('Table existence and row counts:')
    for t in tables:
        cur.execute("SELECT to_regclass(%s)", (t,))
        exists = cur.fetchone()[0]
        if not exists:
            print(f" - {t}: MISSING")
            continue
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f" - {t}: {cnt} rows")

    print('\nSample raw_scoreboard rows (id, has_teams, sample teams path if present):')
    cur.execute("SELECT id, payload FROM raw_scoreboard ORDER BY id DESC LIMIT 10")
    rows = cur.fetchall()
    for r in rows:
        rid, payload = r
        try:
            if isinstance(payload, str):
                p = json.loads(payload)
            else:
                p = payload
        except Exception:
            p = None
        has_teams = False
        sample = None
        if isinstance(p, dict) and 'teams' in p:
            has_teams = True
            sample = p.get('teams')[:1]
        print(' -', rid, 'has_teams=', has_teams, 'sample=', json.dumps(sample) if sample is not None else None)

    print('\nSample raw_competition rows (id, hosting_team present?):')
    cur.execute("SELECT id, payload FROM raw_competition ORDER BY id DESC LIMIT 5")
    rows = cur.fetchall()
    for r in rows:
        rid, payload = r
        try:
            if isinstance(payload, str):
                p = json.loads(payload)
            else:
                p = payload
        except Exception:
            p = None
        hosting = None
        if isinstance(p, dict):
            hosting = p.get('hosting_team') or p.get('hostingTeam')
        print(' -', rid, 'hosting_team=', bool(hosting))

    print('\nTop 10 dim_team rows:')
    cur.execute("SELECT * FROM dim_team LIMIT 10")
    drows = cur.fetchall()
    for dr in drows:
        print(' -', dr)

    cur.close(); conn.close()
except Exception:
    traceback.print_exc()
    print('DB check failed')
