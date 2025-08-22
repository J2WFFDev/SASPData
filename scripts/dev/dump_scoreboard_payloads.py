import os
import json
import psycopg2

from dotenv import load_dotenv

root_env = os.path.join(os.getcwd(), '.env')
if os.path.exists(root_env):
    load_dotenv(root_env)

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST','localhost'),
        port=int(os.getenv('PGPORT','5432')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

def main(limit=10):
    cx = get_conn()
    cur = cx.cursor()
    cur.execute("SELECT id, match_number, payload, source, ingested_at FROM raw_scoreboard ORDER BY ingested_at DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({
            'id': r[0],
            'match_number': r[1],
            'payload': r[2],
            'source': r[3],
            'ingested_at': r[4].isoformat() if r[4] is not None else None
        })
    cur.close()
    cx.close()
    target = os.path.join(os.getcwd(), 'tmp_scoreboard_payloads.json')
    with open(target, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print('Wrote', target)

if __name__ == '__main__':
    main()
