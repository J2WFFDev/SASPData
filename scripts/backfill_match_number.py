import psycopg2
import os
import re
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('PGHOST','localhost'),
    port=int(os.getenv('PGPORT','5432')),
    dbname=os.getenv('PGDATABASE','saspdata'),
    user=os.getenv('PGUSER','postgres'),
    password=os.getenv('PGPASSWORD',''),
)
conn.autocommit = True
pat = re.compile(r"/(\d+)(?:/?$)")

with conn.cursor() as cur:
    for tbl in ('raw_scoreboard','raw_schedule'):
        # `source` column stores the original URL/source identifier
        cur.execute(f"SELECT id, source FROM {tbl} WHERE match_number IS NULL")
        rows = cur.fetchall()
        updated = 0
        for r in rows:
            id, src = r
            m = pat.search(src or '')
            if m:
                mid = int(m.group(1))
                cur.execute(f"UPDATE {tbl} SET match_number=%s WHERE id=%s", (mid, id))
                updated += 1
        print(f"{tbl} backfilled {updated} rows")

conn.close()
