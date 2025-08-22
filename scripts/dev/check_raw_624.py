import os
import psycopg2

cx = psycopg2.connect(
    host=os.getenv('PGHOST','localhost'),
    port=int(os.getenv('PGPORT','5432')),
    dbname=os.getenv('PGDATABASE','saspdata'),
    user=os.getenv('PGUSER','postgres'),
    password=os.getenv('PGPASSWORD','')
)
cur = cx.cursor()
cur.execute("SELECT id, match_number, source, ingested_at FROM raw_scoreboard WHERE source LIKE %s ORDER BY ingested_at DESC LIMIT 5;", ('%sasp-scoreboard/624%',))
rows = cur.fetchall()
print('rows returned:', len(rows))
for r in rows:
    print(r)
cur.close()
cx.close()
