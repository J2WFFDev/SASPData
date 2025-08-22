from dotenv import load_dotenv
import os, psycopg2
load_dotenv()
conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=int(os.getenv('PGPORT','5432')), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
cur = conn.cursor()
print('--- dim_team columns ---')
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='dim_team' ORDER BY ordinal_position")
print([r[0] for r in cur.fetchall()])

print('\n--- dim_team count + sample rows ---')
cur.execute("SELECT COUNT(*) FROM dim_team")
print('count:', cur.fetchone()[0])
cur.execute("SELECT team_id, ent_id, source, source_ent_id, team_name, metadata FROM dim_team ORDER BY team_id DESC LIMIT 10")
for r in cur.fetchall():
    print(r)

print('\n--- fact_team_snapshot count + recent ---')
cur.execute("SELECT COUNT(*) FROM fact_team_snapshot")
print('count:', cur.fetchone()[0])
cur.execute("SELECT snapshot_id, team_id, raw_id, snapshot_at FROM fact_team_snapshot ORDER BY snapshot_at DESC LIMIT 10")
for r in cur.fetchall():
    print(r)

cur.close(); conn.close()
