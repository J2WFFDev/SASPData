from dotenv import load_dotenv
import os, psycopg2
load_dotenv()
conn=psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=int(os.getenv('PGPORT','5432')), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
cur=conn.cursor()
cur.execute("SELECT to_regclass('public.dim_team')")
print('regclass dim_team=', cur.fetchone())
cur.close(); conn.close()
