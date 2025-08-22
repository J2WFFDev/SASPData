from dotenv import load_dotenv
import os, psycopg2
load_dotenv()
conn = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=int(os.getenv('PGPORT','5432')), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))
cur = conn.cursor()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='dim_team' ORDER BY ordinal_position")
rows = cur.fetchall()
print(rows)
cur.close()
conn.close()
