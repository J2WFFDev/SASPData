import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load .env from repo root if present so scripts work when run directly
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

def main():
    out = []
    try:
        cx = get_conn()
    except Exception as e:
        with open('tmp_db_schema.json', 'w', encoding='utf-8') as f:
            json.dump({'ERROR': str(e)}, f)
        print('ERROR: could not connect to DB:', e)
        return

    try:
        cur = cx.cursor()
        # list user tables in public schema
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type='BASE TABLE'
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]

        for t in tables:
            cur.execute(
                "SELECT column_name, data_type, is_nullable, ordinal_position, column_default FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
                (t,)
            )
            cols = [{'column': r[0], 'type': r[1], 'nullable': r[2], 'ordinal_position': r[3], 'column_default': r[4]} for r in cur.fetchall()]

            # get count safely
            try:
                cur.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier(t)))
                count = cur.fetchone()[0]
            except Exception:
                count = None

            out.append({'table': t, 'columns': cols, 'count': count})

        cur.close()
    finally:
        cx.close()

    target = os.path.join(os.getcwd(), 'tmp_db_schema.json')
    with open(target, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print('Wrote', target)

if __name__ == '__main__':
    main()
