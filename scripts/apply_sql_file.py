import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=int(os.getenv('PGPORT') or 5432),dbname=os.getenv('PGDATABASE'),user=os.getenv('PGUSER'),password=os.getenv('PGPASSWORD'))

def apply_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    # naive split on semicolons, preserve statements
    stmts = []
    cur_lines = []
    for line in sql.splitlines():
        if line.strip().startswith('--'):
            continue
        cur_lines.append(line)
        if ';' in line:
            stmt = '\n'.join(cur_lines).strip()
            while stmt.endswith(';'):
                stmt = stmt[:-1].rstrip()
            if stmt:
                stmts.append(stmt)
            cur_lines = []
    if cur_lines:
        stmt = '\n'.join(cur_lines).strip()
        if stmt:
            stmts.append(stmt)

    conn = get_conn()
    results = []
    try:
        with conn:
            with conn.cursor() as cur:
                for s in stmts:
                    try:
                        cur.execute(s)
                        results.append((True, s.split('\n',1)[0]))
                    except Exception as e:
                        results.append((False, str(e)))
    finally:
        conn.close()
    return results

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: apply_sql_file.py path/to/file.sql')
        sys.exit(2)
    res = apply_file(sys.argv[1])
    for ok, info in res:
        print(('OK' if ok else 'ERR'), info)
