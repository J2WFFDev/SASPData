import os
import json
import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sasp_silver')

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST','localhost'),
        port=int(os.getenv('PGPORT','5432')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

def apply_ddl(conn, ddl_path):
    with open(ddl_path, 'r', encoding='utf-8') as f:
        ddl = f.read()
    # split by semicolon like ingest.ensure_tables
    statements = []
    cur_lines = []
    for line in ddl.splitlines():
        if line.strip().startswith('--'):
            continue
        cur_lines.append(line)
        if ';' in line:
            stmt = '\n'.join(cur_lines).strip()
            while stmt.endswith(';'):
                stmt = stmt[:-1].rstrip()
            if stmt:
                statements.append(stmt)
            cur_lines = []
    if cur_lines:
        stmt = '\n'.join(cur_lines).strip()
        if stmt:
            statements.append(stmt)

    results = []
    with conn.cursor() as cur:
        for s in statements:
            try:
                cur.execute(s)
                results.append({'ok': True, 'stmt': s.split('\n',1)[0]})
            except Exception as e:
                logger.warning('DDL statement failed: %s -> %s', s.split('\n',1)[0], e)
                results.append({'ok': False, 'stmt': s.split('\n',1)[0], 'error': str(e)})
    conn.commit()
    return results

def main():
    ddl = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'silver_scoreboard.sql')
    out = os.path.join(os.getcwd(), 'tmp_silver_result.json')
    try:
        conn = get_conn()
    except Exception as e:
        print('ERROR: could not connect to DB', e)
        with open(out, 'w', encoding='utf-8') as f:
            json.dump({'ERROR': str(e)}, f)
        return

    results = apply_ddl(conn, ddl)
    conn.close()
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    print('Wrote', out)

if __name__ == '__main__':
    main()
