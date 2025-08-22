from src import ingest
import traceback

try:
    conn = ingest.get_db_conn()
    ingest.ensure_tables(conn)
    print('OK')
    conn.close()
except Exception:
    traceback.print_exc()
