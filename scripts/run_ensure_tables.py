from src import ingest

def main():
    conn = ingest.get_db_conn()
    try:
        ingest.ensure_tables(conn)
        print('ensure_tables completed')
    finally:
        conn.close()

if __name__ == '__main__':
    main()
