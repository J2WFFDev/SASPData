import os, json, psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(host=os.getenv('PGHOST'),port=int(os.getenv('PGPORT') or 5432),dbname=os.getenv('PGDATABASE'),user=os.getenv('PGUSER'),password=os.getenv('PGPASSWORD'))

def main():
    conn = get_conn()
    cur = conn.cursor()
    metrics = {}
    for t in ['dim_athlete','map_roster','fact_stage_series_result','fact_penalty','fact_stage_total']:
        try:
            cur.execute(f'SELECT count(*) FROM {t}')
            metrics[t] = cur.fetchone()[0]
        except Exception as e:
            metrics[t] = str(e)
    # schedule tables
    for t in ['silver_schedule','silver_schedule_slot','silver_slot_flight','silver_slot_lineup']:
        try:
            cur.execute(f'SELECT count(*) FROM {t}')
            metrics[t] = cur.fetchone()[0]
        except Exception as e:
            metrics[t] = str(e)
    cur.close(); conn.close()
    out = os.path.join(os.getcwd(), 'tmp_silver_counts.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)
    print('Wrote', out)

if __name__ == '__main__':
    main()
