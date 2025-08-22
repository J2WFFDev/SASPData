import os
import json
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sasp_schedule_transform')

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST','localhost'),
        port=int(os.getenv('PGPORT','5432')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

def upsert_schedule(cur, raw_row):
    payload = raw_row['payload']
    cur.execute(
        """
        INSERT INTO silver_schedule (raw_id, external_id, name, generated_at, primary_contact, host_team, range_contact, location_name, range_name, location_phone, location_email, notes, sub_label)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (raw_id) DO UPDATE SET external_id = EXCLUDED.external_id, name = EXCLUDED.name, generated_at = EXCLUDED.generated_at, primary_contact = EXCLUDED.primary_contact, host_team = EXCLUDED.host_team, range_contact = EXCLUDED.range_contact, location_name = EXCLUDED.location_name, range_name = EXCLUDED.range_name, location_phone = EXCLUDED.location_phone, location_email = EXCLUDED.location_email, notes = EXCLUDED.notes, sub_label = EXCLUDED.sub_label
        RETURNING schedule_id
        """,
        (
            raw_row['id'], payload.get('id'), payload.get('name'), payload.get('generated'), payload.get('primary_contact'), payload.get('host_team'), payload.get('range_contact'), payload.get('location_name'), payload.get('range_name'), payload.get('location_phone'), payload.get('location_email'), payload.get('notes'), payload.get('sub_label')
        )
    )
    return cur.fetchone()[0]

def upsert_slot(cur, schedule_id, slot_idx, elem):
    rid = elem.get('rid')
    slot_label = elem.get('name')
    slot_number = elem.get('number')
    discipline = elem.get('discipline')
    stage = elem.get('stage')
    expanded = elem.get('expanded')
    cur.execute(
        """
        INSERT INTO silver_schedule_slot (schedule_id, slot_idx, rid, slot_label, slot_number, discipline, stage, expanded)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (schedule_id, slot_idx) DO UPDATE SET rid = EXCLUDED.rid, slot_label = EXCLUDED.slot_label, slot_number = EXCLUDED.slot_number, discipline = EXCLUDED.discipline, stage = EXCLUDED.stage, expanded = EXCLUDED.expanded
        RETURNING slot_id
        """,
        (schedule_id, slot_idx, rid, slot_label, slot_number, discipline, stage, expanded)
    )
    return cur.fetchone()[0]

def upsert_flight(cur, slot_id, flight_idx, fobj):
    rid = fobj.get('rid')
    flight_label = fobj.get('flight')
    time_label = fobj.get('time')
    dt_text = fobj.get('date')
    location = fobj.get('location')
    cur.execute(
        """
        INSERT INTO silver_slot_flight (slot_id, flight_idx, rid, flight_label, time_label, dt_text, location)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (slot_id, flight_idx) DO UPDATE SET rid = EXCLUDED.rid, flight_label = EXCLUDED.flight_label, time_label = EXCLUDED.time_label, dt_text = EXCLUDED.dt_text, location = EXCLUDED.location
        RETURNING flight_id
        """,
        (slot_id, flight_idx, rid, flight_label, time_label, dt_text, location)
    )
    return cur.fetchone()[0]

def upsert_lineup(cur, slot_id, lineup_idx, ln):
    station = ln.get('station')
    is_open = ln.get('is_open')
    exists_flag = ln.get('exists')
    athlete_name = ln.get('name')
    team_name = ln.get('team')
    class_label = ln.get('class')
    cur.execute(
        """
        INSERT INTO silver_slot_lineup (slot_id, lineup_idx, station, is_open, exists_flag, athlete_name, team_name, class_label)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (slot_id, lineup_idx) DO UPDATE SET station = EXCLUDED.station, is_open = EXCLUDED.is_open, exists_flag = EXCLUDED.exists_flag, athlete_name = EXCLUDED.athlete_name, team_name = EXCLUDED.team_name, class_label = EXCLUDED.class_label
        RETURNING lineup_id
        """,
        (slot_id, lineup_idx, station, is_open, exists_flag, athlete_name, team_name, class_label)
    )
    return cur.fetchone()[0]

def process_raw_row(cur, raw_row):
    payload = raw_row['payload']
    schedule_id = upsert_schedule(cur, raw_row)
    slots = payload.get('slots') or []
    for i, s in enumerate(slots, start=1):
        slot_id = upsert_slot(cur, schedule_id, i, s)
        # flights
        flights = s.get('flights') or []
        for j, f in enumerate(flights, start=1):
            upsert_flight(cur, slot_id, j, f)
        # lineup
        lineup = s.get('lineup') or []
        for k, ln in enumerate(lineup, start=1):
            upsert_lineup(cur, slot_id, k, ln)
    return {'raw_id': raw_row['id'], 'schedule_id': schedule_id}

def run(raw_id=None):
    conn = get_conn()
    out = os.path.join(os.getcwd(), 'tmp_silver_schedule_transform_result.json')
    results = []
    try:
        with conn:
            with conn.cursor() as cur:
                if raw_id:
                    cur.execute('SELECT id, payload FROM raw_schedule WHERE id = %s', (raw_id,))
                else:
                    cur.execute('SELECT id, payload FROM raw_schedule ORDER BY id')
                rows = cur.fetchall()
                for r in rows:
                    raw_row = {'id': r[0], 'payload': r[1]}
                    logger.info('Processing raw_schedule id=%s', r[0])
                    res = process_raw_row(cur, raw_row)
                    results.append(res)
    finally:
        conn.close()
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        logger.info('Wrote %s', out)

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--raw-id', type=int, help='Process a single raw_schedule id')
    args = p.parse_args()
    run(args.raw_id)
import os
import json
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sasp_schedule_transform')

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST','localhost'),
        port=int(os.getenv('PGPORT','5432')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

def run(raw_id=None):
    # scaffold: will implement schedule normalization next
    logger.info('Schedule transform scaffold invoked. Implement normalization logic in this file.')

if __name__ == '__main__':
    run()
