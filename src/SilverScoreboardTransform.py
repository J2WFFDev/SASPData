import os
import json
import logging
import re
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sasp_silver_transform')

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST','localhost'),
        port=int(os.getenv('PGPORT','5432')),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

def canonicalize_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

def upsert_dim_scoreboard(cur, payload, raw_id):
    external_id = payload.get('id')
    name = payload.get('name')
    generated_at = None
    cur.execute(
        """
        INSERT INTO dim_scoreboard (external_id, name, generated_at, raw_id)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (external_id) DO UPDATE SET name = EXCLUDED.name, raw_id = EXCLUDED.raw_id
        RETURNING scoreboard_id
        """,
        (external_id, name, generated_at, raw_id)
    )
    return cur.fetchone()[0]

def upsert_dim_team(cur, ent_id, team_name):
    cur.execute(
        "INSERT INTO dim_team (ent_id, team_name) VALUES (%s,%s) ON CONFLICT (ent_id) DO UPDATE SET team_name = EXCLUDED.team_name RETURNING team_id",
        (ent_id, team_name)
    )
    return cur.fetchone()[0]

def upsert_dim_discipline(cur, code, label):
    cur.execute(
        "INSERT INTO dim_discipline (code, label) VALUES (%s,%s) ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label RETURNING discipline_id",
        (code, label)
    )
    return cur.fetchone()[0]

def upsert_dim_athlete(cur, ath):
    ath_id = ath.get('ath_id')
    fname = ath.get('fname')
    lname = ath.get('lname')
    cur.execute(
        "INSERT INTO dim_athlete (ath_id, fname, lname) VALUES (%s,%s,%s) ON CONFLICT (ath_id) DO UPDATE SET fname = EXCLUDED.fname, lname = EXCLUDED.lname RETURNING athlete_id",
        (ath_id, fname, lname)
    )
    return cur.fetchone()[0]

def insert_map_roster(cur, scoreboard_id, athlete_id, team_id, discipline_id, athlete):
    slot_id = athlete.get('slot_id')
    class_id = athlete.get('class_id')
    eligible = None
    status = athlete.get('status')
    cur.execute(
        "INSERT INTO map_roster (scoreboard_id, athlete_id, team_id, discipline_id, slot_id, class_id, eligible, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (scoreboard_id, athlete_id, discipline_id) DO UPDATE SET slot_id = EXCLUDED.slot_id, class_id = EXCLUDED.class_id, status = EXCLUDED.status",
        (scoreboard_id, athlete_id, team_id, discipline_id, slot_id, class_id, eligible, status)
    )

def insert_fact_stage_series(cur, scoreboard_id, athlete_id, team_id, discipline_id, stage_num, series_num, value, payload):
    cur.execute(
        "INSERT INTO fact_stage_series_result (scoreboard_id, athlete_id, team_id, discipline_id, stage_num, series_num, value, payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (scoreboard_id, athlete_id, stage_num, series_num) DO UPDATE SET value = EXCLUDED.value, payload = EXCLUDED.payload",
        (scoreboard_id, athlete_id, team_id, discipline_id, stage_num, series_num, value, json.dumps(payload))
    )

def insert_fact_penalty(cur, scoreboard_id, athlete_id, stage_num, series_num, penalty_code, penalty_value):
    cur.execute(
        "INSERT INTO fact_penalty (scoreboard_id, athlete_id, stage_num, series_num, penalty_code, penalty_value) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (scoreboard_id, athlete_id, stage_num, series_num, penalty_code, penalty_value)
    )

def process_raw_row(cur, raw_row):
    raw_id = raw_row['id']
    payload = raw_row['payload']
    scoreboard_id = upsert_dim_scoreboard(cur, payload, raw_id)

    teams = payload.get('teams') or []
    for team in teams:
        ent_id = team.get('ent_id')
        team_name = team.get('name') or ent_id
        team_id = upsert_dim_team(cur, ent_id, team_name)
        disciplines = team.get('disciplines') or []
        for disc in disciplines:
            disc_code = disc.get('name')
            discipline_id = upsert_dim_discipline(cur, disc_code, disc_code)
            athletes = disc.get('athletes') or []
            for ath in athletes:
                athlete_id = upsert_dim_athlete(cur, ath)
                insert_map_roster(cur, scoreboard_id, athlete_id, team_id, discipline_id, ath)

                # pivot sppX_Y keys -> fact_stage_series_result
                for k,v in list(ath.items()):
                    m = re.match(r'^spp(\d+)_(\d+)$', k)
                    if m:
                        stage_num = int(m.group(1))
                        series_num = int(m.group(2))
                        try:
                            value = float(v) if v not in (None, '') else None
                        except Exception:
                            value = None
                        insert_fact_stage_series(cur, scoreboard_id, athlete_id, team_id, discipline_id, stage_num, series_num, value, {k: v})

                # penalties: more specific patterns (pen, penalty, _pen, penX_YY)
                for k,v in list(ath.items()):
                    if not re.search(r'(^pen|_pen|pen_|penalty)', k, re.IGNORECASE):
                        continue
                    # attempt numeric
                    try:
                        pv = float(v)
                    except Exception:
                        continue
                    # map to stage/series if possible: pen, pen1, pen1_2, penalty_1_2
                    m = re.match(r'^(?:pen|penalty)_?(\d+)?_?(\d+)?', k, re.IGNORECASE)
                    stage_num = int(m.group(1)) if m and m.group(1) else 0
                    series_num = int(m.group(2)) if m and m.group(2) else None
                    insert_fact_penalty(cur, scoreboard_id, athlete_id, stage_num, series_num, k, pv)

                # compute stage totals for this athlete/scoreboard (sum series values + penalties)
                # gather distinct stage_nums present for this athlete in the inserted series
                cur.execute(
                    "SELECT DISTINCT stage_num FROM fact_stage_series_result WHERE scoreboard_id=%s AND athlete_id=%s",
                    (scoreboard_id, athlete_id)
                )
                stage_rows = [r[0] for r in cur.fetchall()]
                # ensure we also consider stages that only have penalties (stage_num from fact_penalty)
                cur.execute(
                    "SELECT DISTINCT stage_num FROM fact_penalty WHERE scoreboard_id=%s AND athlete_id=%s",
                    (scoreboard_id, athlete_id)
                )
                pen_stages = [r[0] for r in cur.fetchall()]
                for sn in set(stage_rows + pen_stages):
                    # sum series values (ignore NULLs)
                    cur.execute(
                        "SELECT SUM(value) FROM fact_stage_series_result WHERE scoreboard_id=%s AND athlete_id=%s AND stage_num=%s",
                        (scoreboard_id, athlete_id, sn)
                    )
                    raw_sum = cur.fetchone()[0]
                    # sum penalties for this stage
                    cur.execute(
                        "SELECT SUM(penalty_value) FROM fact_penalty WHERE scoreboard_id=%s AND athlete_id=%s AND stage_num=%s",
                        (scoreboard_id, athlete_id, sn)
                    )
                    pen_sum = cur.fetchone()[0]
                    if raw_sum is None and pen_sum is None:
                        final = None
                    else:
                        rs = raw_sum or 0
                        ps = pen_sum or 0
                        final = rs + ps
                    cur.execute(
                        "INSERT INTO fact_stage_total (scoreboard_id, athlete_id, stage_num, raw_time, penalties, final_time) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (scoreboard_id, athlete_id, stage_num) DO UPDATE SET raw_time = EXCLUDED.raw_time, penalties = EXCLUDED.penalties, final_time = EXCLUDED.final_time",
                        (scoreboard_id, athlete_id, sn, raw_sum, pen_sum, final)
                    )

    return {'raw_id': raw_id, 'scoreboard_id': scoreboard_id}

def run(raw_id=None):
    conn = get_conn()
    out = os.path.join(os.getcwd(), 'tmp_silver_transform_result.json')
    results = []
    try:
        with conn:
            with conn.cursor() as cur:
                if raw_id:
                    cur.execute('SELECT id, payload FROM raw_scoreboard WHERE id = %s', (raw_id,))
                    rows = cur.fetchall()
                else:
                    cur.execute('SELECT id, payload FROM raw_scoreboard ORDER BY id')
                    rows = cur.fetchall()
                for r in rows:
                    raw_row = {'id': r[0], 'payload': r[1]}
                    logger.info('Processing raw_scoreboard id=%s', r[0])
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
    p.add_argument('--raw-id', type=int, help='Process single raw_scoreboard id')
    args = p.parse_args()
    run(args.raw_id)
