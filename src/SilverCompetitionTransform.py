import json
import logging
import hashlib
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('sasp_competition_transform')


def get_conn():
    load_dotenv()
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=os.getenv('PGPORT','5432'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD'))


def upsert_dim_state(cur, state_obj):
    if not state_obj:
        return None
    sid = state_obj.get('id')
    if sid is None:
        return None
    cur.execute("SELECT state_id FROM dim_state WHERE state_id=%s", (sid,))
    if cur.fetchone():
        return sid
    cur.execute("INSERT INTO dim_state (state_id, region_id, name, abbr) VALUES (%s,%s,%s,%s) ON CONFLICT (state_id) DO NOTHING", (sid, state_obj.get('region_id'), state_obj.get('name'), state_obj.get('abbr')))
    return sid


def upsert_dim_team(cur, team_obj):
    if not team_obj:
        return None
    ext = team_obj.get('id')
    if ext is None:
        return None
    # Detect whether dim_team uses 'external_id' or 'ent_id' or neither
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='dim_team'")
    cols = {r[0] for r in cur.fetchall()}
    if 'external_id' in cols:
        cur.execute("SELECT team_id FROM dim_team WHERE external_id=%s", (ext,))
        r = cur.fetchone()
        if r:
            return r[0]
        cur.execute(
            "INSERT INTO dim_team (external_id, name, paper_name, paper_email, org, external_ref, autocomplete_id, state_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING team_id",
            (ext, team_obj.get('name'), team_obj.get('paper_name'), team_obj.get('paper_email'), team_obj.get('org'), team_obj.get('external_id'), team_obj.get('autocomplete_id'), None),
        )
        return cur.fetchone()[0]
    elif 'ent_id' in cols:
        # older scoreboard dim_team uses ent_id/team_name
        cur.execute("SELECT team_id FROM dim_team WHERE ent_id=%s", (str(ext),))
        r = cur.fetchone()
        if r:
            return r[0]
        # attempt to insert with ent_id and team_name column names if available
        insert_cols = []
        insert_vals = []
        insert_cols.extend(['ent_id','team_name'])
        insert_vals.extend([str(ext), team_obj.get('name')])
        # optional columns if exist
        if 'paper_email' in cols:
            insert_cols.append('paper_email'); insert_vals.append(team_obj.get('paper_email'))
        if 'org' in cols:
            insert_cols.append('org'); insert_vals.append(team_obj.get('org'))
        cols_sql = ','.join(insert_cols)
        vals_sql = ','.join(['%s']*len(insert_vals))
        cur.execute(f"INSERT INTO dim_team ({cols_sql}) VALUES ({vals_sql}) RETURNING team_id", tuple(insert_vals))
        return cur.fetchone()[0]
    else:
        # fallback: match by name
        cur.execute("SELECT team_id FROM dim_team WHERE team_name=%s LIMIT 1", (team_obj.get('name'),))
        r = cur.fetchone()
        if r:
            return r[0]
        cur.execute("INSERT INTO dim_team (team_name) VALUES (%s) RETURNING team_id", (team_obj.get('name'),))
        return cur.fetchone()[0]


def upsert_dim_range(cur, range_obj):
    if not range_obj:
        return None
    rid = range_obj.get('id')
    if rid is None:
        return None
    cur.execute("SELECT range_id FROM dim_range WHERE range_id=%s", (rid,))
    if cur.fetchone():
        return rid
    cur.execute("INSERT INTO dim_range (range_id, name, phone, email, url) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (range_id) DO NOTHING",
                (rid, range_obj.get('name'), range_obj.get('phone'), range_obj.get('email'), range_obj.get('url')))
    return rid


def upsert_dim_contact(cur, contact_obj):
    if not contact_obj:
        return None
    cid = contact_obj.get('id')
    if cid is None:
        return None
    cur.execute("SELECT contact_id FROM dim_contact WHERE contact_id=%s", (cid,))
    if cur.fetchone():
        return cid
    cur.execute("INSERT INTO dim_contact (contact_id, fname, lname, full_name) VALUES (%s,%s,%s,%s) ON CONFLICT (contact_id) DO NOTHING",
                (cid, contact_obj.get('fname'), contact_obj.get('lname'), contact_obj.get('full_name')))
    return cid


def upsert_registration_type(cur, rt):
    if not rt:
        return None
    ext = rt.get('id')
    if ext is None:
        return None
    cur.execute("SELECT registration_type_id FROM dim_registration_type WHERE external_id=%s", (ext,))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("INSERT INTO dim_registration_type (external_id, cat_id, value, descr) VALUES (%s,%s,%s,%s) RETURNING registration_type_id", (ext, rt.get('cat_id'), rt.get('value'), rt.get('descr')))
    return cur.fetchone()[0]


def upsert_classification(cur, cl):
    if not cl:
        return None
    ext = cl.get('id')
    if ext is None:
        return None
    cur.execute("SELECT classification_id FROM dim_classification WHERE external_id=%s", (ext,))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("INSERT INTO dim_classification (external_id, cat_id, value, descr) VALUES (%s,%s,%s,%s) RETURNING classification_id", (ext, cl.get('cat_id'), cl.get('value'), cl.get('descr')))
    return cur.fetchone()[0]


def upsert_competition(cur, payload, raw_id, registration_type_id=None, classification_id=None, hosting_team_id=None, range_id=None, contact_id=None):
    # derive external id with fallbacks
    ext = payload.get('id') or payload.get('external_id') or raw_id
    cur.execute("SELECT competition_id FROM dim_competition WHERE external_id=%s", (ext,))
    r = cur.fetchone()
    if r:
        return r[0]

    # map fields with fallbacks
    name = payload.get('name') or payload.get('title')
    org = payload.get('org')
    type_code = payload.get('type')
    post_label = payload.get('post') or payload.get('post_label')
    status_code = payload.get('status')
    start_date = payload.get('start_date')
    end_date = payload.get('end_date')

    # insert header
    cur.execute(
        """
        INSERT INTO dim_competition (external_id, name, org, type_code, post_label, status_code, start_date, end_date, registration_type_id, classification_id, hosting_team_id, range_id, contact_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING competition_id
        """,
        (
            ext,
            name,
            org,
            type_code,
            post_label,
            status_code,
            start_date,
            end_date,
            registration_type_id,
            classification_id,
            hosting_team_id,
            range_id,
            contact_id,
        ),
    )
    comp_id = cur.fetchone()[0]

    # stages - support keys like 'stage_one', 'stage_two', etc.
    stage_keys = {1: 'stage_one', 2: 'stage_two', 3: 'stage_three', 4: 'stage_four'}
    for i in range(1, 5):
        key = stage_keys.get(i)
        val = payload.get(key) or payload.get(f'stage_{i}') or payload.get(f'stage_{i}_name')
        if val:
            cur.execute("INSERT INTO competition_stage (competition_id, stage_num, stage_name) VALUES (%s,%s,%s) ON CONFLICT (competition_id, stage_num) DO UPDATE SET stage_name=EXCLUDED.stage_name", (comp_id, i, val))

    return comp_id


def process_raw_row(cur, raw_row):
    payload = raw_row['payload'] if isinstance(raw_row['payload'], dict) else json.loads(raw_row['payload'])
    raw_id = raw_row['id']

    results = []

    # input file may contain a top-level 'data' array with multiple competitions
    items = []
    if isinstance(payload, dict) and 'data' in payload and isinstance(payload['data'], list):
        items = payload['data']
    elif isinstance(payload, list):
        items = payload
    else:
        items = [payload]

    for item in items:
        # upsert related dims
        rt = item.get('registration_type')
        reg_id = upsert_registration_type(cur, rt)
        cl = item.get('classification')
        cl_id = upsert_classification(cur, cl)
        team = item.get('hosting_team')
        team_id = upsert_dim_team(cur, team)
        rng = item.get('range')
        range_id = upsert_dim_range(cur, rng)
        contact = item.get('contact') or item.get('primary_contact')
        contact_id = None
        if isinstance(contact, dict):
            contact_id = upsert_dim_contact(cur, contact)

        comp_id = upsert_competition(cur, item, raw_id, registration_type_id=reg_id, classification_id=cl_id, hosting_team_id=team_id, range_id=range_id, contact_id=contact_id)

        # invited teams bridge
        invited = item.get('users_invited_teams') or []
        for t in invited:
            tid = upsert_dim_team(cur, t)
            if tid:
                cur.execute("INSERT INTO bridge_competition_invited_team (competition_id, team_id) VALUES (%s,%s) ON CONFLICT (competition_id, team_id) DO NOTHING", (comp_id, tid))

        results.append(comp_id)

    return results


def run(raw_id=None):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if raw_id:
                    cur.execute("SELECT id, payload FROM raw_competition WHERE id=%s", (raw_id,))
                    rows = cur.fetchall()
                else:
                    cur.execute("SELECT id, payload FROM raw_competition")
                    rows = cur.fetchall()
                results = []
                for r in rows:
                    raw_row = {'id': r[0], 'payload': r[1]}
                    log.info(f"Processing raw_competition id={r[0]}")
                    comp_ids = process_raw_row(cur, raw_row)
                    # comp_ids may be a list of ids (one per competition in the raw payload)
                    if isinstance(comp_ids, list):
                        for cid in comp_ids:
                            results.append({'raw_id': r[0], 'competition_id': cid})
                    else:
                        results.append({'raw_id': r[0], 'competition_id': comp_ids})
                Path('tmp_silver_competition_transform_result.json').write_text(json.dumps(results, default=str, indent=2))
                log.info('Wrote tmp_silver_competition_transform_result.json')
    finally:
        conn.close()


if __name__ == '__main__':
    run()
