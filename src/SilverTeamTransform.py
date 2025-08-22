import os
import json
import logging
import hashlib
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import Json

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sasp_silver_team')


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


def compute_hash(obj):
    return hashlib.sha256(canonicalize_json(obj).encode('utf-8')).hexdigest()


def upsert_dim_team(cur, source, source_ent_id, team_name, country=None, city=None, state=None, metadata=None):
    # Try modern schema first (source, source_ent_id). If dim_team uses legacy schema (ent_id), fall back.
    try:
        cur.execute(
            """
            INSERT INTO dim_team (source, source_ent_id, team_name, country, city, state, metadata, first_seen, last_seen, active)
            VALUES (%s,%s,%s,%s,%s,%s,%s,now(),now(),true)
            ON CONFLICT (source, source_ent_id) DO UPDATE SET
              team_name = EXCLUDED.team_name,
              country = COALESCE(EXCLUDED.country, dim_team.country),
              city = COALESCE(EXCLUDED.city, dim_team.city),
              state = COALESCE(EXCLUDED.state, dim_team.state),
              metadata = COALESCE(EXCLUDED.metadata, dim_team.metadata),
              last_seen = now(),
              active = true
            RETURNING team_id
            """,
            (source, source_ent_id, team_name, country, city, state, Json(metadata) if metadata is not None else None)
        )
        return cur.fetchone()[0]
    except Exception as e:
        # If the failure is due to missing columns (legacy schema), rollback and try legacy path
        try:
            cur.connection.rollback()
        except Exception:
            pass
        # legacy schema uses ent_id and team_name
        cur.execute(
            """
            INSERT INTO dim_team (ent_id, team_name)
            VALUES (%s,%s)
            ON CONFLICT (ent_id) DO UPDATE SET team_name = EXCLUDED.team_name
            RETURNING team_id
            """,
            (source_ent_id, team_name)
        )
        return cur.fetchone()[0]


def insert_snapshot(cur, team_id, raw_id, payload):
    phash = compute_hash(payload)
    cur.execute(
        "INSERT INTO fact_team_snapshot (team_id, raw_id, payload, payload_hash) VALUES (%s,%s,%s,%s) RETURNING snapshot_id",
        (team_id, raw_id, Json(payload), phash)
    )
    return cur.fetchone()[0]


def process_raw_row(cur, raw_row):
    raw_id = raw_row['id']
    payload = raw_row['payload']
    # source and source_ent_id
    # payload may include 'id' or 'ent_id' or similar; fallback to raw_id
    source = payload.get('source') or 'sssfonline'
    source_ent_id = str(payload.get('id') or payload.get('ent_id') or payload.get('team_id') or extract_id_from_payload(payload) or raw_id)
    team_name = payload.get('name') or payload.get('teamName') or payload.get('displayName') or source_ent_id
    # coerce potential dict values into strings to ensure psycopg2 can adapt them
    if isinstance(team_name, (dict, list)):
        try:
            team_name = json.dumps(team_name, ensure_ascii=False)
        except Exception:
            team_name = str(team_name)
    country = payload.get('country') or payload.get('nation')
    if isinstance(country, (dict, list)):
        country = str(country)
    city = payload.get('city')
    if isinstance(city, (dict, list)):
        city = str(city)
    state = payload.get('state')
    if isinstance(state, (dict, list)):
        state = str(state)
    # build metadata and ensure it's a dict (or None)
    raw_meta = payload.get('meta') or payload.get('metadata')
    if raw_meta is None:
        metadata = {k: v for k, v in payload.items() if k not in ('id', 'ent_id', 'team_id', 'name', 'teamName', 'displayName', 'country', 'nation', 'city', 'state')}
    else:
        metadata = raw_meta if isinstance(raw_meta, dict) else {'value': raw_meta}

    team_id = upsert_dim_team(cur, source, source_ent_id, team_name, country, city, state, metadata)
    snapshot_id = insert_snapshot(cur, team_id, raw_id, payload)

    # Optional: process roster members if present
    roster = payload.get('members') or payload.get('roster') or payload.get('athletes') or []
    if roster:
        for member in roster:
            # attempt to upsert person minimal representation; skip complex mapping for now
            src = source
            pid = member.get('id') or member.get('ent_id') or member.get('person_id')
            if not pid:
                continue
            # upsert person
            cur.execute(
                "INSERT INTO dim_person (source, source_ent_id, given_name, family_name, full_name, country, metadata) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source, source_ent_id) DO UPDATE SET given_name = EXCLUDED.given_name, family_name = EXCLUDED.family_name, full_name = EXCLUDED.full_name RETURNING person_id",
                (src, str(pid), member.get('given_name') or member.get('first'), member.get('family_name') or member.get('last'), member.get('full_name') or member.get('name'), member.get('country'), Json(member))
            )
            person_id = cur.fetchone()[0]
            # insert mapping; use role if present
            role = member.get('role') or member.get('type') or 'player'
            try:
                cur.execute(
                    "INSERT INTO map_team_members (team_id, person_id, role, source_raw_id) VALUES (%s,%s,%s,%s) ON CONFLICT (team_id, person_id, role) DO NOTHING",
                    (team_id, person_id, role, raw_id)
                )
            except Exception:
                logger.exception('Failed to insert map_team_members for raw_id=%s', raw_id)

    return {'raw_id': raw_id, 'team_id': team_id, 'snapshot_id': snapshot_id}


def extract_id_from_payload(payload):
    # fallback heuristics
    for key in ('id', 'ent_id', 'team_id'):
        v = payload.get(key)
        if v:
            return v
    return None


def run(raw_id=None):
    conn = get_conn()
    out = os.path.join(os.getcwd(), 'tmp_silver_team_transform_result.json')
    results = []
    try:
        with conn:
            with conn.cursor() as cur:
                if raw_id:
                    cur.execute('SELECT id, payload FROM raw_teams WHERE id = %s', (raw_id,))
                    rows = cur.fetchall()
                else:
                    cur.execute('SELECT id, payload FROM raw_teams ORDER BY id')
                    rows = cur.fetchall()
                for r in rows:
                    raw_row = {'id': r[0], 'payload': r[1]}
                    logger.info('Processing raw_teams id=%s', r[0])
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
    p.add_argument('--raw-id', type=int, help='Process single raw_teams id')
    args = p.parse_args()
    run(args.raw_id)
