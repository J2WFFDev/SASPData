from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('PGHOST','localhost'),
    port=int(os.getenv('PGPORT','5432')),
    dbname=os.getenv('PGDATABASE'),
    user=os.getenv('PGUSER'),
    password=os.getenv('PGPASSWORD')
)
cur = conn.cursor()
try:
    stmts = [
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS source TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS source_ent_id TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS display_name TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS short_name TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS country TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS city TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS state TEXT;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS metadata JSONB;",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ DEFAULT now();",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ DEFAULT now();",
        "ALTER TABLE dim_team ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT true;",
        # Create snapshot table if missing
        "CREATE TABLE IF NOT EXISTS fact_team_snapshot (\n  snapshot_id BIGSERIAL PRIMARY KEY,\n  team_id BIGINT REFERENCES dim_team(team_id) ON DELETE SET NULL,\n  raw_id BIGINT REFERENCES raw_teams(id) ON DELETE SET NULL,\n  snapshot_at TIMESTAMPTZ DEFAULT now(),\n  payload JSONB,\n  payload_hash TEXT\n);",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_team ON fact_team_snapshot(team_id);",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_raw ON fact_team_snapshot(raw_id);",
        # Create dim_person and map_team_members if missing
        "CREATE TABLE IF NOT EXISTS dim_person (\n  person_id BIGSERIAL PRIMARY KEY,\n  source TEXT NOT NULL,\n  source_ent_id TEXT NOT NULL,\n  given_name TEXT,\n  family_name TEXT,\n  full_name TEXT,\n  dob DATE,\n  country TEXT,\n  metadata JSONB,\n  UNIQUE (source, source_ent_id)\n);",
        "CREATE TABLE IF NOT EXISTS map_team_members (\n  team_id BIGINT REFERENCES dim_team(team_id) ON DELETE CASCADE,\n  person_id BIGINT REFERENCES dim_person(person_id) ON DELETE CASCADE,\n  role TEXT,\n  source_raw_id BIGINT,\n  added_at TIMESTAMPTZ DEFAULT now(),\n  PRIMARY KEY (team_id, person_id, role)\n);",
    ]
    for s in stmts:
        try:
            cur.execute(s)
            print('OK', s.split('\n',1)[0])
        except Exception as e:
            print('ERR', e)
            conn.rollback()
    # populate source_ent_id from ent_id where possible
    try:
        cur.execute("UPDATE dim_team SET source_ent_id = ent_id WHERE source_ent_id IS NULL AND ent_id IS NOT NULL;")
        cur.execute("UPDATE dim_team SET source = 'legacy' WHERE source IS NULL;")
        conn.commit()
        print('Populated legacy source/source_ent_id')
    except Exception as e:
        print('ERR population', e)
        conn.rollback()
finally:
    cur.close()
    conn.close()
