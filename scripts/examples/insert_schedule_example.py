import json
import hashlib
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def canonicalize(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def get_conn():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def insert_example(file_path: str):
    p = Path(file_path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    payload_text = canonicalize(raw)
    h = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_schedule (match_number, payload, source, source_hash)
                    VALUES (%s, %s::jsonb, %s, %s)
                    ON CONFLICT (source_hash) DO UPDATE SET ingested_at = raw_schedule.ingested_at
                    RETURNING id
                    """,
                    (raw.get("id"), payload_text, str(p.name), h),
                )
                row = cur.fetchone()
                if row:
                    print(f"Inserted/Found raw_schedule.id={row[0]}")
                    return row[0]
                else:
                    cur.execute("SELECT id FROM raw_schedule WHERE source_hash=%s", (h,))
                    r = cur.fetchone()
                    if r:
                        print(f"Found existing raw_schedule.id={r[0]}")
                        return r[0]
    finally:
        conn.close()


if __name__ == "__main__":
    example = Path(__file__).resolve().parents[1] / "doc" / "examples" / "Schedule 589.json"
    if not example.exists():
        print("Example file not found:", example)
    else:
        insert_example(str(example))
