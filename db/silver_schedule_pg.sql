-- Postgres SQL to create Silver schedule tables and load from bronze_raw_json (jsonb)
-- Run these statements in order: schedule -> slots -> flights -> lineup

-- 1) Schedule header
CREATE TABLE IF NOT EXISTS silver_schedule (
  schedule_id   BIGSERIAL PRIMARY KEY,
  raw_id        BIGINT NOT NULL UNIQUE,
  external_id   TEXT NULL,
  name          TEXT NULL,
  generated_at  TEXT NULL,
  primary_contact TEXT NULL,
  host_team     TEXT NULL,
  range_contact TEXT NULL,
  location_name TEXT NULL,
  range_name    TEXT NULL,
  location_phone TEXT NULL,
  location_email TEXT NULL,
  notes         TEXT NULL,
  sub_label     TEXT NULL
);

-- 2) Slots
CREATE TABLE IF NOT EXISTS silver_schedule_slot (
  slot_id       BIGSERIAL PRIMARY KEY,
  schedule_id   BIGINT NOT NULL REFERENCES silver_schedule(schedule_id) ON DELETE CASCADE,
  slot_idx      INT NOT NULL,
  rid           INT NULL,
  slot_label    TEXT NULL,
  slot_number   INT NULL,
  discipline    TEXT NULL,
  stage         TEXT NULL,
  expanded      BOOLEAN NULL,
  is_blocked    BOOLEAN GENERATED ALWAYS AS (discipline IS NULL) STORED,
  UNIQUE (schedule_id, slot_idx)
);

-- 3) Flights
CREATE TABLE IF NOT EXISTS silver_slot_flight (
  flight_id     BIGSERIAL PRIMARY KEY,
  slot_id       BIGINT NOT NULL REFERENCES silver_schedule_slot(slot_id) ON DELETE CASCADE,
  flight_idx    INT NOT NULL,
  rid           INT NULL,
  flight_label  TEXT NULL,
  time_label    TEXT NULL,
  dt_text       TEXT NULL,
  location      TEXT NULL,
  UNIQUE (slot_id, flight_idx)
);

-- 4) Lineup
CREATE TABLE IF NOT EXISTS silver_slot_lineup (
  lineup_id     BIGSERIAL PRIMARY KEY,
  slot_id       BIGINT NOT NULL REFERENCES silver_schedule_slot(slot_id) ON DELETE CASCADE,
  lineup_idx    INT NOT NULL,
  station       INT NULL,
  is_open       BOOLEAN NULL,
  exists_flag   BOOLEAN NULL,
  athlete_name  TEXT NULL,
  team_name     TEXT NULL,
  class_label   TEXT NULL,
  UNIQUE (slot_id, lineup_idx)
);

-- Insert schedule header from bronze
INSERT INTO silver_schedule (raw_id, external_id, name, generated_at, primary_contact, host_team, range_contact, location_name, range_name, location_phone, location_email, notes, sub_label)
SELECT br.id, br.payload->> 'id', br.payload->> 'name', br.payload->> 'generated', br.payload->> 'primary_contact', br.payload->> 'host_team', br.payload->> 'range_contact', br.payload->> 'location_name', br.payload->> 'range_name', br.payload->> 'location_phone', br.payload->> 'location_email', br.payload->> 'notes', br.payload->> 'sub_label'
FROM raw_schedule br
WHERE jsonb_typeof(br.payload->'slots') = 'array'
  AND NOT EXISTS (SELECT 1 FROM silver_schedule s WHERE s.raw_id = br.id);

-- Insert slots using lateral jsonb_array_elements
INSERT INTO silver_schedule_slot (schedule_id, slot_idx, rid, slot_label, slot_number, discipline, stage, expanded)
SELECT s.schedule_id, js.slot_idx, js.rid, js.slot_label, js.slot_number, js.discipline, js.stage, js.expanded
FROM silver_schedule s
JOIN raw_schedule br ON br.id = s.raw_id
JOIN LATERAL (
  SELECT row_number() OVER () AS slot_idx, (elem->>'rid')::int AS rid, elem->>'name' AS slot_label, (elem->>'number')::int AS slot_number, elem->>'discipline' AS discipline, elem->>'stage' AS stage, (elem->>'expanded')::boolean AS expanded
  FROM jsonb_array_elements(br.payload->'slots') elem
) js ON true
WHERE NOT EXISTS (SELECT 1 FROM silver_schedule_slot x WHERE x.schedule_id = s.schedule_id AND x.slot_idx = js.slot_idx);

-- Insert flights per slot using nested lateral
INSERT INTO silver_slot_flight (slot_id, flight_idx, rid, flight_label, time_label, dt_text, location)
SELECT sl.slot_id, f.flight_idx, (f.rid)::int, f.flight_label, f.time_label, f.dt_text, f.location
FROM silver_schedule_slot sl
JOIN silver_schedule s ON s.schedule_id = sl.schedule_id
JOIN raw_schedule br ON br.id = s.raw_id
JOIN LATERAL (
  SELECT row_number() OVER (PARTITION BY 1) AS slot_ord, slem as slots_elem
  FROM jsonb_array_elements(br.payload->'slots') slem
) slots_data ON ( (slots_data.slots_elem->>'name') = sl.slot_label OR true ) -- best effort join by ordinal below
JOIN LATERAL (
  SELECT row_number() OVER () AS flight_idx, (f->>'rid')::int AS rid, f->>'flight' AS flight_label, f->>'time' AS time_label, f->>'date' AS dt_text, f->>'location' AS location
  FROM jsonb_array_elements(slots_data.slots_elem->'flights') f
) f ON true
WHERE /* match slot ordinal by position using slot_idx */
  (SELECT count(*) FROM jsonb_array_elements(br.payload->'slots') WITH ORDINALITY a(elem,ord) WHERE ord = sl.slot_idx) > 0
  AND NOT EXISTS (SELECT 1 FROM silver_slot_flight x WHERE x.slot_id = sl.slot_id AND x.flight_idx = f.flight_idx);

-- Insert lineup per slot
INSERT INTO silver_slot_lineup (slot_id, lineup_idx, station, is_open, exists_flag, athlete_name, team_name, class_label)
SELECT sl.slot_id, l.lineup_idx, (l.station)::int, l.is_open, l.exists_flag, l.athlete_name, l.team_name, l.class_label
FROM silver_schedule_slot sl
JOIN silver_schedule s ON s.schedule_id = sl.schedule_id
JOIN raw_schedule br ON br.id = s.raw_id
JOIN LATERAL (
  SELECT slem as slots_elem, row_number() OVER () AS slot_ord FROM jsonb_array_elements(br.payload->'slots') slem
) slots_data(elem) ON true
JOIN LATERAL (
  SELECT row_number() OVER () AS lineup_idx, (ln->>'station')::int AS station, (ln->>'is_open')::boolean AS is_open, (ln->>'exists')::boolean AS exists_flag, ln->>'name' AS athlete_name, ln->>'team' AS team_name, ln->>'class' AS class_label
  FROM jsonb_array_elements(slots_data.elem->'lineup') ln
) l ON true
WHERE slots_data.elem is not null
  AND NOT EXISTS (SELECT 1 FROM silver_slot_lineup x WHERE x.slot_id = sl.slot_id AND x.lineup_idx = l.lineup_idx);

-- Rebuild helper (delete silver rows for a raw_id). Replace :raw_id placeholder.
-- DELETE FROM silver_slot_lineup WHERE slot_id IN (SELECT slot_id FROM silver_schedule_slot WHERE schedule_id IN (SELECT schedule_id FROM silver_schedule WHERE raw_id = :raw_id));
-- DELETE FROM silver_slot_flight WHERE slot_id IN (SELECT slot_id FROM silver_schedule_slot WHERE schedule_id IN (SELECT schedule_id FROM silver_schedule WHERE raw_id = :raw_id));
-- DELETE FROM silver_schedule_slot WHERE schedule_id IN (SELECT schedule_id FROM silver_schedule WHERE raw_id = :raw_id);
-- DELETE FROM silver_schedule WHERE raw_id = :raw_id;

-- Notes:
-- * This file targets Postgres jsonb functions. Confirm `bronze_raw_json.payload` is jsonb type and contains schedule documents.
-- * The slot->flight join uses ordinals; if you need exact positional matching, consider expanding the lateral queries to expose WITH ORDINALITY and join on ordinals.
