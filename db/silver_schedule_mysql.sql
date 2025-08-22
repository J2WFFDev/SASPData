-- MySQL 8 InnoDB DDL + JSON_TABLE load statements for Schedule -> Silver
-- Run on MySQL 8+. Assumes bronze_raw_json(raw_id, payload_json) exists.

-- 1) Schedule header
CREATE TABLE IF NOT EXISTS silver_schedule (
  schedule_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_id        BIGINT UNSIGNED NOT NULL,
  external_id   VARCHAR(64)  NULL,
  name          VARCHAR(200) NULL,
  generated_at  VARCHAR(64)  NULL,
  primary_contact VARCHAR(160) NULL,
  host_team     VARCHAR(160) NULL,
  range_contact VARCHAR(160) NULL,
  location_name VARCHAR(160) NULL,
  range_name    VARCHAR(160) NULL,
  location_phone VARCHAR(40) NULL,
  location_email VARCHAR(160) NULL,
  notes         TEXT NULL,
  sub_label     VARCHAR(160) NULL,
  PRIMARY KEY (schedule_id),
  UNIQUE KEY uq_schedule_raw (raw_id)
) ENGINE=InnoDB;

-- 2) Slots
CREATE TABLE IF NOT EXISTS silver_schedule_slot (
  slot_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  schedule_id   BIGINT UNSIGNED NOT NULL,
  slot_idx      INT UNSIGNED NOT NULL,
  rid           INT NULL,
  slot_label    VARCHAR(160) NULL,
  slot_number   INT NULL,
  discipline    VARCHAR(120) NULL,
  stage         VARCHAR(120) NULL,
  expanded      TINYINT(1) NULL,
  is_blocked    TINYINT(1) AS (discipline IS NULL) STORED,
  PRIMARY KEY (slot_id),
  UNIQUE KEY uq_sched_slotidx (schedule_id, slot_idx),
  KEY ix_sched_disc (schedule_id, discipline),
  CONSTRAINT fk_slot_schedule FOREIGN KEY (schedule_id) REFERENCES silver_schedule(schedule_id)
) ENGINE=InnoDB;

-- 3) Flights
CREATE TABLE IF NOT EXISTS silver_slot_flight (
  flight_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  slot_id       BIGINT UNSIGNED NOT NULL,
  flight_idx    INT UNSIGNED NOT NULL,
  rid           INT NULL,
  flight_label  VARCHAR(80) NULL,
  time_label    VARCHAR(40) NULL,
  dt_text       VARCHAR(64) NULL,
  location      VARCHAR(120) NULL,
  PRIMARY KEY (flight_id),
  UNIQUE KEY uq_slot_flightidx (slot_id, flight_idx),
  KEY ix_slot_time (slot_id, dt_text),
  CONSTRAINT fk_flight_slot FOREIGN KEY (slot_id) REFERENCES silver_schedule_slot(slot_id)
) ENGINE=InnoDB;

-- 4) Lineup
CREATE TABLE IF NOT EXISTS silver_slot_lineup (
  lineup_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  slot_id       BIGINT UNSIGNED NOT NULL,
  lineup_idx    INT UNSIGNED NOT NULL,
  station       INT NULL,
  is_open       TINYINT(1) NULL,
  exists_flag   TINYINT(1) NULL,
  athlete_name  VARCHAR(160) NULL,
  team_name     VARCHAR(160) NULL,
  class_label   VARCHAR(120) NULL,
  PRIMARY KEY (lineup_id),
  UNIQUE KEY uq_slot_lineupidx (slot_id, lineup_idx),
  KEY ix_slot_station (slot_id, station),
  CONSTRAINT fk_lineup_slot FOREIGN KEY (slot_id) REFERENCES silver_schedule_slot(slot_id)
) ENGINE=InnoDB;

-- Insert silver_schedule rows from bronze
INSERT INTO silver_schedule (
  raw_id, external_id, name, generated_at,
  primary_contact, host_team, range_contact,
  location_name, range_name, location_phone, location_email,
  notes, sub_label
)
SELECT
  br.raw_id,
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.id')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.name')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.generated')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.primary_contact')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.host_team')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.range_contact')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.location_name')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.range_name')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.location_phone')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.location_email')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.notes')),
  JSON_UNQUOTE(JSON_EXTRACT(br.payload_json, '$.sub_label'))
FROM bronze_raw_json br
WHERE JSON_TYPE(JSON_EXTRACT(br.payload_json, '$.slots')) = 'ARRAY'
  AND NOT EXISTS (SELECT 1 FROM silver_schedule s WHERE s.raw_id = br.raw_id);

-- Insert slots using JSON_TABLE
INSERT INTO silver_schedule_slot (
  schedule_id, slot_idx, rid, slot_label, slot_number,
  discipline, stage, expanded
)
SELECT
  s.schedule_id,
  jt.slot_idx,
  jt.rid,
  jt.slot_label,
  jt.slot_number,
  jt.discipline,
  jt.stage,
  jt.expanded
FROM silver_schedule s
JOIN bronze_raw_json br ON br.raw_id = s.raw_id
JOIN JSON_TABLE(
  br.payload_json,
  '$.slots[*]'
  COLUMNS (
    slot_idx   FOR ORDINALITY,
    rid        INT          PATH '$.rid',
    slot_label VARCHAR(160) PATH '$.name',
    slot_number INT         PATH '$.number',
    discipline VARCHAR(120) PATH '$.discipline',
    stage      VARCHAR(120) PATH '$.stage',
    expanded   TINYINT(1)   PATH '$.expanded'
  )
) AS jt
WHERE NOT EXISTS (
  SELECT 1 FROM silver_schedule_slot x
  WHERE x.schedule_id = s.schedule_id AND x.slot_idx = jt.slot_idx
);

-- Insert flights per slot
INSERT INTO silver_slot_flight (
  slot_id, flight_idx, rid, flight_label, time_label, dt_text, location
)
SELECT
  sl.slot_id,
  f.flight_idx,
  f.rid,
  f.flight_label,
  f.time_label,
  f.dt_text,
  f.location
FROM silver_schedule_slot sl
JOIN silver_schedule s ON s.schedule_id = sl.schedule_id
JOIN bronze_raw_json br ON br.raw_id = s.raw_id
JOIN JSON_TABLE(
  br.payload_json,
  '$.slots[*]'
  COLUMNS (
    slot_idx FOR ORDINALITY,
    NESTED PATH '$.flights[*]'
    COLUMNS (
      flight_idx FOR ORDINALITY,
      rid        INT          PATH '$.rid',
      flight_label VARCHAR(80) PATH '$.flight',
      time_label   VARCHAR(40) PATH '$.time',
      dt_text      VARCHAR(64) PATH '$.date',
      location     VARCHAR(120) PATH '$.location'
    )
  )
) AS f
WHERE f.slot_idx = sl.slot_idx
  AND NOT EXISTS (
    SELECT 1 FROM silver_slot_flight x
    WHERE x.slot_id = sl.slot_id AND x.flight_idx = f.flight_idx
  );

-- Insert lineup per slot
INSERT INTO silver_slot_lineup (
  slot_id, lineup_idx, station, is_open, exists_flag,
  athlete_name, team_name, class_label
)
SELECT
  sl.slot_id,
  l.lineup_idx,
  l.station,
  l.is_open,
  l.exists_flag,
  l.athlete_name,
  l.team_name,
  l.class_label
FROM silver_schedule_slot sl
JOIN silver_schedule s ON s.schedule_id = sl.schedule_id
JOIN bronze_raw_json br ON br.raw_id = s.raw_id
JOIN JSON_TABLE(
  br.payload_json,
  '$.slots[*]'
  COLUMNS (
    slot_idx FOR ORDINALITY,
    NESTED PATH '$.lineup[*]'
    COLUMNS (
      lineup_idx  FOR ORDINALITY,
      station     INT           PATH '$.station',
      is_open     TINYINT(1)    PATH '$.is_open',
      exists_flag TINYINT(1)    PATH '$.exists',
      athlete_name VARCHAR(160) PATH '$.name',
      team_name    VARCHAR(160) PATH '$.team',
      class_label  VARCHAR(120) PATH '$.class'
    )
  )
) AS l
WHERE l.slot_idx = sl.slot_idx
  AND NOT EXISTS (
    SELECT 1 FROM silver_slot_lineup x
    WHERE x.slot_id = sl.slot_id AND x.lineup_idx = l.lineup_idx
  );

-- Rebuild helper: remove silver rows for a raw_id (useful when reprocessing changed bronze rows)
-- DELETE FROM silver_slot_lineup WHERE slot_id IN (SELECT slot_id FROM silver_schedule_slot WHERE schedule_id IN (SELECT schedule_id FROM silver_schedule WHERE raw_id = 123));
-- DELETE FROM silver_slot_flight WHERE slot_id IN (SELECT slot_id FROM silver_schedule_slot WHERE schedule_id IN (SELECT schedule_id FROM silver_schedule WHERE raw_id = 123));
-- DELETE FROM silver_schedule_slot WHERE schedule_id IN (SELECT schedule_id FROM silver_schedule WHERE raw_id = 123);
-- DELETE FROM silver_schedule WHERE raw_id = 123;

-- Notes:
-- * Run the INSERT statements in order: schedule -> slots -> flights -> lineup.
-- * Adjust VARCHAR sizes to taste.
-- * If you need strict datetime parsing, add STR_TO_DATE parsing in the flight insert (dt_text -> datetime).
