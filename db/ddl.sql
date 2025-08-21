-- Enable useful extensions (may require superuser)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Raw tables (append-only, versioned)
CREATE TABLE IF NOT EXISTS raw_scoreboard (
  id bigserial PRIMARY KEY,
  match_number int NULL,
  payload jsonb NOT NULL,
  source text NOT NULL,
  source_hash text NOT NULL UNIQUE,
  ingested_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_schedule (
  id bigserial PRIMARY KEY,
  match_number int NULL,
  payload jsonb NOT NULL,
  source text NOT NULL,
  source_hash text NOT NULL UNIQUE,
  ingested_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_teams (
  id bigserial PRIMARY KEY,
  match_number int NULL,
  payload jsonb NOT NULL,
  source text NOT NULL,
  source_hash text NOT NULL UNIQUE,
  ingested_at timestamptz DEFAULT now()
);
-- Enable useful extensions (may require superuser)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Raw tables (append-only, versioned)
CREATE TABLE IF NOT EXISTS raw_scoreboard (
  id bigserial PRIMARY KEY,
  -- Enable useful extensions (may require superuser)
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
  CREATE EXTENSION IF NOT EXISTS pgcrypto;

  -- Raw tables (append-only, versioned)
  CREATE TABLE IF NOT EXISTS raw_scoreboard (
    id bigserial PRIMARY KEY,
    match_number int NULL,
    payload jsonb NOT NULL,
    source text NOT NULL,
    source_hash text NOT NULL UNIQUE,
    ingested_at timestamptz DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS raw_schedule (
    id bigserial PRIMARY KEY,
    match_number int NULL,
    payload jsonb NOT NULL,
    source text NOT NULL,
    source_hash text NOT NULL UNIQUE,
    ingested_at timestamptz DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS raw_teams (
    id bigserial PRIMARY KEY,
    match_number int NULL,
    payload jsonb NOT NULL,
    source text NOT NULL,
    source_hash text NOT NULL UNIQUE,
    ingested_at timestamptz DEFAULT now()
  );
