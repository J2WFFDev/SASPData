-- Silver schema for Competitions (Postgres)

-- dim_state
CREATE TABLE IF NOT EXISTS dim_state (
  state_id BIGINT PRIMARY KEY,
  region_id BIGINT NULL,
  name TEXT NOT NULL,
  abbr TEXT NOT NULL
);

-- dim_team (hosting_team)
CREATE TABLE IF NOT EXISTS dim_team (
  team_id BIGSERIAL PRIMARY KEY,
  external_id BIGINT NOT NULL UNIQUE,
  type_id BIGINT NULL,
  name TEXT NOT NULL,
  paper_name TEXT NULL,
  paper_email TEXT NULL,
  org TEXT NULL,
  trust_id TEXT NULL,
  external_ref TEXT NULL,
  team_fee_paid_at TIMESTAMPTZ NULL,
  autocomplete_id TEXT NULL,
  state_id BIGINT NULL,
  CONSTRAINT fk_team_state FOREIGN KEY (state_id) REFERENCES dim_state(state_id)
);

-- dim_range
CREATE TABLE IF NOT EXISTS dim_range (
  range_id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  type_id BIGINT NULL,
  contact TEXT NULL,
  paddr TEXT NULL,
  pcity TEXT NULL,
  pstate_id BIGINT NULL,
  pzip TEXT NULL,
  maddr TEXT NULL,
  mcity TEXT NULL,
  mstate_id BIGINT NULL,
  mzip TEXT NULL,
  phone TEXT NULL,
  email TEXT NULL,
  url TEXT NULL,
  CONSTRAINT fk_range_pstate FOREIGN KEY (pstate_id) REFERENCES dim_state(state_id),
  CONSTRAINT fk_range_mstate FOREIGN KEY (mstate_id) REFERENCES dim_state(state_id)
);

-- dim_contact
CREATE TABLE IF NOT EXISTS dim_contact (
  contact_id BIGINT PRIMARY KEY,
  fname TEXT NOT NULL,
  lname TEXT NOT NULL,
  address TEXT NULL,
  city TEXT NULL,
  state_id BIGINT NULL,
  zip TEXT NULL,
  bdate DATE NULL,
  has_login BOOLEAN NULL,
  receive_emails BOOLEAN NULL,
  full_name TEXT NULL,
  CONSTRAINT fk_contact_state FOREIGN KEY (state_id) REFERENCES dim_state(state_id)
);

-- dim_registration_type
CREATE TABLE IF NOT EXISTS dim_registration_type (
  registration_type_id BIGSERIAL PRIMARY KEY,
  external_id BIGINT NOT NULL UNIQUE,
  cat_id BIGINT NULL,
  value TEXT NOT NULL,
  descr TEXT NULL,
  alt TEXT NULL,
  sort_order INT NULL,
  shooting_style TEXT NULL
);

-- dim_classification
CREATE TABLE IF NOT EXISTS dim_classification (
  classification_id BIGSERIAL PRIMARY KEY,
  external_id BIGINT NOT NULL UNIQUE,
  cat_id BIGINT NULL,
  value TEXT NOT NULL,
  descr TEXT NULL,
  alt TEXT NULL,
  sort_order INT NULL,
  shooting_style TEXT NULL
);

-- dim_competition (header)
CREATE TABLE IF NOT EXISTS dim_competition (
  competition_id BIGSERIAL PRIMARY KEY,
  external_id BIGINT NOT NULL UNIQUE,
  mod_id BIGINT NULL,
  name TEXT NOT NULL,
  org TEXT NULL,
  type_code TEXT NULL,
  post_label TEXT NULL,
  post_raw TEXT NULL,
  status_code TEXT NULL,
  shooting_style TEXT NULL,
  start_date DATE NULL,
  end_date DATE NULL,
  open_date DATE NULL,
  close_date DATE NULL,
  open_time_txt TEXT NULL,
  close_time_txt TEXT NULL,
  fee NUMERIC(10,2) NULL,
  coach_flag BOOLEAN NULL,
  note TEXT NULL,
  cache_leaderboards_forever BOOLEAN NULL,
  is_registration_open BOOLEAN NULL,
  can_update BOOLEAN NULL,
  can_delete BOOLEAN NULL,
  is_scorekeeper BOOLEAN NULL,
  can_squad_anyone BOOLEAN NULL,
  is_invite_only BOOLEAN NULL,
  autocomplete_id TEXT NULL,
  create_date TIMESTAMPTZ NULL,
  create_who TEXT NULL,
  update_date TIMESTAMPTZ NULL,
  update_who TEXT NULL,
  registration_type_id BIGINT NULL,
  classification_id BIGINT NULL,
  hosting_team_id BIGINT NULL,
  range_id BIGINT NULL,
  contact_id BIGINT NULL,
  CONSTRAINT fk_comp_registration_type FOREIGN KEY (registration_type_id) REFERENCES dim_registration_type(registration_type_id),
  CONSTRAINT fk_comp_classification FOREIGN KEY (classification_id) REFERENCES dim_classification(classification_id),
  CONSTRAINT fk_comp_team FOREIGN KEY (hosting_team_id) REFERENCES dim_team(team_id),
  CONSTRAINT fk_comp_range FOREIGN KEY (range_id) REFERENCES dim_range(range_id),
  CONSTRAINT fk_comp_contact FOREIGN KEY (contact_id) REFERENCES dim_contact(contact_id)
);

CREATE INDEX IF NOT EXISTS ix_comp_dates ON dim_competition(start_date, end_date);
CREATE INDEX IF NOT EXISTS ix_comp_org_status ON dim_competition(org, status_code);

-- competition_stage (rows from stage_one..stage_four)
CREATE TABLE IF NOT EXISTS competition_stage (
  competition_id BIGINT NOT NULL,
  stage_num SMALLINT NOT NULL,
  stage_name TEXT NULL,
  PRIMARY KEY (competition_id, stage_num),
  CONSTRAINT fk_stage_comp FOREIGN KEY (competition_id) REFERENCES dim_competition(competition_id)
);

-- bridge for invited teams
CREATE TABLE IF NOT EXISTS bridge_competition_invited_team (
  competition_id BIGINT NOT NULL,
  team_id BIGINT NOT NULL,
  PRIMARY KEY (competition_id, team_id),
  CONSTRAINT fk_bridge_comp FOREIGN KEY (competition_id) REFERENCES dim_competition(competition_id),
  CONSTRAINT fk_bridge_team FOREIGN KEY (team_id) REFERENCES dim_team(team_id)
);
