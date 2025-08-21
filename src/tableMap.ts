// Table mapping from raw (bronze) tables to silver targets
// Generated from repository transform scripts and database schema

export type RawTable =
  | 'raw_scoreboard'
  | 'raw_schedule'
  | 'raw_competition';

export type SilverTargetType = 'dim' | 'fact' | 'map' | 'bridge' | 'silver';

export interface TargetInfo {
  table: string; // target table name
  type: SilverTargetType; // dim/fact/map/bridge/silver
  keys?: string[]; // join keys from raw payload to target
  notes?: string; // any transformation notes
}

export interface RawToSilverMapEntry {
  rawTable: RawTable;
  targets: TargetInfo[];
}

export const tableMap: RawToSilverMapEntry[] = [
  {
    rawTable: 'raw_scoreboard',
    targets: [
      { table: 'dim_team', type: 'dim', keys: ['team.id', 'team.name'], notes: 'Upsert teams found in scoreboard payload; uses team external id and name.' },
      { table: 'dim_athlete', type: 'dim', keys: ['athlete.id', 'athlete.name'], notes: 'Upsert athletes referenced in roster and results.' },
      { table: 'dim_discipline', type: 'dim', keys: ['discipline.code'], notes: 'Upsert discipline/event metadata.' },
      { table: 'map_roster', type: 'map', keys: ['match_id', 'team.id', 'athlete.id'], notes: 'Map athletes to roster slots for a match/team.' },
      { table: 'fact_stage_series_result', type: 'fact', keys: ['match_id', 'series_index', 'team.id', 'athlete.id'], notes: 'Insert per-series results; aggregates may be computed later.' },
      { table: 'fact_penalty', type: 'fact', keys: ['match_id', 'team.id', 'penalty.id'], notes: 'Insert penalty records associated with team/match.' },
      { table: 'fact_stage_total', type: 'fact', keys: ['match_id', 'team.id'], notes: 'Per-stage totals for team.' },
      { table: 'silver_scoreboard', type: 'silver', keys: ['source', 'source_hash'], notes: 'Canonicalized scoreboard JSON payload stored for auditing; row inserted by bronze ingest.' }
    ]
  },
  {
    rawTable: 'raw_schedule',
    targets: [
      { table: 'silver_schedule', type: 'silver', keys: ['source', 'source_hash'], notes: 'Canonicalized schedule payload stored to silver layer.' },
      { table: 'silver_schedule_slot', type: 'silver', keys: ['match_id', 'slot_index'], notes: 'Exploded slots (flights/lines) from schedule into slot rows.' },
      { table: 'silver_slot_flight', type: 'silver', keys: ['slot_id', 'flight_index'], notes: 'Per-flight details attached to slots.' },
      { table: 'silver_slot_lineup', type: 'silver', keys: ['slot_id', 'athlete.id'], notes: 'Athlete lineup for slot; links to dim_athlete.' },
      { table: 'dim_team', type: 'dim', keys: ['team.id', 'team.name'], notes: 'Ensure teams referenced in schedule are present in dim_team.' },
      { table: 'dim_range', type: 'dim', keys: ['range.id'], notes: 'Upsert range/location metadata used by schedule.' },
      { table: 'dim_contact', type: 'dim', keys: ['contact.email', 'contact.name'], notes: 'Organizer/contact information.' }
    ]
  },
  {
    rawTable: 'raw_competition',
    targets: [
      { table: 'dim_competition', type: 'dim', keys: ['competition.id', 'competition.name'], notes: 'Primary competition metadata.' },
      { table: 'competition_stage', type: 'dim', keys: ['competition.id', 'stage.index'], notes: 'Stages or phases of the competition.' },
      { table: 'bridge_competition_invited_team', type: 'bridge', keys: ['competition.id', 'team.id'], notes: 'Bridge table for invited teams.' },
      { table: 'dim_registration_type', type: 'dim', keys: ['registration_type.code'], notes: 'Registration type classes.' },
      { table: 'dim_classification', type: 'dim', keys: ['classification.code'], notes: 'Classification metadata.' },
      { table: 'dim_state', type: 'dim', keys: ['state.code'], notes: 'Geographic/state dimension.' },
      { table: 'silver_competition', type: 'silver', keys: ['source', 'source_hash'], notes: 'Canonicalized competition page saved for auditing.' }
    ]
  }
];

export default tableMap;

// Usage note:
// - Import `tableMap` to programmatically inspect which silver/dim/fact/tables
//   are populated from each `raw_*` (bronze) table.
// - Each `TargetInfo.keys` entry is a suggested mapping path inside the
//   canonical JSON or derived columns (e.g., `match_id` derived from URL or
//   payload). Implement transformation logic in your Silver transform scripts.
// - For many-to-many relationships (e.g., teams <-> competitions) use the
//   `bridge` targets; for lists or exploded arrays use `silver_` prefixed tables.

// ASCII-style view helper
export function getAsciiMap(): string {
  return `BRONZE (raw, append-only)
┌──────────────────────────────────────────────────────────────────────────┐
│ raw_* (bronze_raw_json)                                                  │
│ ──────────────────────────────────────────────────────────────────────── │
│ PK: raw_id (BIGINT)                                                       │
│ source VARCHAR(1024)    fetched_at_utc DATETIME(6)                        │
│ http_status INT          source_hash CHAR(64) UNIQUE                      │
│ match_id? VARCHAR(128)   observed_at_utc DATETIME(6)?                     │
│ payload_json JSON (entire canonicalized response)                         │
└──────────────────────────────────────────────────────────────────────────┘
                     │ 1
                     │ (each fetch)
                     ▼
SILVER (query-friendly, normalized)
┌──────────────────────────────────────────────────────────────────────────┐
│ silver_entity                                                             │
│ ──────────────────────────────────────────────────────────────────────── │
│ PK: entity_id BIGINT AUTO_INCREMENT                                       │
│ FK: raw_id BIGINT  → raw_*.raw_id                                          │
│ external_id VARCHAR(128) UNIQUE?  is_latest TINYINT(1)                    │
│ [promoted scalar fields from JSON]                                        │
│   e.g. status_code, name, created_at_utc, updated_at_utc, total_cents     │
└──────────────────────────────────────────────────────────────────────────┘
      │ 1
      │ (one per top-level object in payload)
      ├──────────────┬──────────────┬───────────────────────────────┬───────────────┐
      ▼              ▼              ▼                               ▼               ▼
┌────────────────┐ ┌────────────────────┐ ┌──────────────────────┐ ┌─────────────────────┐ ┌────────────────────────┐
│ silver_entity_ │ │ silver_entity_addr │ │ silver_entity_paymt  │ │ silver_entity_tag   │ │ silver_entity_property │
│ item           │ │ (if JSON has       │ │ (if JSON has         │ │ (if JSON has tags)  │ │ (EAV for rare keys)    │
│────────────────│ │  addresses[])      │ │  payments[])         │ │──────────────────── │ │────────────────────────│
│ PK: item_id    │ │ PK: addr_id        │ │ PK: paymt_id         │ │ PK: entity_id+tag   │ │ PK: entity_id+prop_key │
│ FK: entity_id  │ │ FK: entity_id      │ │ FK: entity_id        │ │ FK: entity_id       │ │ FK: entity_id          │
│ item_idx INT   │ │ addr_idx INT       │ │ paymt_idx INT        │ │ tag VARCHAR(128)    │ │ prop_key VARCHAR(128)  │
│ sku, qty,      │ │ street, city,…     │ │ amount_cents,…       │ │                     │ │ prop_value JSON        │
│ unit_price…    │ │                    │ │                       │ │                     │ │                        │
└────────────────┘ └────────────────────┘ └──────────────────────┘ └─────────────────────┘ └────────────────────────┘

OPTIONAL DIMENSIONS (reference/lookup extracted from JSON or joins to other feeds)
┌──────────────────────┐   ┌───────────────────────┐   ┌────────────────────────┐
│ dim_status_code      │   │ dim_customer          │   │ dim_location           │
│ PK: status_code      │   │ PK: customer_id       │   │ PK: location_id        │
│ status_group,        │   │ name,…                │   │ name, region,…         │
│ is_terminal…         │   └───────────────────────┘   └────────────────────────┘
└──────────────────────┘
   ▲                           ▲
   │                           │
   └────────── FK from silver_entity (status_code, customer_id, location_id, …)
\n
`; 
}

export function printAsciiMap() {
  // Node console-friendly printer
  // eslint-disable-next-line no-console
  console.log(getAsciiMap());
}

// Compact simple mapping printer (user requested simplified view)
export function getSimpleMap(): string {
  return `
raw_schedule
  -> silver_schedule
  -> silver_flight
  -> dim_discipline

raw_scoreboard
  -> dim_stage
  -> athlete_scores

raw_competition
  -> dim_competition
  -> competition_stage
  -> bridge_competition_invited_team

Notes:
- Arrows show source (left) to silver/normalized target (right).
- If a silver table is produced from multiple raw tables, it can be listed
  under each raw source (many-to-one arrows).
`;
}

export function printSimpleMap() {
  // eslint-disable-next-line no-console
  console.log(getSimpleMap());
}
