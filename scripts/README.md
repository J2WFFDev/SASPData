# Scripts Folder Organization

This document defines the organizational structure for the `scripts/` folder to maintain clarity and logical separation of different types of scripts.

## 📁 Folder Structure

### `scripts/bronze/` - Data Ingestion Scripts
**Purpose**: Scripts that ingest raw data from external APIs into bronze (raw) tables
**Target Tables**: `raw_*` tables (raw_teams, raw_scoreboard, raw_competition, raw_schedule)
**Characteristics**:
- HTTP API calls to external services
- Data validation and error handling
- Rate limiting and retry logic
- Concurrent/threaded processing for performance
- Insert into `raw_*` tables with source tracking

**Examples**:
- Team ingestion from `/api/teams/{id}`
- Competition discovery from `/api/shot/SASP/competitions`
- Scoreboard fetching from `/api/shot/sasp-scoreboard/{id}`
- Schedule data ingestion

### `scripts/silver/` - Bronze-to-Silver Transformation Scripts
**Purpose**: Scripts that transform bronze data into silver dimensional models
**Target Tables**: `dim_*` and `fact_*` tables
**Characteristics**:
- ETL processes that read from `raw_*` tables
- Data cleansing, normalization, and standardization
- Dimensional modeling transformations
- Surrogate key generation
- Business rule application
- Data quality validation

**Examples**:
- `transform_dimensions.py` - Create dimension tables from raw JSON
- `transform_facts.py` - Create fact tables with proper FK relationships
- `etl_stage_performance.py` - Aggregate string→stage with "drop slowest" logic
- Performance aggregation pipelines (match/squad levels)

### `scripts/admin/` - Administrative & Maintenance Scripts
**Purpose**: Scripts for database management, deployment, and system administration
**Target**: Database schema, table management, system maintenance
**Characteristics**:
- DDL execution and table creation
- Constraint management and index creation
- Database cleanup and maintenance
- Schema deployment and versioning
- Data integrity checks

**Examples**:
- `deploy_dim_stage.py` - Deploy stage dimension table
- `deploy_performance_tables.py` - Deploy aggregation tables
- `cleanup_constraints.py` - Fix database constraints
- `check_table_structure.py` - Inspect database schemas
- `enhance_dim_athlete.py` - Add columns to existing tables

### `scripts/other/` - Testing, Evaluation & Experimental Scripts
**Purpose**: Scripts for testing approaches, proof-of-concepts, and experimental work
**Target**: Testing and evaluation, not production use
**Characteristics**:
- Small test datasets and validation
- Proof-of-concept implementations
- Performance testing and benchmarking
- Approach evaluation and comparison
- Temporary debugging and analysis scripts

**Examples**:
- `test_concurrent_ingest.py` - Test concurrent ingestion approaches
- `test_threaded_ingest.py` - Validate threaded processing
- `test_dim_classification.py` - Test dimension functions
- `analyze_stages.py` - Analyze stage name patterns
- `test_stage_mapping.py` - Validate stage mappings

## 🎯 Usage Guidelines

### 1. **Bronze Scripts** - Raw Data Ingestion
- Use for any script that fetches data from external APIs
- Focus on robust error handling and rate limiting
- Implement proper logging and progress tracking
- Target `raw_*` tables exclusively

### 2. **Silver Scripts** - Data Transformation
- Use for ETL processes that transform bronze→silver
- Implement data quality checks and validation
- Focus on business logic and dimensional modeling
- Target `dim_*` and `fact_*` tables

### 3. **Admin Scripts** - System Management
- Use for database administration tasks
- DDL deployment and schema management
- System maintenance and cleanup
- Production deployment scripts

### 4. **Other Scripts** - Testing & Experiments
- Use for proof-of-concepts and testing
- Experimental approaches and evaluations
- Debugging and analysis tools
- Non-production scripts

## 📋 File Naming Conventions

### Bronze Scripts:
- `ingest_{data_type}.py` - Single data type ingestion
- `concurrent_{data_type}_ingest.py` - Concurrent ingestion
- `{data_type}_discovery.py` - Data discovery and enumeration

### Silver Scripts:
- `transform_{layer}.py` - Layer transformation (dimensions, facts)
- `etl_{process_name}.py` - Specific ETL processes
- `aggregate_{level}.py` - Data aggregation processes

### Admin Scripts:
- `deploy_{component}.py` - Deployment scripts
- `cleanup_{target}.py` - Cleanup and maintenance
- `check_{aspect}.py` - Validation and inspection
- `enhance_{target}.py` - Schema enhancements

### Other Scripts:
- `test_{feature}.py` - Testing scripts
- `analyze_{aspect}.py` - Analysis scripts
- `validate_{component}.py` - Validation scripts

## 🚀 Future Script Development

**All future scripts must be placed in the appropriate folder based on their primary purpose:**

1. **Data ingestion from APIs** → `bronze/`
2. **Bronze-to-silver transformation** → `silver/`
3. **Database administration** → `admin/`
4. **Testing and experimentation** → `other/`

This organization ensures maintainability, clear separation of concerns, and easy navigation of the codebase.

## 📂 Current Organization Summary

### Bronze Scripts (Data Ingestion)
- `complete_teams.py` - Team completion processing
- `concurrent_competition_ingest.py` - Competition discovery with pagination
- `concurrent_scoreboard_ingest.py` - Scoreboard data fetching
- `concurrent_team_ingest.py` - Async team ingestion
- `ingest_all_teams.py` - Sequential team ingestion
- `ingest_teams_range.py` - Range-based team ingestion
- `run_full_team_ingestion.py` - Production team ingestion runner
- `threaded_team_ingest.py` - Thread-based team ingestion

### Silver Scripts (Data Transformation)
- `etl_performance_aggregation.py` - Performance aggregation ETL pipeline
- `etl_stage_performance.py` - Stage aggregation with "drop slowest" logic
- `run_silver_etl.py` - Main silver ETL orchestrator
- `transform_dimensions.py` - Raw JSON to dimension table transformations
- `transform_facts.py` - Raw JSON to fact table transformations
- `transform_utils.py` - Common transformation utilities and helpers

### Admin Scripts (Database Management)
- `check_table_structure.py` - Database schema inspection
- `cleanup_constraints.py` - Constraint cleanup and fixes
- `cleanup_database.sql` - Database cleanup SQL scripts
- `deploy_dim_classification.py` - Classification dimension deployment
- `deploy_dim_stage.py` - Stage dimension deployment
- `deploy_performance_tables.py` - Performance tables deployment
- `enhance_dim_athlete.py` - Athlete dimension enhancements
- `fix_constraints.py` - Constraint modification scripts
- `run_ensure_tables.py` - Table creation and verification

### Other Scripts (Testing & Analysis)
- `analyze_stages.py` - Stage name pattern analysis
- `test_concurrent_ingest.py` - Concurrent ingestion testing
- `test_dim_classification.py` - Classification dimension testing
- `test_stage_mapping.py` - Stage mapping validation
- `test_threaded_ingest.py` - Thread-based ingestion testing

### Root Level (Orchestration)
- `run_complete_team_ingest.ps1` - PowerShell team ingestion orchestrator
- `run_ingest.ps1` - PowerShell general ingestion orchestrator
- `run_silver_etl.ps1` - PowerShell silver ETL orchestrator