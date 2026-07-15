# Deid v2 — Runbook

## Table of Contents
1. [Overview](#1-overview)
2. [Config File Reference](#2-config-file-reference)
3. [Historical vs Incremental Config](#3-historical-vs-incremental-config)
4. [Running Manually (Python Script)](#4-running-manually-python-script)
5. [Running from Airflow DAG (Incremental)](#5-running-from-airflow-dag-incremental)
6. [config_key Rules — One Key Per YAML](#6-config_key-rules--one-key-per-yaml)
7. [join_db for Incremental Runs](#7-join_db-for-incremental-runs)
8. [AthenaOne pipeline](#8-athenaone-pipeline)

---

## 1. Overview

`deid run` is a single blocking command that:
- Spawns its own Celery workers (fetch / process / write)
- Processes all tables in `tables.csv` in parallel batches
- Writes progress to `state.db` (SQLite)
- Exits with code `0` on success, non-zero on failure

State is tracked per `(table_name, config_key)` — so historical and incremental runs share the same `state.db` without interfering with each other.

---

## 2. Config File Reference

All config files live in:
```
deidentification/Airflow/scripts/configs/
```

| File | Purpose |
|------|---------|
| `config_historical.yaml` | Historical full-load run |
| `config_incremental.yaml` | Incremental (diff-merged) run |
| `arizona_config_rules.csv` | PHI rules per table/column |
| `reference_mappings.yaml` | Join chains for indirect patient/encounter IDs |
| `pii_config.yaml` | PII name table config for notes de-identification |
| `secondary_pii_config.yaml` | Secondary PII config |

### Key fields in config YAML

```yaml
# ── Source & Destination Databases ──────────────────────────────────
source_db:
  type: mssql
  host: 127.0.0.1
  port: 1437
  database: mobiledoc_base_2302    # MSSQL source
  username: ${SOURCE_USER}
  password: ${SOURCE_PW}

destination_db:
  type: mysql
  host: localhost
  port: 3306
  database: test_deid              # MySQL destination (staging)
  username: ${DEST_USER}
  password: ${DEST_PW}

mappings_db:
  type: mysql
  host: localhost
  port: 3306
  database: mapping_latest         # mapping tables DB
  username: ${DEST_USER}
  password: ${DEST_PW}

# OPTIONAL — only for incremental (see Section 7)
# join_db:
#   type: mssql
#   host: 127.0.0.1
#   port: 1437
#   database: mobiledoc_base_2302
#   username: ${SOURCE_USER}
#   password: ${SOURCE_PW}

# ── Namespace Key ────────────────────────────────────────────────────
config_key: historical             # MUST be unique per YAML file (see Section 6)

# ── State Storage ────────────────────────────────────────────────────
state_db_path: /absolute/path/to/deid_runtime/state.db
failed_rows_db_path: /absolute/path/to/deid_runtime/failed_rows.db

# ── Redis ────────────────────────────────────────────────────────────
redis_url: redis://localhost:6379/0

# ── De-Identification Settings ───────────────────────────────────────
deidentification:
  batch_size: 100000               # rows per batch
  date_offset_days: 34             # days to shift all dates
  patient_id_prefix: 10000000      # prefix added to de-identified patient IDs
  random_seed: 42

# ── Tables & Rules ───────────────────────────────────────────────────
rules_csv: /absolute/path/to/arizona_config_rules.csv

# ── Pipeline Phases ──────────────────────────────────────────────────
phases:
  - setup
  - deidentify

# ── Worker Settings ──────────────────────────────────────────────────
workers:
  fetchers: 2                      # parallel fetch workers
  processors: 16                   # parallel process workers
  max_retries: 1
  task_timeout: 3600               # seconds before a task is considered stuck

# ── Logging ──────────────────────────────────────────────────────────
logging:
  log_dir: /absolute/path/to/logs/historical

# ── PII / Notes Config ───────────────────────────────────────────────
pii_db:
  master_connection_str: "mysql+pymysql://${DEST_USER}:${DEST_PW}@localhost:3306/master_sep"
  secondary_pii_connection_str: "mysql+pymysql://${DEST_USER}:${DEST_PW}@localhost:3306/master_sep"
pii_config_path: /absolute/path/to/pii_config.yaml
secondary_pii_config_path: /absolute/path/to/secondary_pii_config.yaml

# ── Reference Mapping Joins ───────────────────────────────────────────
reference_mappings_path: /absolute/path/to/reference_mappings.yaml
```

---

## 3. Historical vs Incremental Config

| Setting | Historical (`config_historical.yaml`) | Incremental (`config_incremental.yaml`) |
|---------|--------------------------------------|-----------------------------------------|
| `config_key` | `historical` | `incremental` |
| `source_db.database` | `mobiledoc_base_2302` (full DB) | `mobiledoc_diff_merged` (delta only) |
| `destination_db.database` | `test_deid` | `deidentified_mobiledoc_diff_staging` |
| `mappings_db.database` | `mapping_latest` | `mapping_sep_test` |
| `join_db` | Not needed | Point to `mobiledoc_base_2302` (see Section 7) |
| `logging.log_dir` | `.../logs/historical` | `.../logs/incremental` |

**Both configs share the same:**
- `state_db_path` — same `state.db`, isolated by `config_key`
- `rules_csv` — same PHI rules
- `reference_mappings_path` — same join definitions
- `pii_config_path` / `secondary_pii_config_path`

---

## 4. Running Manually (Python Script)

Script location:
```
deidentification/Airflow/scripts/manual_deidentification_run_v2.py
```

### Basic usage

```bash
# Run historical (default config and tables)
python manual_deidentification_run_v2.py

# Run with specific config and tables
python manual_deidentification_run_v2.py \
  --config /path/to/config_historical.yaml \
  --tables /path/to/input/test.csv


# Resume a previously interrupted run (skip --rerun, keep existing state)
python manual_deidentification_run_v2.py --no-rerun
```

### What `--rerun` does (default: ON)
- Clears all batch state for the configured tables in `state.db`
- Purges stale Celery queues from Redis
- Starts a fresh run from scratch

Use `--no-rerun` only to resume a run that was interrupted mid-way.

### Environment
The script automatically loads `.env` from `Airflow/.env`. No need to export variables manually.

The Python executable used is read from the `DEID_PYTHON` env variable (defaults to `new_deid` conda env):
```
DEID_PYTHON=/opt/miniconda3/envs/new_deid/bin/python
```

---

## 5. Running from Airflow DAG (Incremental)

DAG: `deidentification_runner_v2`

The DAG runs the incremental config by default. The flow is:

```
run_mapping_jobs
    ↓
validate_mapping_master   (runs regardless of mapping outcome)
    ↓
run_deid_v2               (blocks until deid run exits — no timeout)
```

### DAG parameters (can be overridden at trigger time)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `config_yaml` | `configs/config_incremental.yaml` | Config YAML to use |
| `tables_filepath` | `input/tables.csv` | Tables CSV |

### Triggering manually
```
Airflow UI → deidentification_runner_v2 → Trigger DAG w/ Config
```

Override `config_yaml` to use a different config (e.g. historical) if needed.

### Post-process DAG
After `deidentification_runner_v2` succeeds, trigger:
```
deid_post_process_runner_v2
```
This reads `state.db`, checks row counts, then runs merge → delete.

---

## 6. config_key Rules — One Key Per YAML

**Rule: each YAML file must have a unique `config_key`. Never run two YAML files with the same `config_key` simultaneously.**

### Why
`config_key` is the namespace in `state.db`. All batch progress, table status, and Celery queue names are scoped to it:

- `state.db` tables: `batch_states`, `table_states` — partitioned by `config_key`
- Celery queues: `deid-fetch-{config_key}`, `deid-process-{config_key}`, `deid-write-{config_key}-{table}`

If two runs share the same `config_key`:
- Their batch rows in `state.db` will mix — one run's completed batches will inflate the other's progress counts
- Their Celery queues will collide — workers from one run will pick up tasks belonging to the other
- Both runs will produce incorrect output and report wrong completion status

### Correct setup

| Run type | YAML file | `config_key` |
|----------|-----------|-------------|
| Historical | `config_historical.yaml` | `historical` |
| Incremental | `config_incremental.yaml` | `incremental` |

Running historical and incremental simultaneously is safe because they have different `config_key` values.

**Never do this:**
```yaml
# config_historical.yaml  ← config_key: historical
# config_historical_v2.yaml  ← config_key: historical  ← WRONG, same key
```

---

## 7. join_db for Incremental Runs

### The problem

Some tables do not have a direct `patient_id` or `encounter_id` column. They link to patients indirectly — e.g. `edi_inv_cpt` has `InvoiceId`, which joins to `edi_invoice`, which has `PatientId`.

This is configured in `reference_mappings.yaml`:
```yaml
edi_inv_cpt:
  conditions:
    - source_column: InvoiceId
      reference_table: edi_invoice
      column_name: Id
```

During processing, the engine fetches rows from `edi_invoice` to resolve `PatientId` for each batch.

**For historical runs:** `source_db` is `mobiledoc_base_2302` — it contains both `edi_inv_cpt` and `edi_invoice`. No problem.

**For incremental runs:** `source_db` is `mobiledoc_diff_merged` — this only contains the *delta* (new/changed records). `edi_invoice` may have only a subset of records there, or none at all. When a batch of `edi_inv_cpt` has `InvoiceId` values that don't exist in `mobiledoc_diff_merged.edi_invoice`, the join returns null — `PatientId` is unresolved and the row is de-identified incorrectly.

### The fix — configure `join_db`

Add `join_db` to `config_incremental.yaml` pointing to the full historical database:

```yaml
join_db:
  type: mssql
  host: 127.0.0.1
  port: 1437
  database: mobiledoc_base_2302    # full DB — has all reference table records
  username: ${SOURCE_USER}
  password: ${SOURCE_PW}
```

When `join_db` is set, `reference_mappings.yaml` lookups (the indirect joins) use `join_db` instead of `source_db`. The incremental data is still fetched from `source_db` (`mobiledoc_diff_merged`) — only the reference table lookups go to `join_db`.

**Rule:** Always configure `join_db` in the incremental config whenever `reference_mappings.yaml` has entries for any table in your `tables.csv`.

---

## 8. AthenaOne pipeline

AthenaOne uses the consolidated architecture (`dags/athenaone/athenaone_pipeline_dag.py`,
`dag_id="athenaone_pipeline"`) but ingests from **Snowflake** instead of an on-prem
restore/diff-merge. All artifacts live under `scripts/athenaone/`.

```
snowflake_ingest → check_continue → run_mapping → [pii_data, master_insurance]
    → validate_mapping_master → init_deid_state → run_deid → check_deid_status
    → [merge_deid_data, s3_transfer]; merge_deid_data → delete_original_diffmerged
```

### Stages

| Stage | Script | Notes |
|-------|--------|-------|
| Ingest | `snowflake_ingest.py` (→ `ingest/*`) | 10-step Snowflake→MySQL pull; recreates `ATHENAONE_INCREMENTAL_SCHEMA`, merges into `ATHENAONE_HISTORICAL_SCHEMA` |
| Mapping | `incremental_mapping_update_v5_athenaone.py --config athenaone_mapping_master_config.yaml` | writes `mapping_pg` (patient/chart/encounter); runs with cwd=`scripts/athenaone` so the config's relative `sql/*.sql` paths resolve |
| Master (delta) | `master_pii_delta.py` + `master_insurance_delta.py` (shared `make_pii_data`/`make_master_insurance` factory tasks) | **true delta** — read the incremental pull (`--staging_schema` = incremental schema), resolve nd_patient_id via `mapping_pg`, per-patient DELETE+INSERT into `master_pg`. Same contract as eCW/Greenway. Master tables auto-create on first run. |
| Deid + post | shared tasks via `manual_deidentification_run_v2.py --config athenaone_config_incremental.yaml` | then merge + S3 |

Deps (`snowflake-*`) are in `deidentification_v2/requirements.txt`.

### Setup

1. Set the `ATHENAONE_*` + `DB_*` + deid `.env` vars (see the ATHENAONE INGESTION block
   in `.env.example`). Point `PII_YAML`/`SECONDARY_PII_YAML`/`REF_MAPPING_YAML` at
   `scripts/athenaone/*`, and the v5 `MAPPING_*`/`MASTER_*_CONN` + `MAPPING_CONFIG_FILE`
   at `mapping_pg`/`master_pg`.
2. Run `scripts/sql/mapping_migration_to_new_structure_athenaone.sql` once to create
   `mapping_pg`/`master_pg`.
3. Set `base_nd_id` in `athenaone_mapping_master_config.yaml` (unique per project).
4. `RULES_CSV`: point at a path that does not exist yet — the deid engine auto-generates
   it from the ingested schema on first run, then tune the `rule` column.
5. Provide `DEID_TABLES_CSV` / `DEID_POST_PROCESS_TABLES_CSV`.

Standalone test order: `snowflake_ingest.py` → mapping/master `--dry-run` → deid.
