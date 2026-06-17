# master_recreate_v3.py — Detailed Explanation

**File:** `Airflow/scripts/master_recreate_v3.py`  
**Purpose:** Reads patient data from a source database, joins it with `patient_mapping_table`
to assign de-identified IDs (`nd_patient_id`), and writes the result into destination MySQL
master tables (e.g. `pii_data_table`, `master_insurance_table`).

> **Must run AFTER** `incremental_mapping_update_v3.py` — it depends on `patient_mapping_table`
> being populated first.

---

## Prerequisites

| Requirement | Detail |
|-------------|--------|
| `patient_mapping_table` populated | Run mapping script first |
| YAML config | Passed via `--config` CLI arg — same file as the mapping script |
| Three env vars in `.env` | `V3_MASTER_SOURCE_CONN`, `V3_MASTER_MYSQL_CONN`, `V3_MASTER_MAPPING_CONN` |

---

## How to Run

```bash
# Normal run
python master_recreate_v3.py --config /path/to/mapping_master_config.yaml

# Dry run — reads and validates but writes nothing to DB
python master_recreate_v3.py --config /path/to/mapping_master_config.yaml --dry-run
```

---

## Database Connections

Three **separate** connection strings (SQLAlchemy format), set in `.env`:

| Env Var | Used for | Example value |
|---------|----------|---------------|
| `V3_MASTER_SOURCE_CONN` | Read PII/master data from source | `mysql+pymysql://user:pass@host/veradigm` |
| `V3_MASTER_MYSQL_CONN` | Write output tables to destination | `mysql+pymysql://user:pass@host/mapping` |
| `V3_MASTER_MAPPING_CONN` | Read `patient_mapping_table` | `mysql+pymysql://user:pass@host/mapping` |

The **dialect prefix** in the connection string picks the driver automatically:
- `mssql+pymssql://` → uses pymssql (MSSQL source)
- `mysql+pymysql://` → uses pymysql (MySQL source)

No code change is needed to switch between MSSQL and MySQL — only the connection string changes.

---

## YAML Config (master_tables section)

The script reads `master_tables` from the same YAML file used by the mapping script:

```yaml
master_tables:
  - name: pii_data                    # label used in logs and state file
    sql_file: /path/to/pii.sql        # SQL to fetch source data
    join_cols:                         # identifier columns used to look up nd_patient_id
      - patientid                      # tried in order — first match wins per row
      - profileid
    target_table: pii_data_table       # destination MySQL table name
```

**Rules for `join_cols`:**
- Every entry must be in `patient.identifier_columns` (validated at config load time).
- Tried **in order** per row — the first column that finds a match is used.
- This is **NOT** a multi-column AND join — it is a **prioritised fallback** per row.
- If no column matches → row gets `NULL` `nd_patient_id` (warning logged, row still written).

---

## Step-by-Step Execution

### Step 1 — Parse CLI args

- `--config` (required): path to YAML config file.
- `--dry-run` (optional): if set, all DB writes are skipped; only logging happens.

### Step 2 — Load and validate config

- Calls `load_config()` from `incremental_mapping_update_v3.py` (reused).
- Validates `master_tables` section:
  - Each table must have: `name`, `sql_file`, `join_cols`, `target_table`.
  - `join_cols` must be a subset of `patient.identifier_columns`.

### Step 3 — Load DB connection strings from `db_config`

- Reads `V3_MASTER_SOURCE_CONN_STR`, `V3_MASTER_MYSQL_CONN_STR`, `V3_MASTER_MAPPING_CONN_STR`.
- These come from `db_config.py` which loads them from `Airflow/.env`.

### Step 4 — Setup logging

- Creates log directory: `{STATE_DIR}/mapping_master_v3/v3_master_runner/`
- Log file name: `v3_master_YYYY-MM-DD.log`
- Writes to both the log file and stdout simultaneously.

### Step 5 — Daily deduplication check (skip if already done today)

- Reads state from `{STATE_DIR}/mapping_master_v3/v3_master_state.json`.
- If **all** master tables have `last_processed_date = today` AND `success = True` → **skip** entirely.
- This prevents re-running the same data multiple times in one day.
- Bypassed when `--dry-run` is used.

### Step 6 — Load patient mapping into memory

**Function:** `load_patient_maps_from_mysql(mapping_conn_str, identifier_columns)`

- Connects to the **mapping MySQL** using `V3_MASTER_MAPPING_CONN`.
- Runs: `SELECT patientid, profileid, ..., nd_patient_id FROM patient_mapping_table`
- Builds a per-column lookup dict:
  ```
  patient_maps_by_col = {
    "patientid":  {123 → 100100150000001, 456 → 100100150000002, ...},
    "profileid":  {10  → 100100150000001, 20  → 100100150000002, ...},
  }
  ```
- All `NULL` values are excluded from the map.
- This map is built **once** and shared across all master tables.

### Step 7 — Process each master table (in YAML order)

For each entry in `master_tables`:

#### 7a — Fetch source data

**Function:** `execute_source_query_df(source_conn_str, sql_file_path)`

- Reads the SQL file from disk.
- Connects to **source DB** using `V3_MASTER_SOURCE_CONN`.
- Executes the query and returns results as a pandas DataFrame.

#### 7b — Remove duplicate rows

- `df.drop_duplicates(inplace=True)` — exact duplicate rows are removed.
- Row count before and after is logged.

#### 7c — Validate join columns

- Checks that at least one of `join_cols` is present in the DataFrame columns.
- If **none** are present → raises `ValueError` and aborts (nothing written).
- If some are missing → logs a warning per missing column; uses remaining ones.

#### 7d — Resolve `nd_patient_id` (join logic)

**Function:** `resolve_nd_patient_ids_df(df, patient_maps_by_col, join_cols)`

- Adds a new column `nd_patient_id` to the DataFrame.
- **Algorithm (ordered fallback per row):**
  1. Start with `nd_patient_id = NULL` for all rows.
  2. For each column in `join_cols` (in order):
     - Map that column's values through `patient_maps_by_col[col]`.
     - Fill in `nd_patient_id` only for rows where it is still `NULL`.
  3. After all columns are tried, any row still `NULL` = unmapped patient.

- **Example:**
  ```
  Row: patientid=123, profileid=10
  join_cols: [patientid, profileid]

  Step 1: patientid=123 → found → nd_patient_id = 100100150000001 ✓ (stop)
  Step 2: skipped (already filled)
  ```

  ```
  Row: patientid=NULL, profileid=10
  Step 1: patientid=NULL → skip
  Step 2: profileid=10 → found → nd_patient_id = 100100150000001 ✓
  ```

- Unmapped rows (nd_patient_id = NULL) are **logged as warnings** but still written.

#### 7e — Dry-run check

- If `--dry-run`: logs row count and unmapped count, returns immediately. No DB writes.

#### 7f — Table rotation

**Function:** `rotate_table(mysql_engine, table_name)`

- Runs on the **destination MySQL**.
- Logic:
  1. `DROP TABLE IF EXISTS pii_data_table_old` — remove previous backup if it exists.
  2. Check if `pii_data_table` currently exists.
  3. If yes: `RENAME TABLE pii_data_table TO pii_data_table_old` — preserve old data.
  4. If no: skip rename (first run).
- This ensures the old table is always available as `_old` for rollback.

#### 7g — Write new table

- `df.to_sql(target_table, con=mysql_engine, if_exists="replace", chunksize=1000)`
- Writes the entire DataFrame to `pii_data_table` (or whichever `target_table` is).
- `chunksize=1000` → inserts in batches of 1000 rows.
- `index=False` → DataFrame row index is not written as a column.
- The `nd_patient_id` column is included alongside all source columns.

### Step 8 — Save state

- Writes results for each table to `v3_master_state.json`:
  ```json
  {
    "pii_data": {
      "last_processed_date": "2026-06-09",
      "success": true,
      "row_count": 5000,
      "unmapped_rows": 12
    }
  }
  ```
- On failure: writes `success: false` and the error message, then re-raises the exception.

---

## Error Handling Rules

| Scenario | Behaviour |
|----------|-----------|
| `join_cols` not found in source SQL at all | `ValueError` raised — table aborted |
| Some `join_cols` missing (partial) | Warning logged — uses remaining columns |
| Rows with no matching patient | `nd_patient_id = NULL` — written with warning |
| Script already ran today (success) | Skip entirely — no re-processing |
| Any exception during write | State saved as `success: false`, exception re-raised |
| `--dry-run` | All reads and validations happen; zero DB writes |

---

## Output

For each master table in config, the script produces:

1. **A MySQL table** (e.g. `pii_data_table`) with all source columns + `nd_patient_id`.
2. **A backup table** (e.g. `pii_data_table_old`) containing the previous run's data.
3. **A log file** at `{STATE_DIR}/mapping_master_v3/v3_master_runner/v3_master_YYYY-MM-DD.log`.
4. **A state file** at `{STATE_DIR}/mapping_master_v3/v3_master_state.json`.

---

## State File Location

```
{STATE_DIR}/
└── mapping_master_v3/
    ├── v3_master_state.json          ← per-table success/failure tracking
    └── v3_master_runner/
        └── v3_master_2026-06-09.log  ← today's log
```

---

## Function Summary

| Function | Purpose |
|----------|---------|
| `setup_logging(state_dir)` | Create log dir + configure file + stdout logging |
| `MasterStateManager` | Read/write `v3_master_state.json` |
| `load_patient_maps_from_mysql(conn_str, id_cols)` | Build `{col → {val → nd_patient_id}}` from mapping table |
| `resolve_nd_patient_ids_df(df, maps, join_cols)` | Add `nd_patient_id` column via ordered-fallback join |
| `execute_source_query_df(conn_str, sql_file)` | Fetch source data as DataFrame |
| `rotate_table(engine, table_name)` | Rename current table to `_old`, drop previous `_old` |
| `transfer_master_table(...)` | Full pipeline for one master table (fetch → join → rotate → write) |
| `run_master_etl(config, ...)` | Load patient map once, then call transfer for each table |
| `main()` | CLI entry point: parse args → load config → check state → run ETL → save state |
