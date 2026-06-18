# New Project Rollout — Mapping & Deidentification (V3)

Follow these steps in order for each new project. Use an existing project (e.g. `noran`) as a reference throughout.

---

## Step 1 — Pull Code

```bash
# Airflow automation repo
git clone -b arizona_automation https://github.com/neurodiscoveryai/airflow-automation.git
# or if already cloned:
cd /path/to/airflow-automation
git fetch origin && git checkout arizona_automation && git pull origin arizona_automation

# Deidentification repo
git clone -b chartid_deid_reworked https://github.com/neurodiscoveryai/deidentification_v2.git
# or if already cloned:
cd /path/to/deidentification_v2
git fetch origin && git checkout chartid_deid_reworked && git pull origin chartid_deid_reworked
```

---

## Step 2 — Set V3 Environment Variables in `.env`

Open `Airflow/.env` and add/update the following sections.

### V3 Mapping Source (MSSQL or MySQL)

```env
V3_MAPPING_SOURCE_HOST=<source_host>
V3_MAPPING_SOURCE_PORT=1433           # 3306 for MySQL
V3_MAPPING_SOURCE_DATABASE=<source_database>
V3_MAPPING_SOURCE_USER=<source_user>
V3_MAPPING_SOURCE_PASSWORD=<source_password>
```

### V3 Mapping Destination (always MySQL — the `mapping_pg` schema)

```env
V3_MAPPING_MYSQL_HOST=localhost
V3_MAPPING_MYSQL_DATABASE=mapping_pg
V3_MAPPING_MYSQL_USER=<mysql_user>
V3_MAPPING_MYSQL_PASSWORD=<mysql_password>
```

### V3 Master Recreate (connection string style)

```env
# Source — where PII / master data is fetched from
V3_MASTER_SOURCE_CONN=mssql+pymssql://<user>:<password>@<host>:<port>/<database>
# or for MySQL source:
# V3_MASTER_SOURCE_CONN=mysql+pymysql://<user>:<password>@<host>:<port>/<database>

# Destination — where pii_data_table / master_insurance_table are written
V3_MASTER_MYSQL_CONN=mysql+pymysql://<user>:<password>@localhost:3306/<master_db>

# Mapping — where patient_mapping_table lives
V3_MASTER_MAPPING_CONN=mysql+pymysql://<user>:<password>@localhost:3306/mapping_pg
```

### Mapping Config Path & Project Name

```env
# Full absolute path to the mapping master YAML you will create in Step 3
V3_MAPPING_CONFIG_FILE=/path/to/airflow-automation/Airflow/scripts/configs/<project>/mapping_master_config_<project>.yaml

# Must match the folder name under Airflow/scripts/configs/
PROJECT_NAME=<project>

# Path to the deid tables CSV for this project
DEID_TABLES_CSV=/path/to/airflow-automation/Airflow/input/<project>/tables.csv
```

---

## Step 3 — Create the Mapping Master YAML

Create `Airflow/scripts/configs/<project>/mapping_master_config_<project>.yaml`.

Copy from an existing project (e.g. `noran/mapping_master_config_noran.yaml`) and update:

```yaml
# V3 mapping config for <project>.
# source_type: 'mssql' or 'mysql'
source_type: mssql

patient:
  sql_file: /path/to/airflow-automation/Airflow/scripts/sql/<project>/patient_mapping.sql
  identifier_columns:
    - patientid
    - profileid
    - pid
  # nd_patient_id starts at this value when the mapping table is empty.
  # Must be unique across all projects — coordinate with team to avoid collision.
  base_nd_id: <unique_15_digit_base_id>

child_tables:

  - name: encounter
    sql_file: /path/to/airflow-automation/Airflow/scripts/sql/<project>/encounter_mapping.sql
    patient_ref_cols:
      - profileid       # column(s) in encounter SQL that link back to a patient identifier
    source_id_col: encounter_id
    date_col: encounter_date
    nd_id_suffix_len: 4
    mapping_table: encounter_mapping_table
    nd_id_col: nd_encounter_id

master_tables:

  - name: pii_data
    sql_file: /path/to/airflow-automation/Airflow/scripts/sql/<project>/master_pii_data.sql
    join_cols:
      - patientid       # must be in patient.identifier_columns
    target_table: pii_data_table

  - name: master_insurance
    sql_file: /path/to/airflow-automation/Airflow/scripts/sql/<project>/master_insurance_data.sql
    join_cols:
      - profileid
    target_table: master_insurance_table
```

> **Note:** `base_nd_id` must be unique per project. Each project gets its own numeric prefix range so nd_patient_ids never collide across projects on the same MySQL server.

---

## Step 4 — Create `config_historical.yaml` and `config_incremental.yaml`

Create the folder `Airflow/scripts/configs/<project>/` and add both files.
Copy from `Airflow/scripts/configs/noran/` as a template and update the fields below.

### Fields to update in both files

| Field | What to change |
|---|---|
| `source_db.database` | Source DB schema name for this project |
| `destination_db.database` | `historical` → your deidentified merge DB; `incremental` → your deid staging DB |
| `mappings_db.database` | Usually `mapping_pg` (same for all projects) |
| `join_db.database` | Source original DB (historical only; remove if not needed) |
| `config_key` | `historical` or `incremental` (leave as-is) |
| `rules_csv` | Absolute path to the project PHI rules CSV |
| `logging.log_dir` | Absolute path under `deid_runtime/logs/` |
| `pii_db.master_connection_str` | Point to `master_pg` for this server |
| `pii_config_path` | Absolute path to the project PII config YAML |
| `secondary_pii_config_path` | Absolute path to the project secondary PII config YAML |
| `reference_mappings_path` | Absolute path to the reference mappings YAML |
| `mapping_tables.patient.identifier_columns` | Must match `patient.identifier_columns` in the mapping master YAML |

### Testing tip — `config_historical.yaml`

When running a test deid, change `destination_db.database` to a throwaway test schema so you don't write to the real deidentified DB.

---

## Step 5 — Run the Mapping Migration SQL in MySQL Workbench

Open MySQL Workbench and connect to the MySQL server.

Run the migration SQL file for this project:

```
Airflow/scripts/sql/mapping_migration_to_new_structure_<project>.sql
```

> If any identifier column (e.g. `patientid`) is stored as text in the source (`mapping_prod`), the destination column must be `VARCHAR` instead of `BIGINT`.
> Run this **before** executing the INSERT in the migration script:
> ```sql
> ALTER TABLE mapping_pg.patient_mapping_table
>   MODIFY COLUMN patientid VARCHAR(64);
> ```
> Do the same for any other text identifier columns (`profileid`, `pid`).

If the migration SQL doesn't exist yet for this project, create it by copying from `mapping_migration_to_new_structure_greenway_ecw.sql` and updating the source schema name.

---

## Step 6 — Verify `mapping_pg` Data

Run these queries in MySQL Workbench after the migration:

```sql
-- Patient count should match source
SELECT COUNT(*) FROM mapping_pg.patient_mapping_table;

-- Encounter count should match source
SELECT COUNT(*) FROM mapping_pg.encounter_mapping_table;

-- Spot-check a few rows
SELECT * FROM mapping_pg.patient_mapping_table LIMIT 10;
SELECT * FROM mapping_pg.encounter_mapping_table LIMIT 10;

-- Confirm no NULLs in nd_patient_id
SELECT COUNT(*) FROM mapping_pg.patient_mapping_table WHERE nd_patient_id IS NULL;
```

---

## Step 7 — Run the `mapping_master_v3` DAG

1. Open the Airflow UI
2. Navigate to the `mapping_master_v3` DAG
3. Trigger a manual run
4. Monitor each task: `run_mapping_jobs` → `validate_mapping_master` → (master recreate tasks)
5. All tasks should turn green

---

## Step 8 — Verify Data After DAG Run

```sql
-- pii_data_table and master_insurance_table should be populated
SELECT COUNT(*) FROM master_pg.pii_data_table;
SELECT COUNT(*) FROM master_pg.master_insurance_table;

-- Check state files on disk
cat $STATE_DIR/mapping_master_v3/v3_mapping_update_state.json
cat $STATE_DIR/mapping_master_v3/v3_master_state.json
```

Both state files should contain `"success": true`.

---

## Step 9 — Run Manual Deidentification (Historical Test)

Before running against the real destination, change `destination_db.database` in `config_historical.yaml` to a test schema.

```bash
cd /path/to/airflow-automation/Airflow/scripts

python manual_deidentification_run_v2.py \
  --config configs/<project>/config_historical.yaml \
  --tables /path/to/input/<project>/tables.csv
```

Once verified, restore `destination_db.database` to the real deidentified schema and re-run for production.
