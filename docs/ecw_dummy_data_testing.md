# eCW pipeline — testing with dummy data

How to stand up a throwaway BASE/PROD (MSSQL) + mapping_pg/master_pg (MySQL)
environment and run the `ecw_pipeline` DAG against synthetic data instead of a
real client's database.

Scripts live in `tests/ecw_dummy_data/`. Diff+merge is hardcoded to MSSQL
(`Airflow/scripts/db_config.py`), independent of `SOURCE_DB_TYPE`, so BASE,
PROD, STAGING, and MERGED all need to live on one SQL Server instance —
that's what the Docker setup below provides.

## 1. Start the MSSQL container

```bash
cd airflow-automation/tests/ecw_dummy_data
MSSQL_SA_PASSWORD='Ecw_Test_Pass1!' ./setup_mssql.sh
```

This starts SQL Server in Docker and creates the 4 databases (base/prod/staging/merged).
Needs Docker (or Colima) running first — `colima start` if you're on Colima and
`docker ps` errors with "Cannot connect to the Docker daemon".

## 2. Install dependencies (once)

```bash
pip install sqlalchemy pyodbc pymysql faker
```

`pyodbc` also needs the Microsoft ODBC driver installed on the host (it connects
from your machine into the container over the network):

```bash
brew tap microsoft/mssql-release && brew install msodbcsql17 mssql-tools
```

## 3. Seed the MSSQL source (BASE + PROD)

```bash
python seed_source_mssql.py --num-patients 1000 --password 'Ecw_Test_Pass1!'
```

Generates dummy `users`/`patients`/`enc`/`edi_invoice`/`hcfa`/`progressnotes`
data in both databases (PROD has more/newer data than BASE — some patients are
new-only-in-PROD, some are changed between BASE and PROD, most are unchanged),
and writes `tables.csv` + `primary_keys.csv` for the diff+merge stage.

## 4. Seed the MySQL destination (mapping_pg + master_pg)

```bash
python seed_dest_mysql.py --password '<your mysql password>'
```

Creates the empty destination tables (`patient_mapping_table`,
`encounter_mapping_table`, `pii_data_table`, `master_insurance_table`) that the
pipeline populates as it runs.

## 5. Point `Airflow/.env` at this test setup

```
SOURCE_DB_TYPE=mssql
SOURCE_DB_HOST=localhost
SOURCE_DB_USER=sa
SOURCE_DB_PASS=Ecw_Test_Pass1!
ODBC_DRIVER=ODBC Driver 17 for SQL Server
BASE_DB_NAME=ecw_test_base
PROD_DB_NAME=ecw_test_prod
STAGING_DB_NAME=ecw_test_staging
MERGED_DB_NAME=ecw_test_diffmerged
DIFF_MERGE_TABLES_CSV=/absolute/path/to/tests/ecw_dummy_data/tables.csv
PK_CSV=/absolute/path/to/tests/ecw_dummy_data/primary_keys.csv

# existing MySQL destination
DB_HOST=<your mysql host>
DB_USER=<your mysql user>
DB_PASS=<your mysql password>
MAPPING_DB_NAME=mapping_pg
MASTER_DB_NAME=master_pg
```

## 6. Run

Trigger the `ecw_pipeline` DAG in Airflow with param `skip_restore=true` — it
runs diff+merge → mapping → PII/insurance/pnotes decrypt → deid against this
dummy data.

## Reference

Full design rationale (why diff+merge forces MSSQL, exact column sources,
PK vs. no-PK table choices) is in
`/Users/karanchilwal/.claude/plans/get-context-of-ecw-pipeline-dag-eventual-yeti.md`.
