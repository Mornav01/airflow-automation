# Deid v2 — First-Time Installation Guide

## Prerequisites

- Miniconda or Anaconda installed
- Access to the deid v2 source code at:
  `/Users/ndaicnsarizona/Desktop/deidentification_testing/deidentification_v2/`
- Redis running on `localhost:6379`
- MSSQL source DB accessible
- MySQL destination DB accessible

---

## Step 1 — Create the Conda Environment

```bash
conda create -n new_deid python=3.12 -y
conda activate new_deid
```

---

## Step 2 — Install Dependencies

```bash
cd /Users/ndaicnsarizona/Desktop/deidentification_testing/deidentification_v2

pip install -r requirements.txt
```

This installs all required packages including:
- `celery`, `redis` — task queue
- `polars`, `pandas`, `pyarrow` — data processing
- `sqlalchemy`, `pymssql`, `pymysql` — database drivers
- `spacy`, `presidio-analyzer` — NLP/PII detection
- `pydantic`, `typer` — config and CLI

---

## Step 3 — Install the deid Package (Editable Mode)

```bash
cd /Users/ndaicnsarizona/Desktop/deidentification_testing/deidentification_v2

pip install -e .
```

This registers the `deid` command globally in the `new_deid` environment.

**Verify:**
```bash
deid --help
# or
python -m deid --help
```

You should see the list of commands: `run`, `status`, `retry`, `mapping`, `qc`, etc.

---

## Step 4 — Install spaCy Language Model

Required for notes de-identification (PII scanning in free text):

```bash
python -m spacy download en_core_web_lg
```

**Verify:**
```bash
python -c "import spacy; spacy.load('en_core_web_lg'); print('OK')"
```

---

## Step 5 — Configure Environment Variables

Create or update `deidentification/Airflow/.env`:

```bash
# Source DB (MSSQL)
SOURCE_USER=sa
SOURCE_PW=your_mssql_password

# Destination DB (MySQL)
DEST_USER=ndadmin
DEST_PW=your_mysql_password

# Python executable (used by manual script and Airflow DAG)
DEID_PYTHON=/opt/miniconda3/envs/new_deid/bin/python
AIRFLOW_PYTHON=/opt/miniconda3/envs/new_deid/bin/python
```

---

## Step 6 — Create Runtime Directories

```bash
mkdir -p /Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/deid_runtime/logs
```

---

## Step 7 — Verify Redis is Running

```bash
redis-cli ping
# Should return: PONG
```

If not running:
```bash
redis-server --daemonize yes
```

---

## Step 8 — Run Mapping (One-Time Before First Run)

Mapping tables must exist before running `deid run`:

```bash
conda activate new_deid
cd /Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/scripts

python -m deid mapping --config configs/config_historical.yaml
```

---

## Step 9 — Verify Installation End-to-End

```bash
conda activate new_deid

# Check deid command works
deid --help

# Check status (state.db may not exist yet — that's fine)
deid status \
  --state-db /Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/deid_runtime/state.db \
  --config-key historical
```

---

## Quick Reference — Activating for Manual Runs

Every time you open a new terminal:

```bash
conda activate new_deid
cd /Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/scripts

# Run historical
python manual_deidentification_run_v2.py

# Run with specific tables
python manual_deidentification_run_v2.py --tables /path/to/input/test.csv
```

---

## Re-installation After Code Changes

If the deid v2 source code is updated (new features, bug fixes):

```bash
conda activate new_deid
cd /Users/ndaicnsarizona/Desktop/deidentification_testing/deidentification_v2

# Pull latest code (if using git)
git pull

# Re-install (editable install picks up changes automatically,
# but re-run if pyproject.toml or entry points changed)
pip install -e .
```

> **Note:** Since deid is installed in editable mode (`pip install -e .`), most code changes are picked up automatically without re-installing. Only run `pip install -e .` again if `pyproject.toml` or `requirements.txt` changes.
