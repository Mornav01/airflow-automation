"""
V3 Incremental Mapping Update — general, YAML-driven star-schema mapper.

Patient is the centre table. Any number of child tables (encounter, appointment,
chart, …) are configured in a YAML file; nothing is hardcoded.
"""

import pymssql
import mysql.connector
import argparse
import logging
import sys
import os
import json
import random
import yaml
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))
BATCH_SIZE = 1000


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)
    runner_dir = os.path.join(state_dir, "v3_mapping_runner")
    os.makedirs(runner_dir, exist_ok=True)
    log_file = os.path.join(runner_dir, f"v3_mapping_{date.today().isoformat()}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config(config_path):
    """Load and validate the YAML mapping config file."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    source_type = cfg.get("source_type", "mssql").lower()
    if source_type not in ("mssql", "mysql"):
        raise ValueError(f"source_type must be 'mssql' or 'mysql', got: '{source_type}'")
    cfg["source_type"] = source_type

    for key in ("patient", "child_tables"):
        if key not in cfg:
            raise ValueError(f"Config missing required top-level key: '{key}'")

    patient = cfg["patient"]
    for key in ("sql_file", "identifier_columns", "base_nd_id"):
        if key not in patient:
            raise ValueError(f"Config 'patient' section missing required key: '{key}'")
    if not isinstance(patient["identifier_columns"], list) or not patient["identifier_columns"]:
        raise ValueError("'patient.identifier_columns' must be a non-empty list")

    for tbl in cfg["child_tables"]:
        required = (
            "name", "sql_file", "patient_ref_cols", "source_id_col",
            "date_col", "nd_id_suffix_len", "mapping_table", "nd_id_col",
        )
        for key in required:
            if key not in tbl:
                raise ValueError(f"Child table '{tbl.get('name', '?')}' missing required key: '{key}'")
        if not isinstance(tbl["patient_ref_cols"], list) or not tbl["patient_ref_cols"]:
            raise ValueError(f"Child table '{tbl['name']}': 'patient_ref_cols' must be a non-empty list")

    # Every patient_ref_col must exist in identifier_columns so lookups can succeed.
    identifier_set = set(cfg["patient"]["identifier_columns"])
    for tbl in cfg["child_tables"]:
        invalid = [c for c in tbl["patient_ref_cols"] if c not in identifier_set]
        if invalid:
            raise ValueError(
                f"Child table '{tbl['name']}': patient_ref_cols {invalid} "
                f"must all be in patient.identifier_columns {sorted(identifier_set)}"
            )

    return cfg


# ─── State ────────────────────────────────────────────────────────────────────

class MappingStateManager:

    def __init__(self, state_dir):
        self.state_file = os.path.join(state_dir, "v3_mapping_update_state.json")
        os.makedirs(state_dir, exist_ok=True)

    def load(self):
        if not os.path.exists(self.state_file):
            return {}
        with open(self.state_file) as f:
            return json.load(f)

    def save(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)


# ─── QC: SQL column validation ────────────────────────────────────────────────

def validate_patient_sql_columns(src_data, identifier_columns):
    """Fail fast if the patient SQL result is missing expected columns."""
    if not src_data:
        logging.warning("[patient] Patient SQL returned 0 rows.")
        return
    required = set(identifier_columns) | {"registration_date"}
    actual   = set(src_data[0].keys())
    missing  = required - actual
    if missing:
        raise ValueError(
            f"Patient SQL is missing columns: {sorted(missing)}. "
            f"Expected: {sorted(required)}. Got: {sorted(actual)}"
        )
    extra = actual - required
    if extra:
        logging.warning(
            f"[patient] SQL returns extra columns not used by the script: {sorted(extra)}. "
            "These will be ignored."
        )


def validate_child_sql_columns(src_data, table_cfg):
    """Fail fast if the child SQL result is missing source_id_col, date_col, or all patient_ref_cols."""
    mapping_table = table_cfg["mapping_table"]
    name          = table_cfg["name"]
    if not src_data:
        logging.warning(f"[{mapping_table}] Child SQL returned 0 rows.")
        return
    actual       = set(src_data[0].keys())
    source_id    = table_cfg["source_id_col"]
    date_col     = table_cfg["date_col"]
    ref_cols     = table_cfg["patient_ref_cols"]
    missing_core = {source_id, date_col} - actual
    if missing_core:
        raise ValueError(
            f"[{name}] Child SQL missing required columns: {sorted(missing_core)}. "
            f"Got: {sorted(actual)}"
        )
    present_refs = [c for c in ref_cols if c in actual]
    if not present_refs:
        raise ValueError(
            f"[{name}] Child SQL must return at least one of patient_ref_cols "
            f"{ref_cols}. Got: {sorted(actual)}"
        )
    missing_refs = [c for c in ref_cols if c not in actual]
    if missing_refs:
        logging.warning(
            f"[{name}] patient_ref_cols {missing_refs} not in SQL result — "
            "fallback lookup will use remaining ref cols only."
        )


# ─── QC: patient source deduplication ────────────────────────────────────────

def dedup_patient_source_data(src_data, identifier_columns):
    """
    Keep only the first occurrence of each unique identifier combination.
    Drops duplicate rows (same values across all identifier_columns) with a warning.
    This guarantees at most one row per patient enters the insertion loop, regardless
    of how many times the source table repeats the same patient.
    """
    seen    = set()
    deduped = []
    dropped = 0
    for row in src_data:
        key = tuple(row.get(col) for col in identifier_columns)
        if key in seen:
            dropped += 1
        else:
            seen.add(key)
            deduped.append(row)
    if dropped:
        logging.warning(
            f"[patient] Dropped {dropped} duplicate source rows "
            f"(same {identifier_columns} combination). Keeping first occurrence per patient."
        )
    return deduped


def dedup_child_source_data(src_data, source_id_col):
    """
    Keep only the first occurrence of each unique source_id_col value.
    Duplicate rows (same source entity ID repeated) are dropped with a warning.
    NULL source_ids are passed through unchanged — they are skipped downstream.
    """
    seen    = set()
    deduped = []
    dropped = 0
    for row in src_data:
        sid = row.get(source_id_col)
        if sid is None:
            deduped.append(row)
            continue
        if sid in seen:
            dropped += 1
        else:
            seen.add(sid)
            deduped.append(row)
    if dropped:
        logging.warning(
            f"[{source_id_col}] Dropped {dropped} duplicate source rows. "
            "Keeping first occurrence per source ID."
        )
    return deduped


# ─── QC: identifier uniqueness ────────────────────────────────────────────────

def validate_identifier_uniqueness(src_data, identifier_columns):
    """
    Fail fast if identifier columns are not all 1:1 with each other.
    All identifier_columns must have an identical distinct count across every
    unique combination of their values.
    """
    seen = set()
    unique_rows = []
    for row in src_data:
        key = tuple(row.get(col) for col in identifier_columns)
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)

    distinct_counts = {
        col: len({r.get(col) for r in unique_rows if r.get(col) is not None})
        for col in identifier_columns
    }
    distinct_counts["combined"] = len(seen)

    counts = list(distinct_counts.values())
    if len(set(counts)) != 1:
        raise ValueError(
            f"Identifier columns are NOT 1:1. Distinct counts: {distinct_counts}. ETL aborted."
        )

    logging.info(
        f"Identifier uniqueness check passed — {counts[0]} unique patients "
        f"across columns: {identifier_columns}"
    )


# ─── Patient lookup ───────────────────────────────────────────────────────────

def resolve_nd_patient_id(row, patient_maps_by_col, patient_ref_cols):
    """
    Try each ref col in order; return the first nd_patient_id match found.
    Returns None if no ref col in the row maps to a known patient.
    """
    for col in patient_ref_cols:
        val = row.get(col)
        if val is not None:
            nd_pid = patient_maps_by_col.get(col, {}).get(val)
            if nd_pid is not None:
                return nd_pid
    return None


# ─── ND-ID generation ─────────────────────────────────────────────────────────

def generate_nd_id(nd_patient_id, last_nd_id, suffix_len, use_len_guard=False):
    """
    Generate the next nd_xxx_id for a patient.

    suffix_len: digits in the suffix appended to nd_patient_id on first ID.
                e.g. suffix_len=4  → "{nd_patient_id}0001"
                     suffix_len=5  → "{nd_patient_id}00001"
    use_len_guard: when True (chart-style), treat any existing ID whose string
                   length is ≤ 15 (i.e. equal to nd_patient_id alone) as
                   uninitialised and restart from the suffixed base.
    """
    suffix = "0" * (suffix_len - 1) + "1"
    if use_len_guard:
        if last_nd_id is None or len(str(last_nd_id)) <= 15:
            return int(str(nd_patient_id) + suffix)
        return last_nd_id + 1
    else:
        if last_nd_id is None:
            return int(str(nd_patient_id) + suffix)
        return last_nd_id + 1


# ─── Table DDL ────────────────────────────────────────────────────────────────

def build_create_table_ddl(table_cfg):
    """Return CREATE TABLE IF NOT EXISTS DDL from a child table config dict."""
    mapping_table = table_cfg["mapping_table"]
    source_id_col = table_cfg["source_id_col"]
    nd_id_col     = table_cfg["nd_id_col"]
    date_col      = table_cfg["date_col"]
    return (
        f"CREATE TABLE IF NOT EXISTS `{mapping_table}` ("
        f"  nd_patient_id     BIGINT        NOT NULL,"
        f"  `{source_id_col}` VARCHAR(255)  NOT NULL,"
        f"  `{nd_id_col}`     BIGINT        NOT NULL,"
        f"  `{date_col}`      DATETIME,"
        f"  nd_ActiveFlag     CHAR(1)       NOT NULL DEFAULT 'Y',"
        f"  created_at        DATETIME      NOT NULL,"
        f"  updated_at        DATETIME      NOT NULL,"
        f"  PRIMARY KEY (`{source_id_col}`),"
        f"  INDEX idx_nd_patient_id (nd_patient_id)"
        f")"
    )


def ensure_child_table(mysql_conn, mysql_cursor, table_cfg):
    """Create the child mapping table if it doesn't exist; ensure nd_ActiveFlag column."""
    mapping_table = table_cfg["mapping_table"]

    mysql_cursor.execute(build_create_table_ddl(table_cfg))
    mysql_conn.commit()
    logging.info(f"Ensured table exists: {mapping_table}")

    mysql_cursor.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        f"AND TABLE_NAME = '{mapping_table}' "
        "AND COLUMN_NAME = 'nd_ActiveFlag'"
    )
    row = mysql_cursor.fetchone()
    cnt = row["cnt"] if isinstance(row, dict) else row[0]
    if not cnt:
        logging.info(f"nd_ActiveFlag missing on {mapping_table}; adding with default 'Y'.")
        mysql_cursor.execute(
            f"ALTER TABLE `{mapping_table}` ADD COLUMN nd_ActiveFlag CHAR(1) NOT NULL DEFAULT 'Y'"
        )
        mysql_cursor.execute(f"UPDATE `{mapping_table}` SET nd_ActiveFlag = 'Y'")
        mysql_conn.commit()


# ─── Patient mapping table DDL ───────────────────────────────────────────────

def ensure_patient_mapping_table(mysql_conn, mysql_cursor, identifier_columns):
    """Create patient_mapping_table if it doesn't exist, driven by identifier_columns."""
    id_col_defs = " ".join(f"`{c}` VARCHAR(255)," for c in identifier_columns)
    id_indexes  = " ".join(f"INDEX idx_{c} (`{c}`)," for c in identifier_columns)
    mysql_cursor.execute(
        f"CREATE TABLE IF NOT EXISTS patient_mapping_table ("
        f"  nd_patient_id     BIGINT NOT NULL,"
        f"  {id_col_defs}"
        f"  `offset`          INT NOT NULL,"
        f"  registration_date DATETIME,"
        f"  created_at        DATETIME NOT NULL,"
        f"  updated_at        DATETIME NOT NULL,"
        f"  PRIMARY KEY (nd_patient_id),"
        f"  {id_indexes.rstrip(',')}"
        f")"
    )
    mysql_conn.commit()
    logging.info("Ensured patient_mapping_table exists.")


# ─── Source DB query (MSSQL or MySQL) ────────────────────────────────────────

def execute_source_query(source_type, conn_params, sql_file_path):
    """Execute a SELECT query against the source DB (mssql or mysql)."""
    query = Path(sql_file_path).read_text()
    if source_type == "mssql":
        conn = pymssql.connect(
            server=conn_params["host"],
            port=conn_params["port"],
            database=conn_params["database"],
            user=conn_params["user"],
            password=conn_params["password"],
        )
        cursor = conn.cursor(as_dict=True)
    else:
        conn   = mysql.connector.connect(**conn_params)
        cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        cursor.close()
        conn.close()


# ─── Patient phase ────────────────────────────────────────────────────────────

def _load_patient_maps_from_db(mysql_cursor, identifier_columns):
    """
    Build patient_maps_by_col: {identifier_col → {col_value → nd_patient_id}}
    One dict per identifier column so any child table can look up by whichever
    identifier columns its SQL returns.
    """
    id_cols_csv = ", ".join(f"`{c}`" for c in identifier_columns)
    mysql_cursor.execute(
        f"SELECT {id_cols_csv}, nd_patient_id FROM patient_mapping_table"
    )
    rows = mysql_cursor.fetchall()

    patient_maps_by_col = {col: {} for col in identifier_columns}
    for row in rows:
        nd_pid = row["nd_patient_id"] if isinstance(row, dict) else row[-1]
        for col in identifier_columns:
            val = row.get(col) if isinstance(row, dict) else None
            if val is not None:
                patient_maps_by_col[col][val] = nd_pid
    return patient_maps_by_col


def run_patient_phase(source_type, source_conn_params, mysql_conn, mysql_cursor, patient_cfg, now, dry_run=False):
    """
    Execute patient SQL → validate identifiers → insert new patients.
    Returns (patient_maps_by_col, new_patient_count).
    patient_maps_by_col is updated in-memory with newly inserted patients
    so child phases can resolve nd_patient_id immediately.
    """
    identifier_columns = patient_cfg["identifier_columns"]
    base_nd_id         = patient_cfg["base_nd_id"]
    first_id_col       = identifier_columns[0]

    ensure_patient_mapping_table(mysql_conn, mysql_cursor, identifier_columns)

    src_data = execute_source_query(source_type, source_conn_params, patient_cfg["sql_file"])
    logging.info(f"[patient] Fetched {len(src_data)} rows from patient SQL.")

    validate_patient_sql_columns(src_data, identifier_columns)
    validate_identifier_uniqueness(src_data, identifier_columns)
    src_data = dedup_patient_source_data(src_data, identifier_columns)
    logging.info(f"[patient] {len(src_data)} unique patients after dedup.")

    patient_maps_by_col = _load_patient_maps_from_db(mysql_cursor, identifier_columns)
    logging.info(f"[patient] Loaded {len(patient_maps_by_col[first_id_col])} existing patients.")

    mysql_cursor.execute(
        f"SELECT COALESCE(MAX(nd_patient_id), {base_nd_id - 1}) AS max_id "
        "FROM patient_mapping_table"
    )
    row = mysql_cursor.fetchone()
    max_patient_id = row["max_id"] if isinstance(row, dict) else row[0]

    new_patients = []

    for row in src_data:
        pid = row.get(first_id_col)
        if pid is None:
            logging.warning("[patient] Row missing primary identifier; skipping.")
            continue
        if pid in patient_maps_by_col[first_id_col]:
            continue  # already mapped

        max_patient_id += 1
        nd_pid = max_patient_id

        for col in identifier_columns:
            val = row.get(col)
            if val is not None:
                patient_maps_by_col[col][val] = nd_pid

        offset = random.choice([
            random.randint(-38, -30),
            random.randint(30, 38),
        ])
        id_values = tuple(row.get(col) for col in identifier_columns)
        new_patients.append(
            (nd_pid,) + id_values + (offset, row.get("registration_date"), now, now)
        )

    logging.info(f"[patient] New patients to insert: {len(new_patients)}")

    if new_patients and not dry_run:
        col_names    = ["nd_patient_id"] + identifier_columns + ["offset", "registration_date", "created_at", "updated_at"]
        cols_str     = ", ".join(f"`{c}`" for c in col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        for i in range(0, len(new_patients), BATCH_SIZE):
            mysql_cursor.executemany(
                f"INSERT INTO patient_mapping_table ({cols_str}) VALUES ({placeholders})",
                new_patients[i:i + BATCH_SIZE],
            )
        mysql_conn.commit()
        logging.info(f"[patient] Inserted {len(new_patients)} new patients.")
    elif dry_run and new_patients:
        logging.info(f"[DRY RUN] [patient] Would insert {len(new_patients)} new patients.")

    return patient_maps_by_col, len(new_patients)


# ─── Child table phase ────────────────────────────────────────────────────────

def _load_child_table_state(mysql_cursor, table_cfg):
    """
    Load existing child mapping table rows into three in-memory structures:
      source_id_map:       {source_id → nd_patient_id}            — all records (for transfer detection)
      source_id_to_nd_id:  {source_id → nd_id_col value}          — active only (for dedup)
      last_nd_per_patient: {nd_patient_id → max nd_id_col value}  — for ID generation
    """
    mapping_table = table_cfg["mapping_table"]
    source_id_col = table_cfg["source_id_col"]
    nd_id_col     = table_cfg["nd_id_col"]

    mysql_cursor.execute(
        f"SELECT nd_patient_id, `{source_id_col}`, `{nd_id_col}`, nd_ActiveFlag "
        f"FROM `{mapping_table}`"
    )
    rows = mysql_cursor.fetchall()
    logging.info(f"[{mapping_table}] Loaded {len(rows)} existing rows.")

    source_id_map       = {}
    source_id_to_nd_id  = {}
    last_nd_per_patient = {}

    for row in rows:
        if isinstance(row, dict):
            nd_pid    = row["nd_patient_id"]
            source_id = row[source_id_col]
            nd_id     = row[nd_id_col]
            active    = row["nd_ActiveFlag"]
        else:
            nd_pid, source_id, nd_id, active = row[0], row[1], row[2], row[3]

        source_id_map[source_id] = nd_pid

        if active == "Y":
            source_id_to_nd_id[source_id] = nd_id

        if nd_id is not None:
            if nd_pid not in last_nd_per_patient or nd_id > last_nd_per_patient[nd_pid]:
                last_nd_per_patient[nd_pid] = nd_id

    return source_id_map, source_id_to_nd_id, last_nd_per_patient


def run_child_table_phase(
    source_type, source_conn_params, mysql_conn, mysql_cursor,
    table_cfg, patient_maps_by_col, now, dry_run=False,
):
    """
    Process one child mapping table: fetch source data, determine new/transfer/skip,
    and write changes to MySQL.
    """
    mapping_table    = table_cfg["mapping_table"]
    source_id_col    = table_cfg["source_id_col"]
    nd_id_col        = table_cfg["nd_id_col"]
    date_col         = table_cfg["date_col"]
    patient_ref_cols = table_cfg["patient_ref_cols"]
    suffix_len       = table_cfg["nd_id_suffix_len"]
    use_len_guard    = table_cfg.get("use_len_guard", False)

    src_data = execute_source_query(source_type, source_conn_params, table_cfg["sql_file"])
    logging.info(f"[{mapping_table}] Fetched {len(src_data)} rows from source SQL.")

    validate_child_sql_columns(src_data, table_cfg)
    src_data = dedup_child_source_data(src_data, source_id_col)
    logging.info(f"[{mapping_table}] {len(src_data)} unique rows after dedup.")
    ensure_child_table(mysql_conn, mysql_cursor, table_cfg)

    source_id_map, source_id_to_nd_id, last_nd_per_patient = _load_child_table_state(
        mysql_cursor, table_cfg
    )

    new_rows        = []
    deactivate_list = []
    transferred_ids = set()

    for row in src_data:
        source_id = row.get(source_id_col)
        if source_id is None:
            continue

        if source_id in transferred_ids:
            logging.warning(
                f"[{mapping_table}] {source_id_col}={source_id} already transferred "
                "in this run; skipping duplicate."
            )
            continue

        nd_pid = resolve_nd_patient_id(row, patient_maps_by_col, patient_ref_cols)
        if nd_pid is None:
            logging.warning(
                f"[{mapping_table}] {source_id_col}={source_id} has no matching patient; skipping."
            )
            continue

        existing_nd_pid = source_id_map.get(source_id)

        if existing_nd_pid is not None and existing_nd_pid != nd_pid:
            # Transfer: this source ID now belongs to a different patient
            logging.info(
                f"[{mapping_table}] {source_id_col}={source_id} transferring "
                f"nd_patient_id {existing_nd_pid} → {nd_pid}. Marking old record inactive."
            )
            deactivate_list.append((existing_nd_pid, source_id))
            source_id_map.pop(source_id, None)
            source_id_to_nd_id.pop(source_id, None)
            transferred_ids.add(source_id)

        elif source_id in source_id_to_nd_id:
            continue  # already mapped and active — skip

        # New record (either truly new, or just transferred from another patient)
        last_nd = last_nd_per_patient.get(nd_pid)
        nd_id   = generate_nd_id(nd_pid, last_nd, suffix_len, use_len_guard)

        last_nd_per_patient[nd_pid]  = nd_id
        source_id_map[source_id]     = nd_pid
        source_id_to_nd_id[source_id] = nd_id

        new_rows.append((nd_pid, source_id, nd_id, row.get(date_col), now, now))

    logging.info(
        f"[{mapping_table}] New rows: {len(new_rows)}, "
        f"Transfers to deactivate: {len(deactivate_list)}"
    )

    if deactivate_list and not dry_run:
        for i in range(0, len(deactivate_list), BATCH_SIZE):
            batch       = deactivate_list[i:i + BATCH_SIZE]
            where_parts = " OR ".join(
                [f"(nd_patient_id = %s AND `{source_id_col}` = %s)"] * len(batch)
            )
            params = [val for pair in batch for val in pair]
            mysql_cursor.execute(
                f"UPDATE `{mapping_table}` "
                f"SET nd_ActiveFlag = 'N', updated_at = %s "
                f"WHERE {where_parts}",
                [now] + params,
            )
            logging.info(
                f"[{mapping_table}] Deactivated batch {i // BATCH_SIZE + 1}: {len(batch)} records."
            )
        mysql_conn.commit()
    elif dry_run and deactivate_list:
        logging.info(f"[DRY RUN] [{mapping_table}] Would deactivate {len(deactivate_list)} records.")

    if new_rows and not dry_run:
        insert_sql = (
            f"INSERT INTO `{mapping_table}` "
            f"(nd_patient_id, `{source_id_col}`, `{nd_id_col}`, `{date_col}`, "
            f"created_at, updated_at, nd_ActiveFlag) "
            f"VALUES (%s, %s, %s, %s, %s, %s, 'Y')"
        )
        for i in range(0, len(new_rows), BATCH_SIZE):
            mysql_cursor.executemany(insert_sql, new_rows[i:i + BATCH_SIZE])
        mysql_conn.commit()
        logging.info(f"[{mapping_table}] Inserted {len(new_rows)} new rows.")
    elif dry_run and new_rows:
        logging.info(f"[DRY RUN] [{mapping_table}] Would insert {len(new_rows)} new rows.")

    return {
        "new_rows":   len(new_rows),
        "deactivated": len(deactivate_list),
    }


# ─── Top-level ETL ────────────────────────────────────────────────────────────

def run_etl(config, source_conn_params, mysql_conn_params, dry_run=False):
    now         = datetime.now(IST).replace(tzinfo=None)
    source_type = config["source_type"]

    mysql_conn   = mysql.connector.connect(**mysql_conn_params)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    try:
        patient_maps_by_col, new_patients = run_patient_phase(
            source_type, source_conn_params, mysql_conn, mysql_cursor,
            config["patient"], now, dry_run,
        )

        child_stats = {}
        for table_cfg in config.get("child_tables", []):
            stats = run_child_table_phase(
                source_type, source_conn_params, mysql_conn, mysql_cursor,
                table_cfg, patient_maps_by_col, now, dry_run,
            )
            child_stats[table_cfg["name"]] = stats

        return {
            "success":      True,
            "new_patients": new_patients,
            "child_tables": child_stats,
        }

    finally:
        mysql_cursor.close()
        mysql_conn.close()


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="V3 Incremental Mapping Update")
    parser.add_argument("--config", required=True, help="Path to YAML mapping config file")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without writing to DB")
    args = parser.parse_args()

    config = load_config(args.config)

    from db_config import STATE_DIR, V3_MAPPING_SOURCE_CONN, V3_MAPPING_MYSQL_CONN

    state_dir = os.path.join(STATE_DIR, "mapping_master_v3")
    setup_logging(state_dir)

    state_manager = MappingStateManager(state_dir)
    state         = state_manager.load()
    today         = datetime.now(IST).date().isoformat()

    if state.get("last_processed_date") == today and state.get("success") and not args.dry_run:
        logging.info("V3 mapping already ran successfully today. Skipping.")
        return

    logging.info("Starting V3 Mapping Update ETL")

    try:
        result = run_etl(config, V3_MAPPING_SOURCE_CONN, V3_MAPPING_MYSQL_CONN, args.dry_run)
        state  = {"last_processed_date": today, **result}
        state_manager.save(state)
        logging.info(f"V3 Mapping Update completed: {result}")

    except Exception as e:
        logging.exception("V3 Mapping Update failed")
        state_manager.save({"last_processed_date": today, "success": False, "error": str(e)})
        raise


if __name__ == "__main__":
    main()
