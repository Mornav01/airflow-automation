#!/usr/bin/env python3
"""
active_flag_merge.py — SCD-2 (nd_active_flag) maintenance for the AthenaOne
historical schema.

AthenaOne's merge differs from the other EHRs (which use state_diff_merge_v3):
it keeps every version of a record in the historical schema and marks exactly
one row per primary key as the current version via ``nd_active_flag = 'Y'``.
This module ports the three correctness-critical steps from the reference repo's
services/merge_service.py, decoupled from Django/services.config:

    ensure_indexes()        — PK / active / norm / unique(nd_auto_increment_id)
                              indexes on both schemas (speeds the joins below)
    set_historical_flags()  — before the insert: deactivate ('N') the historical
                              rows whose PK reappears in the incremental pull
    finalize_active_flags() — after the insert: validate one active row per PK,
                              and fix any table that has more than one by electing
                              MAX(nd_auto_increment_id) per PK as the winner ('Y')

nd_auto_increment_id is assigned in the incremental schema by update_nd_auto_inc_id.py
(step 9, continuing from the historical MAX) and copied across by merging.py, so
the newest version of any PK always has the highest id — that is the row elected
active.

Primary keys come from scripts/athenaone/table_primary_keys.csv
(``table_name,primary_key`` with pipe-separated composite keys).
"""
from __future__ import annotations

import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from config_loader import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    INCREMENTAL_SCHEMA,
    HISTORICAL_SCHEMA,
)

CONN_STR = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"

ENSURE_IDX_MAX_WORKERS = 8
SET_FLAGS_MAX_WORKERS = 8
VALIDATION_MAX_WORKERS = 8
FIX_MAX_WORKERS = 8

TEMP_PK_TABLE_PREFIX = "tmp_incr_pks_"
FIX_TMP_PREFIX = "tmp_fix_"
TEXT_PREFIX_LEN = 255

TABLE_PRIMARY_KEYS_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "table_primary_keys.csv"
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _q(name: str) -> str:
    return f"`{name}`"


def _load_primary_keys_csv() -> dict:
    """Load table_primary_keys.csv into {table_name: [pk_col1, pk_col2, ...]}."""
    out: dict[str, list[str]] = {}
    if not os.path.isfile(TABLE_PRIMARY_KEYS_CSV):
        raise RuntimeError(
            f"table_primary_keys.csv not found at {TABLE_PRIMARY_KEYS_CSV} — "
            f"required for the AthenaOne active-flag merge."
        )
    with open(TABLE_PRIMARY_KEYS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tname = (row.get("table_name") or "").strip()
            pk_raw = (row.get("primary_key") or "").strip()
            if tname and pk_raw:
                out[tname] = [c.strip() for c in pk_raw.split("|") if c.strip()]
    return out


def _pk_for(pk_map: dict, table: str):
    return pk_map.get(table) or pk_map.get(table.upper())


def _get_columns(engine, schema: str, table: str) -> list:
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table, schema=schema)]


def _build_index_columns(conn, schema: str, table: str, cols: list) -> list:
    """Return index column specs for CREATE INDEX; TEXT/BLOB get a prefix length."""
    if not cols:
        return []
    rows = conn.execute(
        text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t AND column_name IN :c
        """),
        {"s": schema, "t": table, "c": tuple(cols)},
    ).fetchall()
    text_types = {"text", "blob", "mediumtext", "longtext"}
    text_cols = {r[0] for r in rows if (r[1] or "").lower() in text_types}
    return [f"`{c}`({TEXT_PREFIX_LEN})" if c in text_cols else f"`{c}`" for c in cols]


# ══════════════════════════════════════════════════════════════════════════════
# ensure_indexes
# ══════════════════════════════════════════════════════════════════════════════
def _idx_exists(conn, schema: str, table: str, idx: str) -> bool:
    return conn.execute(text("""
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = :s AND table_name = :t AND index_name = :i LIMIT 1
    """), {"s": schema, "t": table, "i": idx}).scalar() is not None


def _uq_col_exists(conn, schema: str, table: str, col: str) -> bool:
    return conn.execute(text("""
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = :s AND table_name = :t AND column_name = :c AND non_unique = 0 LIMIT 1
    """), {"s": schema, "t": table, "c": col}).scalar() is not None


def _col_names_lower(conn, schema: str, table: str) -> set:
    rows = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()
    return {r[0].lower() for r in rows}


def _ensure_indexes_one_table(engine, schema: str, table: str, pk_cols: list) -> dict:
    """Create idx_pk, idx_active, idx_norm and uq_auto_inc on one table."""
    fqn = f"{_q(schema)}.{_q(table)}"
    logs = []
    try:
        with engine.begin() as conn:
            col_lower = _col_names_lower(conn, schema, table)
            has_active = "nd_active_flag" in col_lower
            has_auto_inc = "nd_auto_increment_id" in col_lower

            if pk_cols:
                idx_pk = f"idx_{table}_pk"[:64]
                if not _idx_exists(conn, schema, table, idx_pk):
                    specs = _build_index_columns(conn, schema, table, pk_cols)
                    if specs:
                        conn.execute(text(f"CREATE INDEX `{idx_pk}` ON {fqn} ({', '.join(specs)})"))
                        logs.append(f"created {idx_pk}")

            if has_active:
                idx_active = f"idx_{table}_active"[:64]
                if not _idx_exists(conn, schema, table, idx_active):
                    conn.execute(text(f"CREATE INDEX `{idx_active}` ON {fqn} (`nd_active_flag`)"))
                    logs.append(f"created {idx_active}")

            if pk_cols and has_auto_inc:
                idx_norm = f"idx_{table}_norm"[:64]
                if not _idx_exists(conn, schema, table, idx_norm):
                    specs = _build_index_columns(conn, schema, table, pk_cols + ["nd_auto_increment_id"])
                    if specs:
                        conn.execute(text(f"CREATE INDEX `{idx_norm}` ON {fqn} ({', '.join(specs)})"))
                        logs.append(f"created {idx_norm}")

            if has_auto_inc and not _uq_col_exists(conn, schema, table, "nd_auto_increment_id"):
                uq = f"uq_{table}_auto_inc"[:64]
                conn.execute(text(f"ALTER TABLE {fqn} ADD UNIQUE KEY `{uq}` (`nd_auto_increment_id`)"))
                logs.append(f"created {uq}")

        return {"table": table, "schema": schema, "status": "SUCCESS", "created": logs}
    except SQLAlchemyError as e:
        return {"table": table, "schema": schema, "status": "FAILED", "error": str(e)}


def ensure_indexes() -> dict:
    """Create PK / active / norm / unique(nd_auto_increment_id) indexes on both
    HISTORICAL_SCHEMA and INCREMENTAL_SCHEMA. Index failures are logged but do
    not abort — they only affect performance, not correctness."""
    engine = create_engine(
        CONN_STR, pool_pre_ping=True, pool_size=ENSURE_IDX_MAX_WORKERS, max_overflow=2
    )
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()

    tasks = []
    for schema in (HISTORICAL_SCHEMA, INCREMENTAL_SCHEMA):
        for table in inspector.get_table_names(schema=schema):
            tasks.append((schema, table, _pk_for(pk_map, table) or []))

    results = []
    with ThreadPoolExecutor(max_workers=ENSURE_IDX_MAX_WORKERS) as ex:
        futs = {ex.submit(_ensure_indexes_one_table, engine, s, t, pk): (s, t) for s, t, pk in tasks}
        for fut in as_completed(futs):
            s, t = futs[fut]
            try:
                results.append(fut.result())
            except Exception as e:  # noqa: BLE001
                results.append({"table": t, "schema": s, "status": "FAILED", "error": str(e)})

    engine.dispose()
    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    failed = sum(1 for r in results if r.get("status") == "FAILED")
    _log(f"ensure_indexes: {len(results)} tables · success={success} · failed={failed}")
    for r in results:
        if r.get("status") == "FAILED":
            _log(f"  ⚠️  index FAILED {r['schema']}.{r['table']}: {r.get('error')}")
    return {"total_tables": len(results), "success": success, "failed": failed}


# ══════════════════════════════════════════════════════════════════════════════
# set_historical_flags — deactivate superseded historical rows (pre-insert)
# ══════════════════════════════════════════════════════════════════════════════
def _pk_table_name_for(table_name: str) -> str:
    name = f"{TEMP_PK_TABLE_PREFIX}{table_name}"
    return name[:64]


def _set_flags_one_table(table_name, engine, pk_cols, hist_cols, hist_fqn, incr_fqn) -> dict:
    """If incremental has rows: stage its PKs, index them, UPDATE historical
    SET nd_active_flag = 'N' via JOIN, then drop the staging table."""
    try:
        with engine.connect() as conn:
            incr_count = conn.execute(text(f"SELECT COUNT(*) FROM {incr_fqn}")).scalar() or 0
        if incr_count == 0:
            return {"table": table_name, "status": "SKIPPED", "error": "incremental has 0 rows"}
    except SQLAlchemyError as e:
        return {"table": table_name, "status": "FAILED", "error": f"count check: {type(e).__name__}: {e}"}

    pk_select = ", ".join(_q(c) for c in pk_cols)
    pk_join = " AND ".join(f"h.{_q(c)} = t.{_q(c)}" for c in pk_cols)
    pk_table_fqn = f"{_q(INCREMENTAL_SCHEMA)}.{_q(_pk_table_name_for(table_name))}"
    try:
        with engine.begin() as conn:
            if "nd_active_flag" not in hist_cols:
                conn.execute(text(f"ALTER TABLE {hist_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'Y'"))
            conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
            conn.execute(text(f"CREATE TABLE {pk_table_fqn} AS SELECT {pk_select} FROM {incr_fqn}"))
            specs = _build_index_columns(conn, INCREMENTAL_SCHEMA, table_name, pk_cols)
            if specs:
                conn.execute(text(f"CREATE INDEX idx_pk ON {pk_table_fqn} ({', '.join(specs)})"))
            conn.execute(text(f"""
                UPDATE {hist_fqn} h
                INNER JOIN {pk_table_fqn} t ON {pk_join}
                SET h.`nd_active_flag` = 'N'
            """))
            conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
        return {"table": table_name, "status": "SUCCESS"}
    except SQLAlchemyError as e:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
        except SQLAlchemyError:
            pass
        return {"table": table_name, "status": "FAILED", "error": f"{type(e).__name__}: {e}"}


def set_historical_flags() -> dict:
    """For each table in BOTH schemas (with a PK and rows in incremental): set
    nd_active_flag = 'N' on the historical rows whose PK reappears in the pull.
    Run BEFORE merging.py so the freshly inserted rows keep their default 'Y'.
    Raises if any table fails — this is correctness-critical."""
    engine = create_engine(
        CONN_STR, pool_pre_ping=True, pool_size=SET_FLAGS_MAX_WORKERS, max_overflow=2
    )
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    hist_lower = {t.lower() for t in inspector.get_table_names(schema=HISTORICAL_SCHEMA)}
    incr_tables = inspector.get_table_names(schema=INCREMENTAL_SCHEMA)
    common = sorted(t for t in incr_tables if t.lower() in hist_lower)

    results = []
    tasks = []
    for table_name in common:
        pk_cols = _pk_for(pk_map, table_name)
        if not pk_cols:
            results.append({"table": table_name, "status": "SKIPPED", "error": "No PK in CSV"})
            continue
        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        incr_cols = _get_columns(engine, INCREMENTAL_SCHEMA, table_name)
        if not all(pc in hist_cols for pc in pk_cols) or not all(pc in incr_cols for pc in pk_cols):
            results.append({"table": table_name, "status": "SKIPPED", "error": "PK cols missing in a schema"})
            continue
        hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
        incr_fqn = f"{_q(INCREMENTAL_SCHEMA)}.{_q(table_name)}"
        tasks.append((table_name, pk_cols, hist_cols, hist_fqn, incr_fqn))

    with ThreadPoolExecutor(max_workers=SET_FLAGS_MAX_WORKERS) as ex:
        futs = {
            ex.submit(_set_flags_one_table, t, engine, pk, hc, hf, incf): t
            for (t, pk, hc, hf, incf) in tasks
        }
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                results.append(fut.result())
            except Exception as e:  # noqa: BLE001
                results.append({"table": t, "status": "FAILED", "error": str(e)})

    engine.dispose()
    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    skipped = sum(1 for r in results if r.get("status") == "SKIPPED")
    failed = [r for r in results if r.get("status") == "FAILED"]
    _log(f"set_historical_flags: {len(results)} tables · success={success} · skipped={skipped} · failed={len(failed)}")
    if failed:
        detail = "; ".join(f"{r['table']}: {r.get('error')}" for r in failed)
        raise RuntimeError(f"set_historical_flags failed for {len(failed)} table(s): {detail}")
    return {"total_tables": len(results), "success": success, "skipped": skipped, "failed": 0}


# ══════════════════════════════════════════════════════════════════════════════
# finalize_active_flags — validate one active per PK, fix the ones that aren't
# ══════════════════════════════════════════════════════════════════════════════
def _validate_one_table(table_name, engine, pk_cols) -> dict:
    """Pass if COUNT(DISTINCT pk) == COUNT(*) WHERE nd_active_flag = 'Y'."""
    hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
    distinct_part = ", ".join(_q(c) for c in pk_cols)
    sql = f"""
        SELECT COUNT(DISTINCT {distinct_part}) AS distinct_pk,
               SUM(CASE WHEN `nd_active_flag` = 'Y' THEN 1 ELSE 0 END) AS active_count
        FROM {hist_fqn}
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql)).fetchone()
        distinct_pk = (row[0] or 0) if row else 0
        active = (row[1] or 0) if row and len(row) > 1 else 0
        if distinct_pk == active:
            return {"table": table_name, "status": "SUCCESS"}
        return {
            "table": table_name, "status": "FAILED",
            "active_row_count": active, "distinct_pk_count": distinct_pk,
            "reason": f"active={active} != distinct_pk={distinct_pk}",
        }
    except SQLAlchemyError as e:
        return {"table": table_name, "status": "FAILED", "error": str(e)}


def _fix_one_table(table_name, engine, pk_cols) -> dict:
    """Elect MAX(nd_auto_increment_id) per PK as the single active ('Y') row."""
    hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
    tmp_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(f'{FIX_TMP_PREFIX}{table_name}'[:64])}"
    pk_part = ", ".join(_q(c) for c in pk_cols)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {tmp_fqn}"))
            conn.execute(text(f"""
                CREATE TABLE {tmp_fqn} AS
                SELECT MAX(`nd_auto_increment_id`) AS winner_id
                FROM {hist_fqn} GROUP BY {pk_part}
            """))
            conn.execute(text(f"""
                UPDATE {hist_fqn} h
                LEFT JOIN {tmp_fqn} w ON h.`nd_auto_increment_id` = w.winner_id
                SET h.`nd_active_flag` = CASE WHEN w.winner_id IS NOT NULL THEN 'Y' ELSE 'N' END
            """))
            conn.execute(text(f"DROP TABLE IF EXISTS {tmp_fqn}"))
        return {"table": table_name, "status": "SUCCESS"}
    except SQLAlchemyError as e:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {tmp_fqn}"))
        except SQLAlchemyError:
            pass
        return {"table": table_name, "status": "FAILED", "error": f"{type(e).__name__}: {e}"}


def finalize_active_flags() -> dict:
    """Run AFTER merging.py. Validate one active row per PK across the tables the
    pull touched; fix (elect MAX(nd_auto_increment_id) per PK) any that fail, then
    re-validate. Raises if any table still violates the invariant."""
    engine = create_engine(
        CONN_STR, pool_pre_ping=True, pool_size=max(VALIDATION_MAX_WORKERS, FIX_MAX_WORKERS), max_overflow=2
    )
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    hist_tables = inspector.get_table_names(schema=HISTORICAL_SCHEMA)
    incr_lower = {t.lower() for t in inspector.get_table_names(schema=INCREMENTAL_SCHEMA)}
    # Only the tables the pull touched (present in both schemas).
    candidates = [t for t in hist_tables if t.lower() in incr_lower]

    to_check = []
    skipped = 0
    for table_name in candidates:
        pk_cols = _pk_for(pk_map, table_name)
        if not pk_cols:
            skipped += 1
            continue
        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        if "nd_active_flag" not in hist_cols or not all(pc in hist_cols for pc in pk_cols):
            skipped += 1
            continue
        to_check.append((table_name, pk_cols))

    # Validate
    failed = []
    with ThreadPoolExecutor(max_workers=VALIDATION_MAX_WORKERS) as ex:
        futs = {ex.submit(_validate_one_table, t, engine, pk): t for t, pk in to_check}
        for fut in as_completed(futs):
            res = fut.result()
            if res.get("status") == "FAILED":
                failed.append(res)
    _log(f"finalize_active_flags: validated {len(to_check)} tables · skipped={skipped} · need_fix={len(failed)}")

    # Fix the failed ones
    pk_by_table = dict(to_check)
    fix_failed = []
    if failed:
        with ThreadPoolExecutor(max_workers=FIX_MAX_WORKERS) as ex:
            futs = {
                ex.submit(_fix_one_table, r["table"], engine, pk_by_table[r["table"]]): r["table"]
                for r in failed if r["table"] in pk_by_table
            }
            for fut in as_completed(futs):
                res = fut.result()
                if res.get("status") != "SUCCESS":
                    fix_failed.append(res)
                else:
                    _log(f"  fixed one-active-per-PK: {res['table']}")

    # Re-validate the tables we fixed
    still_bad = []
    if failed and not fix_failed:
        recheck = [(r["table"], pk_by_table[r["table"]]) for r in failed if r["table"] in pk_by_table]
        with ThreadPoolExecutor(max_workers=VALIDATION_MAX_WORKERS) as ex:
            futs = {ex.submit(_validate_one_table, t, engine, pk): t for t, pk in recheck}
            for fut in as_completed(futs):
                res = fut.result()
                if res.get("status") == "FAILED":
                    still_bad.append(res)

    engine.dispose()

    if fix_failed:
        detail = "; ".join(f"{r['table']}: {r.get('error')}" for r in fix_failed)
        raise RuntimeError(f"finalize_active_flags: fix failed for {len(fix_failed)} table(s): {detail}")
    if still_bad:
        detail = "; ".join(f"{r['table']}: {r.get('reason') or r.get('error')}" for r in still_bad)
        raise RuntimeError(f"finalize_active_flags: still >1 active per PK after fix: {detail}")

    _log(f"finalize_active_flags: OK · fixed={len(failed)}")
    return {"validated": len(to_check), "skipped": skipped, "fixed": len(failed)}
