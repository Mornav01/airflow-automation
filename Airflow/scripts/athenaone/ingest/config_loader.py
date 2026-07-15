#!/usr/bin/env python3
"""
config_loader — env-driven configuration for the AthenaOne Snowflake→MySQL
ingestion step scripts.

This replaces the Django ``SchedulerConfig``-backed loader from the
``athenaone_airflow_automation`` reference repo.  The ingestion step modules
(snowflake_to_mysql_extraction.py, merging.py, …) import flat module-level
names from here — e.g. ``from config_loader import MYSQL_USER, ...`` — so this
module exposes exactly those names, sourced entirely from the process
environment (populated from Airflow/.env by dags/shared/__init__.py, then
inherited by the ingestion subprocess).

No secrets live in this file.  Credentials come from the environment only.
Passwords are URL-encoded here so plain values can be stored in .env, matching
the rest of the repo's DB_* / SOURCE_DB_* convention.
"""
from __future__ import annotations

import os
from urllib.parse import quote as _url_quote


def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(
            f"Required env var {key!r} is not set — configure it in Airflow/.env "
            f"(see the ATHENAONE INGESTION section in Airflow/.env.example)."
        )
    return val


def _first(*keys: str) -> str:
    """Return the first env var that is set among *keys* (or raise)."""
    for k in keys:
        v = os.environ.get(k)
        if v:
            return v
    raise RuntimeError(
        f"None of these env vars are set: {', '.join(keys)} — configure one in Airflow/.env."
    )


def _int(key: str, default: int) -> int:
    val = os.environ.get(key)
    return int(val) if val not in (None, "") else default


def _opt(key: str) -> str | None:
    val = os.environ.get(key)
    return val if val not in (None, "") else None


def _list_or_none(key: str) -> list | None:
    val = os.environ.get(key)
    if not val:
        return None
    return [v.strip() for v in val.split(",") if v.strip()]


# ── Snowflake source ──────────────────────────────────────────────────────────
SNOWFLAKE_USER          = _require("ATHENAONE_SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD      = _url_quote(_require("ATHENAONE_SNOWFLAKE_PASSWORD"), safe="")
SNOWFLAKE_ACCOUNT       = _require("ATHENAONE_SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE      = os.environ.get("ATHENAONE_SNOWFLAKE_DATABASE", "ATHENAHEALTH")
SNOWFLAKE_WAREHOUSE     = os.environ.get("ATHENAONE_SNOWFLAKE_WAREHOUSE", "AH_WAREHOUSE")
SNOWFLAKE_INSECURE_MODE = os.environ.get("ATHENAONE_SNOWFLAKE_INSECURE_MODE", "true").strip().lower() in ("1", "true", "yes")

# ── MySQL destination (the ingested schema) ────────────────────────────────────
# Falls back to the repo-wide MySQL write family (DB_*) when athenaone-specific
# overrides are not provided.
MYSQL_USER     = _first("ATHENAONE_MYSQL_USER", "DB_USER")
MYSQL_PASSWORD = _url_quote(_first("ATHENAONE_MYSQL_PASSWORD", "DB_PASS"), safe="")
MYSQL_HOST     = _first("ATHENAONE_MYSQL_HOST", "DB_HOST")

# ── Schemas ─────────────────────────────────────────────────────────────────
# INCREMENTAL_SCHEMA is dropped+recreated each run; merged into HISTORICAL_SCHEMA.
INCREMENTAL_SCHEMA = _require("ATHENAONE_INCREMENTAL_SCHEMA")
HISTORICAL_SCHEMA  = _require("ATHENAONE_HISTORICAL_SCHEMA")

# ── Extraction settings ───────────────────────────────────────────────────────
_ctx = _list_or_none("ATHENAONE_CONTEXT_IDS") or ["1", "23649"]
CONTEXT_IDS = tuple(int(c) for c in _ctx)
BATCH_SIZE  = _int("ATHENAONE_INGEST_BATCH_SIZE", 10000)
MAX_THREADS = _int("ATHENAONE_INGEST_MAX_THREADS", 10)
TEST_TABLES = _list_or_none("ATHENAONE_TEST_TABLES")

# ── Date window (None ⇒ default "yesterday" incremental in the step scripts) ────
EXTRACTION_DATE = _opt("ATHENAONE_EXTRACTION_DATE")
FROM_DATE       = _opt("ATHENAONE_FROM_DATE")
TO_DATE         = _opt("ATHENAONE_TO_DATE")
