"""
Tests for mssql_incremental_mapping_update_v3.py

Unit tests cover pure functions without any DB/network calls.
Integration tests mock pymssql and mysql.connector to test full phases.

Run: pytest tests/test_mssql_incremental_mapping_update_v3.py -v
"""

import sys
import os
import io
import json
import pytest
import tempfile
import yaml
from unittest.mock import MagicMock, patch, call
from datetime import datetime

# ── Stub out C-extension DB drivers so tests run without them installed ───────
sys.modules.setdefault("pymssql", MagicMock())
sys.modules.setdefault("mysql", MagicMock())
sys.modules.setdefault("mysql.connector", MagicMock())

# ── Import the module under test ──────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Airflow", "scripts"))

from mssql_incremental_mapping_update_v3 import (
    load_config,
    validate_identifier_uniqueness,
    validate_patient_sql_columns,
    validate_child_sql_columns,
    resolve_nd_patient_id,
    generate_nd_id,
    build_create_table_ddl,
    ensure_patient_mapping_table,
    run_patient_phase,
    run_child_table_phase,
    _load_patient_maps_from_db,
    _load_child_table_state,
    MappingStateManager,
)

NOW = datetime(2024, 1, 15, 10, 0, 0)


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

MINIMAL_CONFIG = {
    "patient": {
        "sql_file": "/fake/patient.sql",
        "identifier_columns": ["patientid", "profileid", "pid"],
        "base_nd_id": 100000000000000,
    },
    "child_tables": [
        {
            "name": "encounter",
            "sql_file": "/fake/encounter.sql",
            "patient_ref_cols": ["patientid"],
            "source_id_col": "encounter_id",
            "date_col": "encounter_date",
            "nd_id_suffix_len": 4,
            "mapping_table": "encounter_mapping_table",
            "nd_id_col": "nd_encounter_id",
        },
        {
            "name": "chart",
            "sql_file": "/fake/chart.sql",
            "patient_ref_cols": ["patientid", "profileid", "pid"],
            "source_id_col": "chart_id",
            "date_col": "chart_date",
            "nd_id_suffix_len": 5,
            "mapping_table": "chart_mapping_table",
            "nd_id_col": "nd_chart_id",
            "use_len_guard": True,
        },
    ],
}


def make_mysql_mock(fetchone_values=None, fetchall_values=None):
    """Return (mock_conn, mock_cursor) with pre-configured return values."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = fetchone_values or [{"cnt": 1}]
    cursor.fetchall.side_effect = fetchall_values or [[]]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ══════════════════════════════════════════════════════════════════════════════
# load_config
# ══════════════════════════════════════════════════════════════════════════════

def test_load_config_valid(tmp_path):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(MINIMAL_CONFIG))
    result = load_config(str(cfg_file))
    assert result["patient"]["identifier_columns"] == ["patientid", "profileid", "pid"]
    assert len(result["child_tables"]) == 2


def test_load_config_missing_top_level_key(tmp_path):
    bad = {"patient": MINIMAL_CONFIG["patient"]}   # no child_tables
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="child_tables"):
        load_config(str(cfg_file))


def test_load_config_missing_patient_key(tmp_path):
    bad = {
        "patient": {"identifier_columns": ["patientid"], "base_nd_id": 1},  # no sql_file
        "child_tables": [],
    }
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="sql_file"):
        load_config(str(cfg_file))


def test_load_config_missing_child_key(tmp_path):
    bad_child = dict(MINIMAL_CONFIG["child_tables"][0])
    bad_child.pop("mapping_table")
    bad = {"patient": MINIMAL_CONFIG["patient"], "child_tables": [bad_child]}
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="mapping_table"):
        load_config(str(cfg_file))


def test_load_config_empty_identifier_columns(tmp_path):
    bad = {
        "patient": {"sql_file": "/f.sql", "identifier_columns": [], "base_nd_id": 1},
        "child_tables": [],
    }
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="identifier_columns"):
        load_config(str(cfg_file))


# ══════════════════════════════════════════════════════════════════════════════
# validate_identifier_uniqueness
# ══════════════════════════════════════════════════════════════════════════════

def test_validate_identifiers_pass():
    src = [
        {"patientid": 1, "profileid": 10, "pid": 100, "encounter_id": "E1"},
        {"patientid": 1, "profileid": 10, "pid": 100, "encounter_id": "E2"},
        {"patientid": 2, "profileid": 20, "pid": 200, "encounter_id": "E3"},
    ]
    validate_identifier_uniqueness(src, ["patientid", "profileid", "pid"])  # no exception


def test_validate_identifiers_fail_mismatch():
    src = [
        {"patientid": 1, "profileid": 10, "pid": 100},
        {"patientid": 2, "profileid": 10, "pid": 200},  # profileid=10 shared → not 1:1
    ]
    with pytest.raises(ValueError, match="NOT 1:1"):
        validate_identifier_uniqueness(src, ["patientid", "profileid", "pid"])


def test_validate_identifiers_null_values_handled():
    src = [
        {"patientid": 1, "profileid": None, "pid": 100},
        {"patientid": 2, "profileid": None, "pid": 200},
    ]
    # NULLs are excluded from distinct counts → counts for non-null cols may differ.
    # This is expected to raise since profileid count (0) != patientid count (2).
    with pytest.raises(ValueError):
        validate_identifier_uniqueness(src, ["patientid", "profileid", "pid"])


def test_validate_identifiers_single_column():
    src = [{"patientid": 1}, {"patientid": 2}, {"patientid": 1}]
    validate_identifier_uniqueness(src, ["patientid"])  # single col always 1:1 with itself


# ══════════════════════════════════════════════════════════════════════════════
# validate_patient_sql_columns
# ══════════════════════════════════════════════════════════════════════════════

def test_validate_patient_sql_columns_pass():
    src = [{"patientid": 1, "profileid": 10, "pid": 100, "registration_date": None}]
    validate_patient_sql_columns(src, ["patientid", "profileid", "pid"])  # no error


def test_validate_patient_sql_columns_missing_identifier():
    src = [{"patientid": 1, "registration_date": None}]  # profileid and pid missing
    with pytest.raises(ValueError, match="missing columns"):
        validate_patient_sql_columns(src, ["patientid", "profileid", "pid"])


def test_validate_patient_sql_columns_missing_registration_date():
    src = [{"patientid": 1, "profileid": 10, "pid": 100}]  # no registration_date
    with pytest.raises(ValueError, match="registration_date"):
        validate_patient_sql_columns(src, ["patientid", "profileid", "pid"])


def test_validate_patient_sql_columns_empty_src_no_error():
    validate_patient_sql_columns([], ["patientid", "profileid", "pid"])  # warns, no error


# ══════════════════════════════════════════════════════════════════════════════
# validate_child_sql_columns
# ══════════════════════════════════════════════════════════════════════════════

_CHILD_CFG = {
    "name": "encounter",
    "mapping_table": "encounter_mapping_table",
    "source_id_col": "encounter_id",
    "date_col": "encounter_date",
    "patient_ref_cols": ["patientid", "profileid"],
}

def test_validate_child_sql_columns_pass():
    src = [{"encounter_id": "E1", "patientid": 1, "encounter_date": None}]
    validate_child_sql_columns(src, _CHILD_CFG)  # no error


def test_validate_child_sql_columns_missing_source_id():
    src = [{"patientid": 1, "encounter_date": None}]  # no encounter_id
    with pytest.raises(ValueError, match="encounter_id"):
        validate_child_sql_columns(src, _CHILD_CFG)


def test_validate_child_sql_columns_missing_date_col():
    src = [{"encounter_id": "E1", "patientid": 1}]  # no encounter_date
    with pytest.raises(ValueError, match="encounter_date"):
        validate_child_sql_columns(src, _CHILD_CFG)


def test_validate_child_sql_columns_no_ref_cols_in_result():
    src = [{"encounter_id": "E1", "encounter_date": None}]  # no patientid or profileid
    with pytest.raises(ValueError, match="patient_ref_cols"):
        validate_child_sql_columns(src, _CHILD_CFG)


def test_validate_child_sql_columns_partial_ref_cols_warns():
    # Only patientid present (not profileid) — should warn but not raise
    src = [{"encounter_id": "E1", "patientid": 1, "encounter_date": None}]
    validate_child_sql_columns(src, _CHILD_CFG)  # no error


def test_validate_child_sql_columns_empty_src_no_error():
    validate_child_sql_columns([], _CHILD_CFG)  # warns, no error


# ══════════════════════════════════════════════════════════════════════════════
# resolve_nd_patient_id
# ══════════════════════════════════════════════════════════════════════════════

def test_resolve_first_col_match():
    maps = {"patientid": {1: 100000000000001}, "profileid": {10: 100000000000001}}
    row  = {"patientid": 1, "encounter_id": "E1"}
    assert resolve_nd_patient_id(row, maps, ["patientid", "profileid"]) == 100000000000001


def test_resolve_fallback_to_second_col():
    maps = {"patientid": {}, "profileid": {10: 100000000000002}}
    row  = {"patientid": 999, "profileid": 10}   # patientid=999 not in map
    assert resolve_nd_patient_id(row, maps, ["patientid", "profileid"]) == 100000000000002


def test_resolve_first_col_null_falls_back():
    maps = {"patientid": {1: 100000000000001}, "profileid": {10: 100000000000001}}
    row  = {"patientid": None, "profileid": 10}
    assert resolve_nd_patient_id(row, maps, ["patientid", "profileid"]) == 100000000000001


def test_resolve_no_match_returns_none():
    maps = {"patientid": {1: 100000000000001}}
    row  = {"patientid": 999}
    assert resolve_nd_patient_id(row, maps, ["patientid"]) is None


def test_resolve_all_cols_missing_returns_none():
    maps = {"patientid": {1: 100000000000001}}
    row  = {"encounter_id": "E1"}   # no identifier cols at all
    assert resolve_nd_patient_id(row, maps, ["patientid", "profileid"]) is None


# ══════════════════════════════════════════════════════════════════════════════
# generate_nd_id
# ══════════════════════════════════════════════════════════════════════════════

ND_PID = 100000000000001

def test_generate_nd_id_first_encounter():
    result = generate_nd_id(ND_PID, None, suffix_len=4)
    assert result == int(f"{ND_PID}0001")


def test_generate_nd_id_suffix_len_5():
    result = generate_nd_id(ND_PID, None, suffix_len=5)
    assert result == int(f"{ND_PID}00001")


def test_generate_nd_id_subsequent_increments():
    first = generate_nd_id(ND_PID, None, suffix_len=4)
    second = generate_nd_id(ND_PID, first, suffix_len=4)
    third  = generate_nd_id(ND_PID, second, suffix_len=4)
    assert second == first + 1
    assert third  == first + 2


def test_generate_nd_id_chart_none_uses_suffix():
    result = generate_nd_id(ND_PID, None, suffix_len=5, use_len_guard=True)
    assert result == int(f"{ND_PID}00001")


def test_generate_nd_id_chart_len_guard_short_id():
    # last_nd_id has only 15 digits (= nd_patient_id itself) → restart
    short_id = ND_PID  # 15 digits
    result = generate_nd_id(ND_PID, short_id, suffix_len=5, use_len_guard=True)
    assert result == int(f"{ND_PID}00001")


def test_generate_nd_id_chart_len_guard_normal_id():
    # last_nd_id already has > 15 digits → increment normally
    good_id = int(f"{ND_PID}00003")   # 20 digits
    result = generate_nd_id(ND_PID, good_id, suffix_len=5, use_len_guard=True)
    assert result == good_id + 1


def test_generate_nd_id_without_guard_does_not_apply_len_check():
    # Without use_len_guard, even a 15-digit last_id is treated as valid
    last_id = ND_PID   # 15 digits
    result = generate_nd_id(ND_PID, last_id, suffix_len=4, use_len_guard=False)
    assert result == last_id + 1


# ══════════════════════════════════════════════════════════════════════════════
# build_create_table_ddl
# ══════════════════════════════════════════════════════════════════════════════

def test_build_create_table_ddl_contains_expected_fragments():
    table_cfg = MINIMAL_CONFIG["child_tables"][0]   # encounter
    ddl = build_create_table_ddl(table_cfg)
    assert "encounter_mapping_table" in ddl
    assert "`encounter_id`" in ddl
    assert "`nd_encounter_id`" in ddl
    assert "`encounter_date`" in ddl
    assert "nd_ActiveFlag" in ddl
    assert "PRIMARY KEY" in ddl
    assert "CREATE TABLE IF NOT EXISTS" in ddl


def test_build_create_table_ddl_chart():
    table_cfg = MINIMAL_CONFIG["child_tables"][1]   # chart
    ddl = build_create_table_ddl(table_cfg)
    assert "chart_mapping_table" in ddl
    assert "`chart_id`" in ddl
    assert "`nd_chart_id`" in ddl


# ══════════════════════════════════════════════════════════════════════════════
# run_patient_phase — integration with mocked DB
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# run_patient_phase — integration with mocked execute_mssql_query
# ══════════════════════════════════════════════════════════════════════════════

@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_patient_phase_inserts_new_patients(mock_execute):
    src_rows = [
        {"patientid": 1, "profileid": 10, "pid": 100, "registration_date": None},
        {"patientid": 2, "profileid": 20, "pid": 200, "registration_date": None},
    ]
    mock_execute.return_value = src_rows

    mysql_cursor = MagicMock()
    mysql_cursor.fetchall.return_value = []
    mysql_cursor.fetchone.return_value = {"max_id": 99999999999999}
    mysql_conn = MagicMock()

    maps, count = run_patient_phase(
        {"server": "x"}, mysql_conn, mysql_cursor, MINIMAL_CONFIG["patient"], NOW, dry_run=False
    )

    assert count == 2
    assert mysql_cursor.executemany.called
    assert mysql_conn.commit.called
    assert 1 in maps["patientid"]
    assert 2 in maps["patientid"]


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_patient_phase_skips_existing_patients(mock_execute):
    mock_execute.return_value = [
        {"patientid": 1, "profileid": 10, "pid": 100, "registration_date": None},
    ]
    mysql_cursor = MagicMock()
    mysql_cursor.fetchall.return_value = [
        {"patientid": 1, "profileid": 10, "pid": 100, "nd_patient_id": 100000000000001}
    ]
    mysql_cursor.fetchone.return_value = {"max_id": 100000000000001}
    mysql_conn = MagicMock()

    maps, count = run_patient_phase(
        {"server": "x"}, mysql_conn, mysql_cursor, MINIMAL_CONFIG["patient"], NOW, dry_run=False
    )

    assert count == 0
    mysql_cursor.executemany.assert_not_called()


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_patient_phase_dry_run_does_not_write(mock_execute):
    mock_execute.return_value = [
        {"patientid": 1, "profileid": 10, "pid": 100, "registration_date": None},
    ]
    mysql_cursor = MagicMock()
    mysql_cursor.fetchall.return_value = []
    mysql_cursor.fetchone.return_value = {"max_id": 99999999999999}
    mysql_conn = MagicMock()

    maps, count = run_patient_phase(
        {"server": "x"}, mysql_conn, mysql_cursor, MINIMAL_CONFIG["patient"], NOW, dry_run=True
    )

    assert count == 1
    # No data rows written in dry-run; table creation may still commit schema changes.
    mysql_cursor.executemany.assert_not_called()


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_patient_phase_invalid_identifiers_aborts(mock_execute):
    mock_execute.return_value = [
        {"patientid": 1, "profileid": 10, "pid": 100, "registration_date": None},
        {"patientid": 2, "profileid": 10, "pid": 200, "registration_date": None},  # profileid=10 shared
    ]
    mysql_cursor = MagicMock()
    mysql_cursor.fetchall.return_value = []
    mysql_cursor.fetchone.return_value = {"max_id": 99999999999999}
    mysql_conn = MagicMock()

    with pytest.raises(ValueError, match="NOT 1:1"):
        run_patient_phase(
            {"server": "x"}, mysql_conn, mysql_cursor, MINIMAL_CONFIG["patient"], NOW
        )


# ══════════════════════════════════════════════════════════════════════════════
# run_child_table_phase — integration with mocked execute_mssql_query
# ══════════════════════════════════════════════════════════════════════════════

ND_PID_1 = 100000000000001
ND_PID_2 = 100000000000002

PATIENT_MAPS = {
    "patientid":  {1: ND_PID_1, 2: ND_PID_2},
    "profileid":  {10: ND_PID_1, 20: ND_PID_2},
    "pid":        {100: ND_PID_1, 200: ND_PID_2},
}

ENC_TABLE_CFG = MINIMAL_CONFIG["child_tables"][0]


def _child_mysql_mock(existing_child_rows):
    """MySQL mock: nd_ActiveFlag column exists, returns given existing rows."""
    cursor = MagicMock()
    cursor.fetchone.return_value = {"cnt": 1}
    cursor.fetchall.return_value = existing_child_rows
    conn = MagicMock()
    return conn, cursor


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_inserts_new_rows(mock_execute):
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 1, "encounter_date": None},
        {"encounter_id": "E2", "patientid": 2, "encounter_date": None},
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock([])

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["new_rows"] == 2
    assert stats["deactivated"] == 0
    assert mysql_cursor.executemany.called


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_skips_existing_active(mock_execute):
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 1, "encounter_date": None}
    ]
    existing = [
        {"nd_patient_id": ND_PID_1, "encounter_id": "E1",
         "nd_encounter_id": int(f"{ND_PID_1}0001"), "nd_ActiveFlag": "Y"}
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock(existing)

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["new_rows"] == 0
    mysql_cursor.executemany.assert_not_called()


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_transfer_deactivates_and_reinserts(mock_execute):
    # E1 currently belongs to patient 1; source now says it belongs to patient 2.
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 2, "encounter_date": None}
    ]
    existing = [
        {"nd_patient_id": ND_PID_1, "encounter_id": "E1",
         "nd_encounter_id": int(f"{ND_PID_1}0001"), "nd_ActiveFlag": "Y"}
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock(existing)

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["deactivated"] == 1
    assert stats["new_rows"] == 1


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_transfer_not_double_inserted_in_same_run(mock_execute):
    # E1 appears twice in source — both pointing to patient 2 after transfer.
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 2, "encounter_date": None},
        {"encounter_id": "E1", "patientid": 2, "encounter_date": None},  # duplicate
    ]
    existing = [
        {"nd_patient_id": ND_PID_1, "encounter_id": "E1",
         "nd_encounter_id": int(f"{ND_PID_1}0001"), "nd_ActiveFlag": "Y"}
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock(existing)

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["deactivated"] == 1
    assert stats["new_rows"] == 1   # not 2 — second occurrence skipped


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_unmapped_patient_skipped(mock_execute):
    mock_execute.return_value = [
        {"encounter_id": "E_ORPHAN", "patientid": 999, "encounter_date": None},
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock([])

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["new_rows"] == 0
    mysql_cursor.executemany.assert_not_called()


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_dry_run_no_writes(mock_execute):
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 1, "encounter_date": None}
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock([])

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=True,
    )

    assert stats["new_rows"] == 1
    # In dry-run, no data rows are written (executemany is for INSERTs/UPDATEs only).
    # Table creation (CREATE TABLE IF NOT EXISTS) may still commit schema changes.
    mysql_cursor.executemany.assert_not_called()


@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_child_phase_fallback_ref_col_used(mock_execute):
    # Row has patientid=None but profileid=10 → should resolve via fallback
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": None, "profileid": 10, "encounter_date": None}
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock([])
    cfg = dict(ENC_TABLE_CFG, patient_ref_cols=["patientid", "profileid"])

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        cfg, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["new_rows"] == 1


# ══════════════════════════════════════════════════════════════════════════════
# nd_id continuity — second run inserts nothing for same source data
# ══════════════════════════════════════════════════════════════════════════════

@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_incremental_run_no_duplicates(mock_execute):
    """Second run with identical source data must be a no-op."""
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 1, "encounter_date": None},
        {"encounter_id": "E2", "patientid": 1, "encounter_date": None},
    ]
    nd_id_1 = int(f"{ND_PID_1}0001")
    nd_id_2 = int(f"{ND_PID_1}0002")
    existing = [
        {"nd_patient_id": ND_PID_1, "encounter_id": "E1", "nd_encounter_id": nd_id_1, "nd_ActiveFlag": "Y"},
        {"nd_patient_id": ND_PID_1, "encounter_id": "E2", "nd_encounter_id": nd_id_2, "nd_ActiveFlag": "Y"},
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock(existing)

    stats = run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    assert stats["new_rows"] == 0
    assert stats["deactivated"] == 0
    mysql_cursor.executemany.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# nd_id sequence correctness
# ══════════════════════════════════════════════════════════════════════════════

@patch("mssql_incremental_mapping_update_v3.execute_mssql_query")
def test_nd_id_sequence_for_multiple_encounters_per_patient(mock_execute):
    mock_execute.return_value = [
        {"encounter_id": "E1", "patientid": 1, "encounter_date": None},
        {"encounter_id": "E2", "patientid": 1, "encounter_date": None},
        {"encounter_id": "E3", "patientid": 1, "encounter_date": None},
    ]
    mysql_conn, mysql_cursor = _child_mysql_mock([])

    inserted_rows = []
    mysql_cursor.executemany.side_effect = lambda _, rows: inserted_rows.extend(rows)

    run_child_table_phase(
        {"server": "x"}, mysql_conn, mysql_cursor,
        ENC_TABLE_CFG, PATIENT_MAPS, NOW, dry_run=False,
    )

    nd_ids = sorted(r[2] for r in inserted_rows)
    expected_first = int(f"{ND_PID_1}0001")
    assert nd_ids == [expected_first, expected_first + 1, expected_first + 2]


# ══════════════════════════════════════════════════════════════════════════════
# MappingStateManager
# ══════════════════════════════════════════════════════════════════════════════

def test_state_manager_roundtrip(tmp_path):
    sm = MappingStateManager(str(tmp_path))
    assert sm.load() == {}

    sm.save({"last_processed_date": "2024-01-15", "success": True, "new_patients": 5})
    loaded = sm.load()
    assert loaded["success"] is True
    assert loaded["new_patients"] == 5


def test_state_manager_missing_file_returns_empty(tmp_path):
    sm = MappingStateManager(str(tmp_path))
    assert sm.load() == {}


# ══════════════════════════════════════════════════════════════════════════════
# load_config — patient_ref_cols cross-validation
# ══════════════════════════════════════════════════════════════════════════════

def test_load_config_patient_ref_col_not_in_identifier_columns(tmp_path):
    bad = {
        "patient": {
            "sql_file": "/f.sql",
            "identifier_columns": ["patientid"],
            "base_nd_id": 100000000000000,
        },
        "child_tables": [
            {
                "name": "encounter",
                "sql_file": "/e.sql",
                "patient_ref_cols": ["profileid"],   # not in identifier_columns
                "source_id_col": "encounter_id",
                "date_col": "encounter_date",
                "nd_id_suffix_len": 4,
                "mapping_table": "encounter_mapping_table",
                "nd_id_col": "nd_encounter_id",
            }
        ],
    }
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="patient_ref_cols"):
        load_config(str(cfg_file))


def test_load_config_patient_ref_col_subset_is_valid(tmp_path):
    good = {
        "patient": {
            "sql_file": "/f.sql",
            "identifier_columns": ["patientid", "profileid", "pid"],
            "base_nd_id": 100000000000000,
        },
        "child_tables": [
            {
                "name": "encounter",
                "sql_file": "/e.sql",
                "patient_ref_cols": ["patientid"],   # valid subset
                "source_id_col": "encounter_id",
                "date_col": "encounter_date",
                "nd_id_suffix_len": 4,
                "mapping_table": "encounter_mapping_table",
                "nd_id_col": "nd_encounter_id",
            }
        ],
    }
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(good))
    result = load_config(str(cfg_file))   # should not raise
    assert result["child_tables"][0]["patient_ref_cols"] == ["patientid"]


# ══════════════════════════════════════════════════════════════════════════════
# ensure_patient_mapping_table DDL
# ══════════════════════════════════════════════════════════════════════════════

def test_ensure_patient_mapping_table_ddl_single_col():
    conn = MagicMock()
    cursor = MagicMock()
    ensure_patient_mapping_table(conn, cursor, ["patientid"])
    ddl = cursor.execute.call_args[0][0]
    assert "patient_mapping_table" in ddl
    assert "`patientid`" in ddl
    assert "nd_patient_id" in ddl
    assert "offset" in ddl
    assert "registration_date" in ddl
    assert "idx_patientid" in ddl
    conn.commit.assert_called_once()


def test_ensure_patient_mapping_table_ddl_multi_col():
    conn = MagicMock()
    cursor = MagicMock()
    ensure_patient_mapping_table(conn, cursor, ["patientid", "profileid", "pid"])
    ddl = cursor.execute.call_args[0][0]
    assert "`patientid`" in ddl
    assert "`profileid`" in ddl
    assert "`pid`" in ddl
    assert "idx_patientid" in ddl
    assert "idx_profileid" in ddl
    assert "idx_pid" in ddl
