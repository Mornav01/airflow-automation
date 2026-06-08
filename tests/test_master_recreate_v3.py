"""
Tests for master_recreate_v3.py

Run: pytest tests/test_master_recreate_v3.py -v
"""

import sys
import os
import yaml
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

# Stub C-extension drivers
sys.modules.setdefault("pymssql", MagicMock())
sys.modules.setdefault("mysql", MagicMock())
sys.modules.setdefault("mysql.connector", MagicMock())
sys.modules.setdefault("sqlalchemy", MagicMock())
sys.modules.setdefault("sqlalchemy.engine", MagicMock())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Airflow", "scripts"))

from master_recreate_v3 import (
    resolve_nd_patient_ids_df,
    rotate_table,
    MasterStateManager,
)
from incremental_mapping_update_v3 import load_config

ND_PID_1 = 100000000000001
ND_PID_2 = 100000000000002

PATIENT_MAPS = {
    "patientid":  {1: ND_PID_1, 2: ND_PID_2},
    "profileid":  {10: ND_PID_1, 20: ND_PID_2},
    "pid":        {100: ND_PID_1, 200: ND_PID_2},
}


# ══════════════════════════════════════════════════════════════════════════════
# resolve_nd_patient_ids_df
# ══════════════════════════════════════════════════════════════════════════════

def test_resolve_first_col_match():
    df = pd.DataFrame([{"patientid": 1, "name": "A"}, {"patientid": 2, "name": "B"}])
    result = resolve_nd_patient_ids_df(df, PATIENT_MAPS, ["patientid", "profileid"])
    assert result.tolist() == [ND_PID_1, ND_PID_2]


def test_resolve_fallback_to_second_col():
    # patientid not present → fall back to profileid
    df = pd.DataFrame([{"profileid": 10, "name": "A"}, {"profileid": 20, "name": "B"}])
    result = resolve_nd_patient_ids_df(df, PATIENT_MAPS, ["patientid", "profileid"])
    assert result.tolist() == [ND_PID_1, ND_PID_2]


def test_resolve_no_match_returns_na():
    df = pd.DataFrame([{"patientid": 999, "name": "X"}])
    result = resolve_nd_patient_ids_df(df, PATIENT_MAPS, ["patientid"])
    assert pd.isna(result.iloc[0])


def test_resolve_mixed_rows():
    # Row 0: patientid matches; Row 1: patientid missing, profileid matches
    df = pd.DataFrame([
        {"patientid": 1, "profileid": None, "name": "A"},
        {"patientid": None, "profileid": 20, "name": "B"},
    ])
    result = resolve_nd_patient_ids_df(df, PATIENT_MAPS, ["patientid", "profileid"])
    assert result.iloc[0] == ND_PID_1
    assert result.iloc[1] == ND_PID_2


def test_resolve_col_not_in_df_skipped():
    # Only profileid in df, and join_cols starts with patientid (not in df)
    df = pd.DataFrame([{"profileid": 10}])
    result = resolve_nd_patient_ids_df(df, PATIENT_MAPS, ["patientid", "profileid"])
    assert result.iloc[0] == ND_PID_1   # fell back to profileid


def test_resolve_empty_df():
    df = pd.DataFrame(columns=["patientid"])
    result = resolve_nd_patient_ids_df(df, PATIENT_MAPS, ["patientid"])
    assert len(result) == 0


# ══════════════════════════════════════════════════════════════════════════════
# rotate_table
# ══════════════════════════════════════════════════════════════════════════════

def test_rotate_table_renames_existing():
    mock_engine = MagicMock()
    mock_conn   = MagicMock()
    mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)
    # Table exists → scalar returns 1
    mock_conn.execute.return_value.scalar.return_value = 1

    rotate_table(mock_engine, "pii_data_table")

    # 3 execute calls expected: DROP _old, SELECT COUNT(*), RENAME
    assert mock_conn.execute.call_count == 3


def test_rotate_table_no_rename_if_not_exists():
    mock_engine = MagicMock()
    mock_conn   = MagicMock()
    mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)
    # Table does NOT exist → scalar returns 0 → no RENAME call
    mock_conn.execute.return_value.scalar.return_value = 0

    rotate_table(mock_engine, "pii_data_table")

    # Only 2 calls: DROP _old, SELECT COUNT(*) — no RENAME
    assert mock_conn.execute.call_count == 2


# ══════════════════════════════════════════════════════════════════════════════
# load_config — master_tables validation (via mapping script's load_config)
# ══════════════════════════════════════════════════════════════════════════════

MINIMAL_MASTER_CONFIG = {
    "source_type": "mysql",
    "patient": {
        "sql_file": "/f.sql",
        "identifier_columns": ["patientid", "profileid"],
        "base_nd_id": 100000000000000,
    },
    "child_tables": [],
    "master_tables": [
        {
            "name": "pii_data",
            "sql_file": "/pii.sql",
            "join_cols": ["patientid"],
            "target_table": "pii_data_table",
        }
    ],
}


def test_load_config_valid_master_tables(tmp_path):
    f = tmp_path / "cfg.yaml"
    f.write_text(yaml.dump(MINIMAL_MASTER_CONFIG))
    result = load_config(str(f))
    assert result["master_tables"][0]["target_table"] == "pii_data_table"


def test_load_config_master_table_missing_key(tmp_path):
    bad = dict(MINIMAL_MASTER_CONFIG)
    bad["master_tables"] = [{"name": "pii", "sql_file": "/f.sql", "join_cols": ["patientid"]}]  # no target_table
    f = tmp_path / "cfg.yaml"
    f.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="target_table"):
        load_config(str(f))


def test_load_config_master_table_invalid_join_col(tmp_path):
    bad = dict(MINIMAL_MASTER_CONFIG)
    bad["master_tables"] = [{
        "name": "pii", "sql_file": "/f.sql",
        "join_cols": ["encounter_id"],    # not in identifier_columns
        "target_table": "pii_data_table",
    }]
    f = tmp_path / "cfg.yaml"
    f.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="join_cols"):
        load_config(str(f))


def test_load_config_master_table_empty_join_cols(tmp_path):
    bad = dict(MINIMAL_MASTER_CONFIG)
    bad["master_tables"] = [{
        "name": "pii", "sql_file": "/f.sql",
        "join_cols": [],   # empty list
        "target_table": "pii_data_table",
    }]
    f = tmp_path / "cfg.yaml"
    f.write_text(yaml.dump(bad))
    with pytest.raises(ValueError, match="join_cols"):
        load_config(str(f))


def test_load_config_no_master_tables_still_valid(tmp_path):
    cfg = dict(MINIMAL_MASTER_CONFIG)
    del cfg["master_tables"]
    f = tmp_path / "cfg.yaml"
    f.write_text(yaml.dump(cfg))
    result = load_config(str(f))   # should not raise
    assert result.get("master_tables", []) == []


# ══════════════════════════════════════════════════════════════════════════════
# MasterStateManager
# ══════════════════════════════════════════════════════════════════════════════

def test_master_state_manager_roundtrip(tmp_path):
    sm = MasterStateManager(str(tmp_path))
    assert sm.load() == {}
    sm.save({"pii_data": {"last_processed_date": "2026-06-08", "success": True, "row_count": 100}})
    loaded = sm.load()
    assert loaded["pii_data"]["success"] is True
    assert loaded["pii_data"]["row_count"] == 100
