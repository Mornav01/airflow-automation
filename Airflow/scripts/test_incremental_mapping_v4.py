"""
Standalone tests for _compute_child_changes (v4 logic).

No external imports — all pure functions are copied inline.
Run with: python test_incremental_mapping_v4.py
"""

import sys
import traceback
from datetime import datetime, date

# ─── Inline pure functions (copied from v4) ───────────────────────────────────

_EPOCH = datetime(1, 1, 1)


def _normalize_date(d):
    """Convert date or datetime to datetime for comparison; None stays None."""
    if d is None:
        return None
    if hasattr(d, 'hour'):
        return d
    return datetime(d.year, d.month, d.day)


def pick_winner_nd_pid(deduped_rows, date_col, current_active_owner=None):
    """
    From [(nd_patient_id, row_dict), ...] pick the winner: max encounter_date.
    None dates lose. Tiebreaker: prefer current_active_owner for stability.
    """
    def sort_key(item):
        nd_pid, row = item
        d = _normalize_date(row.get(date_col))
        return (d is not None, d if d is not None else _EPOCH, nd_pid == current_active_owner)
    return max(deduped_rows, key=sort_key)[0]


def generate_nd_id(nd_patient_id, last_nd_id, suffix_len, use_len_guard=False):
    """
    Generate the next nd_xxx_id for a patient.

    suffix_len: digits in the suffix appended to nd_patient_id on first ID.
    use_len_guard: when True (chart-style), treat any existing ID whose string
                   length is <= 15 as uninitialised and restart from the suffixed base.
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


def _compute_child_changes(
    src_data, source_id_col, date_col, patient_ref_cols,
    suffix_len, use_len_guard, patient_maps_by_col,
    all_records, source_id_map, last_nd_per_patient,
    now, mapping_table,
):
    """
    Pure function: computes all changes without DB access.
    - Groups source rows by source_id.
    - For each group, picks the winner by max date (None = lowest priority).
    - Winner: insert as active (new) or reactivate (existing inactive). No new nd_id on reactivation.
    - Non-winner: insert as inactive (new) or deactivate (existing active).
    - Multiple patient_ref_cols: two rows resolving to the same nd_patient_id are deduplicated
      (keep the row with the later date).
    Modifies all_records, source_id_map, last_nd_per_patient in-place.
    Returns: (new_active_rows, new_inactive_rows, deactivate_list, reactivate_list, unmapped_count)
    """
    source_groups = {}
    for row in src_data:
        sid = row.get(source_id_col)
        if sid is not None:
            source_groups.setdefault(sid, []).append(row)

    new_active_rows   = []
    new_inactive_rows = []
    deactivate_set    = set()
    reactivate_set    = set()
    unmapped_count    = 0

    for source_id, rows in source_groups.items():
        # Resolve nd_patient_id for each row; skip unmapped
        resolved = []
        for row in rows:
            nd_pid = resolve_nd_patient_id(row, patient_maps_by_col, patient_ref_cols)
            if nd_pid is None:
                unmapped_count += 1
                continue
            resolved.append((nd_pid, row))

        if not resolved:
            continue

        # Deduplicate by nd_patient_id: keep the row with the later date per patient
        deduped = {}
        for nd_pid, row in resolved:
            if nd_pid not in deduped:
                deduped[nd_pid] = row
            else:
                existing_d = _normalize_date(deduped[nd_pid].get(date_col))
                new_d      = _normalize_date(row.get(date_col))
                if new_d is not None and (existing_d is None or new_d > existing_d):
                    deduped[nd_pid] = row
        deduped_rows = list(deduped.items())

        current_active_owner = source_id_map.get(source_id)
        winner_nd_pid        = pick_winner_nd_pid(deduped_rows, date_col, current_active_owner)

        for nd_pid, row in deduped_rows:
            is_winner = (nd_pid == winner_nd_pid)
            existing  = all_records.get((nd_pid, source_id))

            if is_winner:
                if existing is None:
                    # New record → insert active; deactivate the old owner
                    if current_active_owner is not None and current_active_owner != nd_pid:
                        deactivate_set.add((current_active_owner, source_id))
                    last_nd = last_nd_per_patient.get(nd_pid)
                    nd_id   = generate_nd_id(nd_pid, last_nd, suffix_len, use_len_guard)
                    last_nd_per_patient[nd_pid]          = nd_id
                    all_records[(nd_pid, source_id)]     = {"nd_id": nd_id, "active": "Y"}
                    source_id_map[source_id]             = nd_pid
                    new_active_rows.append((nd_pid, source_id, nd_id, row.get(date_col), now, now))

                elif existing["active"] == "N":
                    # Existing inactive record → reactivate (keep same nd_id)
                    if current_active_owner is not None and current_active_owner != nd_pid:
                        deactivate_set.add((current_active_owner, source_id))
                    reactivate_set.add((nd_pid, source_id))
                    existing["active"]       = "Y"
                    source_id_map[source_id] = nd_pid

                # else: existing["active"] == "Y" → winner already active, nothing to do

            else:
                # Non-winner
                if existing is None:
                    # New non-winner → insert inactive
                    last_nd = last_nd_per_patient.get(nd_pid)
                    nd_id   = generate_nd_id(nd_pid, last_nd, suffix_len, use_len_guard)
                    last_nd_per_patient[nd_pid]      = nd_id
                    all_records[(nd_pid, source_id)] = {"nd_id": nd_id, "active": "N"}
                    new_inactive_rows.append((nd_pid, source_id, nd_id, row.get(date_col), now, now))

                elif existing["active"] == "Y":
                    # Currently active but not winner → deactivate
                    deactivate_set.add((nd_pid, source_id))
                    existing["active"] = "N"
                    if source_id_map.get(source_id) == nd_pid:
                        source_id_map.pop(source_id, None)

                # else: existing inactive non-winner → nothing to do

    return new_active_rows, new_inactive_rows, list(deactivate_set), list(reactivate_set), unmapped_count


# ─── Test helpers ─────────────────────────────────────────────────────────────

NOW = datetime(2025, 1, 1, 12, 0, 0)
SOURCE_ID_COL    = "encounter_id"
DATE_COL         = "encounter_date"
PATIENT_REF_COLS = ["patientid"]
SUFFIX_LEN       = 4
USE_LEN_GUARD    = False
MAPPING_TABLE    = "test_mapping"


def make_state(db_rows):
    """db_rows: list of (nd_patient_id, source_id, nd_id, active_flag)"""
    all_records = {}
    source_id_map = {}
    last_nd_per_patient = {}
    for nd_pid, sid, nd_id, active in db_rows:
        all_records[(nd_pid, sid)] = {"nd_id": nd_id, "active": active}
        if active == "Y":
            source_id_map[sid] = nd_pid
        if nd_id and (nd_pid not in last_nd_per_patient or nd_id > last_nd_per_patient[nd_pid]):
            last_nd_per_patient[nd_pid] = nd_id
    return all_records, source_id_map, last_nd_per_patient


def make_patient_maps(patients, col="patientid"):
    return {col: {p: nd for p, nd in patients}}


def run_compute(src_data, db_rows=None, patient_maps=None,
                patient_ref_cols=None, source_id_col=None, date_col=None):
    """Convenience wrapper that calls _compute_child_changes with defaults."""
    if db_rows is None:
        db_rows = []
    if patient_maps is None:
        patient_maps = make_patient_maps([])
    if patient_ref_cols is None:
        patient_ref_cols = PATIENT_REF_COLS
    if source_id_col is None:
        source_id_col = SOURCE_ID_COL
    if date_col is None:
        date_col = DATE_COL

    all_records, source_id_map, last_nd_per_patient = make_state(db_rows)

    return _compute_child_changes(
        src_data, source_id_col, date_col, patient_ref_cols,
        SUFFIX_LEN, USE_LEN_GUARD, patient_maps,
        all_records, source_id_map, last_nd_per_patient,
        NOW, MAPPING_TABLE,
    )


# ─── Tests ────────────────────────────────────────────────────────────────────

def test_basic_new_encounter():
    """Single new patient + encounter, empty DB → new_active=1."""
    src_data = [{"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 3, 1)}]
    pmaps    = make_patient_maps([(1, 1001)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, patient_maps=pmaps
    )

    assert len(new_active) == 1,    f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0,  f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,    f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,    f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,           f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][0] == 1001  # nd_patient_id
    assert new_active[0][1] == 10    # source_id
    print("✓ test_basic_new_encounter")


def test_basic_existing_active():
    """Existing active encounter, same patient in source → no changes."""
    db_rows  = [(1001, 10, 10010001, "Y")]
    src_data = [{"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 3, 1)}]
    pmaps    = make_patient_maps([(1, 1001)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    print("✓ test_basic_existing_active")


def test_basic_new_encounter_existing_patient():
    """Patient exists, encounter is new → new_active=1."""
    # Patient 1001 has no prior encounter in DB for encounter_id=20
    db_rows  = [(1001, 99, 10010001, "Y")]  # a different encounter for same patient
    src_data = [{"encounter_id": 20, "patientid": 1, "encounter_date": datetime(2025, 5, 1)}]
    pmaps    = make_patient_maps([(1, 1001)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    assert len(new_active) == 1,   f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][1] == 20  # source_id
    print("✓ test_basic_new_encounter_existing_patient")


def test_basic_unmapped_patient():
    """Source row with no patient in maps → unmapped=1, no rows."""
    src_data = [{"encounter_id": 10, "patientid": 999, "encounter_date": datetime(2025, 3, 1)}]
    pmaps    = make_patient_maps([])  # empty patient map

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, patient_maps=pmaps
    )

    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 1,          f"Expected 1 unmapped, got {unmapped}"
    print("✓ test_basic_unmapped_patient")


def test_transfer_to_new_patient():
    """Encounter moves to patient with NO prior record → deactivate=1, new_active=1."""
    # DB: patient 1001 owns encounter 10 (active)
    # Source: encounter 10 now belongs to patient 2 → nd_patient_id 1002 (no DB record for 1002+10)
    db_rows  = [(1001, 10, 10010001, "Y")]
    src_data = [{"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2025, 6, 1)}]
    pmaps    = make_patient_maps([(1, 1001), (2, 1002)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    assert len(new_active) == 1,   f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 1,   f"Expected 1 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][0] == 1002        # winner is patient 1002
    assert deactivate[0] == (1001, 10)     # old owner deactivated
    print("✓ test_transfer_to_new_patient")


def test_scenario_1_reactivation():
    """Encounter moves back to patient with INACTIVE record → deactivate=1, reactivate=1, new_active=0."""
    # DB: patient 1001 owns encounter 10 (active), patient 1002 has encounter 10 (inactive)
    # Source: encounter 10 now has patient 2 (nd=1002) with a later date — winner is 1002
    db_rows  = [
        (1001, 10, 10010001, "Y"),
        (1002, 10, 10020001, "N"),
    ]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 1, 1)},
        {"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2026, 3, 1)},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 1,   f"Expected 1 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 1,   f"Expected 1 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert deactivate[0] == (1001, 10)
    assert reactivate[0] == (1002, 10)
    print("✓ test_scenario_1_reactivation")


def test_scenario_2_winner_already_active():
    """Two source rows (p1 date=2025, p2 date=2026), DB has p2 active → no changes."""
    # DB: patient 1002 owns encounter 10 (active); no record for 1001+10
    db_rows  = [(1002, 10, 10020001, "Y")]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 1, 1)},
        {"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2026, 1, 1)},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    # Winner is p2 (later date) → already active → nothing for winner
    # Non-winner p1 has no DB record → new_inactive=1
    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 1, f"Expected 1 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    print("✓ test_scenario_2_winner_already_active")


def test_scenario_3_new_winner():
    """Three source rows (p1=2025, p2=2026-04, p3=2026-05), DB has (p1 inactive, p2 active) → deactivate p2, new_active p3."""
    db_rows  = [
        (1001, 10, 10010001, "N"),
        (1002, 10, 10020001, "Y"),
    ]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 1, 1)},
        {"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2026, 4, 1)},
        {"encounter_id": 10, "patientid": 3, "encounter_date": datetime(2026, 5, 1)},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002), (3, 1003)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    # Winner = p3 (latest date, no DB record) → new_active=1, deactivate old owner p2
    # Non-winners: p1 (existing inactive → nothing), p2 (existing active → deactivate)
    assert len(new_active) == 1,   f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 1,   f"Expected 1 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][0] == 1003
    assert deactivate[0] == (1002, 10)
    print("✓ test_scenario_3_new_winner")


def test_scenario_4_new_non_winner():
    """Three source rows (p1=2025, p2=2026-04, p3=2025-05), DB has (p1 inactive, p2 active) → new_inactive p3, p2 stays active."""
    db_rows  = [
        (1001, 10, 10010001, "N"),
        (1002, 10, 10020001, "Y"),
    ]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 1, 1)},
        {"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2026, 4, 1)},
        {"encounter_id": 10, "patientid": 3, "encounter_date": datetime(2025, 5, 1)},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002), (3, 1003)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    # Winner = p2 (latest date, already active → nothing)
    # Non-winners: p1 (existing inactive → nothing), p3 (no DB record → new_inactive)
    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 1, f"Expected 1 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_inactive[0][0] == 1003
    print("✓ test_scenario_4_new_non_winner")


def test_null_date_all_null_keeps_current_active():
    """All source dates null, DB has patient=2 active → no changes (current active wins tiebreaker)."""
    db_rows  = [(1002, 10, 10020001, "Y")]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": None},
        {"encounter_id": 10, "patientid": 2, "encounter_date": None},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    # Both dates None → tiebreaker: current_active_owner=1002 wins
    # Winner p2 already active → nothing; non-winner p1 no DB record → new_inactive=1
    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 1, f"Expected 1 new_inactive (p1), got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    print("✓ test_null_date_all_null_keeps_current_active")


def test_null_date_single_new():
    """Null date, no DB state → new_active=1."""
    src_data = [{"encounter_id": 10, "patientid": 1, "encounter_date": None}]
    pmaps    = make_patient_maps([(1, 1001)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, patient_maps=pmaps
    )

    assert len(new_active) == 1,   f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][3] is None  # date preserved as None
    print("✓ test_null_date_single_new")


def test_multi_identifier_same_patient():
    """Two source rows resolving to same nd_patient_id via different ref cols → treated as one patient, no duplicate."""
    # Both "patientid" col value 1 and "alt_id" col value 99 map to nd_patient_id 1001
    pmaps = {
        "patientid": {1: 1001},
        "alt_id":    {99: 1001},
    }
    src_data = [
        {"encounter_id": 10, "patientid": 1,    "alt_id": None, "encounter_date": datetime(2025, 3, 1)},
        {"encounter_id": 10, "patientid": None,  "alt_id": 99,  "encounter_date": datetime(2025, 6, 1)},
    ]

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, patient_maps=pmaps,
        patient_ref_cols=["patientid", "alt_id"]
    )

    # Both rows resolve to 1001 — deduplicated to one patient → keep later date row
    # Single winner (1001), no DB record → new_active=1
    assert len(new_active) == 1,   f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive (deduped same patient), got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][0] == 1001
    print("✓ test_multi_identifier_same_patient")


def test_multi_identifier_different_patients():
    """Two ref cols, two rows resolving to different patients → winner by date."""
    pmaps = {
        "patientid": {1: 1001},
        "alt_id":    {99: 1002},
    }
    src_data = [
        {"encounter_id": 10, "patientid": 1,    "alt_id": None, "encounter_date": datetime(2025, 3, 1)},
        {"encounter_id": 10, "patientid": None,  "alt_id": 99,  "encounter_date": datetime(2026, 1, 1)},
    ]

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, patient_maps=pmaps,
        patient_ref_cols=["patientid", "alt_id"]
    )

    # p1=1001 date 2025-03, p2=1002 date 2026-01 → winner is 1002 (later date)
    assert len(new_active) == 1,   f"Expected 1 new_active, got {len(new_active)}"
    assert len(new_inactive) == 1, f"Expected 1 new_inactive (non-winner), got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_active[0][0] == 1002
    assert new_inactive[0][0] == 1001
    print("✓ test_multi_identifier_different_patients")


def test_nd_id_first_encounter():
    """Verify nd_id = int(str(nd_patient_id) + '0001') for suffix_len=4."""
    nd_patient_id = 1001
    nd_id = generate_nd_id(nd_patient_id, None, suffix_len=4, use_len_guard=False)
    expected = int(str(nd_patient_id) + "0001")
    assert nd_id == expected, f"Expected {expected}, got {nd_id}"

    # Also verify via _compute_child_changes
    src_data = [{"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 3, 1)}]
    pmaps    = make_patient_maps([(1, nd_patient_id)])
    new_active, _, _, _, _ = run_compute(src_data, patient_maps=pmaps)
    assert new_active[0][2] == expected, f"Expected nd_id={expected}, got {new_active[0][2]}"
    print("✓ test_nd_id_first_encounter")


def test_nd_id_increment():
    """Second encounter gets last_nd + 1."""
    nd_patient_id = 1001
    first_nd_id   = int(str(nd_patient_id) + "0001")
    second_nd_id  = generate_nd_id(nd_patient_id, first_nd_id, suffix_len=4, use_len_guard=False)
    assert second_nd_id == first_nd_id + 1, f"Expected {first_nd_id + 1}, got {second_nd_id}"

    # Verify via _compute_child_changes: patient 1001 already has encounter 10; add encounter 20
    db_rows  = [(1001, 10, first_nd_id, "Y")]
    src_data = [{"encounter_id": 20, "patientid": 1, "encounter_date": datetime(2025, 5, 1)}]
    pmaps    = make_patient_maps([(1, nd_patient_id)])
    new_active, _, _, _, _ = run_compute(src_data, db_rows=db_rows, patient_maps=pmaps)
    assert new_active[0][2] == first_nd_id + 1, (
        f"Expected nd_id={first_nd_id + 1}, got {new_active[0][2]}"
    )
    print("✓ test_nd_id_increment")


def test_winner_already_active_non_winner_new():
    """Winner active, non-winner is new → new_inactive=1 for non-winner, no change for winner."""
    db_rows  = [(1002, 10, 10020001, "Y")]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 1, 1)},
        {"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2026, 1, 1)},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    # Winner = p2 (later date) already active → nothing
    # Non-winner = p1 has no DB record → new_inactive=1
    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 1, f"Expected 1 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 0,   f"Expected 0 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert new_inactive[0][0] == 1001
    print("✓ test_winner_already_active_non_winner_new")


def test_inconsistent_db_both_active():
    """DB has both patients active (inconsistent) → non-winner gets deactivated."""
    # Inconsistent DB: both p1 and p2 have encounter 10 as active
    db_rows  = [
        (1001, 10, 10010001, "Y"),
        (1002, 10, 10020001, "Y"),
    ]
    src_data = [
        {"encounter_id": 10, "patientid": 1, "encounter_date": datetime(2025, 1, 1)},
        {"encounter_id": 10, "patientid": 2, "encounter_date": datetime(2026, 6, 1)},
    ]
    pmaps = make_patient_maps([(1, 1001), (2, 1002)])

    new_active, new_inactive, deactivate, reactivate, unmapped = run_compute(
        src_data, db_rows=db_rows, patient_maps=pmaps
    )

    # Winner = p2 (later date), already active → nothing for winner
    # Non-winner = p1, currently active → deactivate
    assert len(new_active) == 0,   f"Expected 0 new_active, got {len(new_active)}"
    assert len(new_inactive) == 0, f"Expected 0 new_inactive, got {len(new_inactive)}"
    assert len(deactivate) == 1,   f"Expected 1 deactivate, got {len(deactivate)}"
    assert len(reactivate) == 0,   f"Expected 0 reactivate, got {len(reactivate)}"
    assert unmapped == 0,          f"Expected 0 unmapped, got {unmapped}"
    assert deactivate[0] == (1001, 10)
    print("✓ test_inconsistent_db_both_active")


# ─── Runner ───────────────────────────────────────────────────────────────────

ALL_TESTS = [
    test_basic_new_encounter,
    test_basic_existing_active,
    test_basic_new_encounter_existing_patient,
    test_basic_unmapped_patient,
    test_transfer_to_new_patient,
    test_scenario_1_reactivation,
    test_scenario_2_winner_already_active,
    test_scenario_3_new_winner,
    test_scenario_4_new_non_winner,
    test_null_date_all_null_keeps_current_active,
    test_null_date_single_new,
    test_multi_identifier_same_patient,
    test_multi_identifier_different_patients,
    test_nd_id_first_encounter,
    test_nd_id_increment,
    test_winner_already_active_non_winner_new,
    test_inconsistent_db_both_active,
]


if __name__ == "__main__":
    passed = 0
    failed = 0
    errors = []

    for test_fn in ALL_TESTS:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            failed += 1
            errors.append((test_fn.__name__, traceback.format_exc()))

    print()
    print(f"Results: {passed} passed, {failed} failed out of {len(ALL_TESTS)} tests.")

    if errors:
        print()
        for name, tb in errors:
            print(f"FAILED: {name}")
            print(tb)
        sys.exit(1)
    else:
        print("All tests passed.")
