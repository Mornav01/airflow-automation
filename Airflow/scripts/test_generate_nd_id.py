"""
Test generate_nd_id from incremental_mapping_update_v3.py

Tests the use_len_guard condition — specifically for chart_mapping_table
where after migration, existing nd_chart_id values are 15-digit old nd_patient_ids.

Run: python test_generate_nd_id.py
"""

def generate_nd_id(nd_patient_id, last_nd_id, suffix_len, use_len_guard=False):
    suffix = "0" * (suffix_len - 1) + "1"
    if use_len_guard:
        if last_nd_id is None or len(str(last_nd_id)) <= 15:
            return int(str(nd_patient_id) + suffix)
        return last_nd_id + 1
    else:
        if last_nd_id is None:
            return int(str(nd_patient_id) + suffix)
        return last_nd_id + 1

ND_PATIENT_ID = 100000000001234  # 15 digits
SUFFIX_LEN    = 5                # gives "00001"
EXPECTED_FIRST = int(str(ND_PATIENT_ID) + "00001")  # 10000000000123400001

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

tests = [
    # (description, last_nd_id, use_len_guard, expected)

    # ── use_len_guard=True (chart table after migration) ─────────────────────
    (
        "None last_nd + guard=True  → restart from {nd_patient_id}00001",
        None,
        True,
        EXPECTED_FIRST,
    ),
    (
        "15-digit last_nd + guard=True → restart from {nd_patient_id}00001  [migration case]",
        ND_PATIENT_ID,           # old nd_patient_id stored as nd_chart_id
        True,
        EXPECTED_FIRST,
    ),
    (
        "19-digit last_nd + guard=True → increment normally",
        EXPECTED_FIRST,          # already a proper nd_chart_id from a previous v3 run
        True,
        EXPECTED_FIRST + 1,
    ),
    (
        "19-digit last_nd + guard=True → increment again",
        EXPECTED_FIRST + 2,
        True,
        EXPECTED_FIRST + 3,
    ),

    # ── use_len_guard=False (encounter table — no guard needed) ───────────────
    (
        "None last_nd + guard=False → restart from {nd_patient_id}00001",
        None,
        False,
        EXPECTED_FIRST,
    ),
    (
        "19-digit last_nd + guard=False → increment normally",
        EXPECTED_FIRST,
        False,
        EXPECTED_FIRST + 1,
    ),
    (
        "15-digit last_nd + guard=False → increments (no guard) [would be WRONG for chart]",
        ND_PATIENT_ID,
        False,
        ND_PATIENT_ID + 1,       # without guard, just +1 on the 15-digit value
    ),
]

print(f"\nND_PATIENT_ID  = {ND_PATIENT_ID}  ({len(str(ND_PATIENT_ID))} digits)")
print(f"EXPECTED_FIRST = {EXPECTED_FIRST} ({len(str(EXPECTED_FIRST))} digits)\n")
print(f"{'':─<80}")

passed = 0
failed = 0

for desc, last_nd, use_guard, expected in tests:
    result = generate_nd_id(ND_PATIENT_ID, last_nd, SUFFIX_LEN, use_guard)
    ok     = result == expected
    status = PASS if ok else FAIL
    print(f"{status}  {desc}")
    if not ok:
        print(f"       expected : {expected}")
        print(f"       got      : {result}")
    passed += ok
    failed += (not ok)

print(f"\n{'':─<80}")
print(f"Results: {passed} passed, {failed} failed\n")
