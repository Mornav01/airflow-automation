"""
Reads constraint_removal_log CSV and prints a summary:
  - total / success / failure counts
  - common failure reasons (grouped by error code)

Also writes a headerless CSV of successful table names to the input folder.
"""

import csv
import re
from collections import Counter
from pathlib import Path



# ── Config ────────────────────────────────────────────────────────────────────
LOG_FILE        = Path("/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/state/adhoc/constraint_removal_log_1437_new1.csv")
SUCCESS_OUT     = Path("/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/constraint_removal_success_tables.csv")
# ─────────────────────────────────────────────────────────────────────────────


def extract_error_reason(error: str) -> str:
    """Return a short, human-readable reason from the raw error string."""
    if not error or not error.strip():
        return "(no error)"

    # Extract pymssql error code + first message line e.g. "(20017, b'DB-Lib error ... Unexpected EOF"
    match = re.search(r"\((\d+),\s*b['\"](.+?)\\n", error)
    if match:
        code, msg = match.group(1), match.group(2).strip()
        return f"[{code}] {msg}"

    # Fallback: first 120 chars
    return error.strip()[:120]


def main():
    rows = []
    with LOG_FILE.open(newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    total    = len(rows)
    success  = [r for r in rows if r["status"] == "success"]
    failures = [r for r in rows if r["status"] != "success"]

    # ── Print summary ────────────────────────────────────────────────────────
    print("=" * 60)
    print("  CONSTRAINT REMOVAL SUMMARY")
    print("=" * 60)
    print(f"  Total tables  : {total}")
    print(f"  Succeeded     : {len(success)}")
    print(f"  Failed        : {len(failures)}")
    print()

    if failures:
        reason_counter = Counter(extract_error_reason(r["error"]) for r in failures)
        print(f"  Common failure reasons ({len(reason_counter)} distinct):")
        print("-" * 60)
        for reason, count in reason_counter.most_common():
            print(f"  [{count:>3}x]  {reason}")
        print()

        print("  Failed tables:")
        print("-" * 60)
        for r in failures:
            print(f"  {r['table_name']:<50}  {extract_error_reason(r['error'])}")
        print()

    # ── Write success table names (no header) ────────────────────────────────
    SUCCESS_OUT.parent.mkdir(parents=True, exist_ok=True)
    with SUCCESS_OUT.open("w", newline="") as f:
        writer = csv.writer(f)
        for r in success:
            writer.writerow([r["table_name"]])

    print(f"  Success tables written to:")
    print(f"  {SUCCESS_OUT}")
    print("=" * 60)


if __name__ == "__main__":
    main()