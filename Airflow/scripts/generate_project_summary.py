"""
generate_project_summary.py

Reads all table state JSON files from the state directory and prints
a consolidated summary of diff, merge, and decryption results.

Usage:
    python generate_project_summary.py --state_dir /path/to/state
    python generate_project_summary.py --state_dir /path/to/state --json
    python generate_project_summary.py --state_dir /path/to/state --failed_only
"""

import csv
import os
import json
import argparse
from datetime import date

from db_config import STATE_DIR

STATE_DIRECTORY = STATE_DIR


def load_all_states(state_dir):
    states = {}
    for fname in sorted(os.listdir(state_dir)):
        if not fname.endswith(".json"):
            continue
        table = fname[:-5]
        path = os.path.join(state_dir, fname)
        try:
            with open(path) as f:
                states[table] = json.load(f)
        except Exception as e:
            states[table] = {"_load_error": str(e)}
    return states


def build_summary(states, today):
    summary = {
        "date": today,
        "total_tables": len(states),

        # Diff
        "diff_success": 0,
        "diff_failed": 0,
        "diff_never_run": 0,

        # Merge
        "merge_success": 0,
        "merge_failed": 0,
        "merge_never_run": 0,

        # Row counts (from successful merges)
        "total_updated_rows": 0,
        "total_new_rows": 0,
        "total_diffmerged_rows": 0,

        # Decryption
        "decryption_success": 0,
        "decryption_failed": 0,
        "decryption_never_run": 0,

        # Failures detail
        "diff_failures": [],
        "merge_failures": [],
        "decryption_failures": [],
    }

    for table, state in states.items():
        if "_load_error" in state:
            summary["diff_never_run"] += 1
            summary["merge_never_run"] += 1
            continue

        # ── Diff ──
        diff = state.get("diff", {})
        if not diff:
            summary["diff_never_run"] += 1
        elif diff.get("success"):
            summary["diff_success"] += 1
        else:
            summary["diff_failed"] += 1
            summary["diff_failures"].append({
                "table": table,
                "date": diff.get("last_processed_date"),
                "error": diff.get("error", "unknown"),
            })

        # ── Merge ──
        merge = state.get("merge", {})
        if not merge:
            summary["merge_never_run"] += 1
        elif merge.get("success"):
            summary["merge_success"] += 1
            summary["total_updated_rows"]    += merge.get("updated_rows", 0) or 0
            summary["total_new_rows"]        += merge.get("new_rows", 0) or 0
            summary["total_diffmerged_rows"] += merge.get("diffMerged_inserted_rows", 0) or 0
        else:
            summary["merge_failed"] += 1
            summary["merge_failures"].append({
                "table": table,
                "date": merge.get("last_processed_date"),
                "error": merge.get("error", "unknown"),
            })

        # ── Decryption ──
        dec = state.get("diff_decryption", {})
        if not dec:
            summary["decryption_never_run"] += 1
        elif dec.get("status") == "success":
            summary["decryption_success"] += 1
        else:
            summary["decryption_failed"] += 1
            summary["decryption_failures"].append({
                "table": table,
                "date": dec.get("last_processed_date"),
                "error": dec.get("error", "unknown"),
            })

    return summary


def print_summary(summary, failed_only=False):
    today = summary["date"]
    total = summary["total_tables"]

    if not failed_only:
        print("=" * 60)
        print(f"  PROJECT SUMMARY  —  {today}")
        print("=" * 60)
        print(f"  Total tables tracked : {total}")
        print()

        print("  DIFF")
        print(f"    Success     : {summary['diff_success']}")
        print(f"    Failed      : {summary['diff_failed']}")
        print(f"    Never run   : {summary['diff_never_run']}")
        print()

        print("  MERGE")
        print(f"    Success     : {summary['merge_success']}")
        print(f"    Failed      : {summary['merge_failed']}")
        print(f"    Never run   : {summary['merge_never_run']}")
        print()

        print("  ROWS (from successful merges)")
        print(f"    Updated rows          : {summary['total_updated_rows']:,}")
        print(f"    New rows              : {summary['total_new_rows']:,}")
        print(f"    DiffMerged inserted   : {summary['total_diffmerged_rows']:,}")
        print()

        print("  DECRYPTION")
        print(f"    Success     : {summary['decryption_success']}")
        print(f"    Failed      : {summary['decryption_failed']}")
        print(f"    Never run   : {summary['decryption_never_run']}")
        print()

    # ── Failures ──
    if summary["diff_failures"]:
        print("=" * 60)
        print(f"  DIFF FAILURES  ({len(summary['diff_failures'])})")
        print("=" * 60)
        for f in summary["diff_failures"]:
            print(f"  [{f['date']}]  {f['table']}")
            print(f"    {f['error'][:120]}")
        print()

    if summary["merge_failures"]:
        print("=" * 60)
        print(f"  MERGE FAILURES  ({len(summary['merge_failures'])})")
        print("=" * 60)
        for f in summary["merge_failures"]:
            print(f"  [{f['date']}]  {f['table']}")
            print(f"    {f['error'][:120]}")
        print()

    if summary["decryption_failures"]:
        print("=" * 60)
        print(f"  DECRYPTION FAILURES  ({len(summary['decryption_failures'])})")
        print("=" * 60)
        for f in summary["decryption_failures"]:
            print(f"  [{f['date']}]  {f['table']}")
            print(f"    {f['error'][:120]}")
        print()

    if not summary["diff_failures"] and not summary["merge_failures"] and not summary["decryption_failures"]:
        print("  No failures.")


def flatten_state(table, state):
    """Flatten a table's nested state JSON into a single dict for CSV export."""
    row = {"table": table}
    if "_load_error" in state:
        row["_load_error"] = state["_load_error"]
        return row

    for section in ("diff", "merge", "deidentification", "diff_decryption"):
        data = state.get(section, {})
        for key, val in data.items():
            row[f"{section}__{key}"] = val

    return row


def export_csv(states, csv_path):
    rows = [flatten_state(table, state) for table, state in states.items()]

    # Collect all column names (preserving order: table first, then all keys)
    all_keys = ["table"]
    seen = {"table"}
    for row in rows:
        for k in row:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV written to: {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate consolidated project summary from state files")
    parser.add_argument("--state_dir",  default=STATE_DIRECTORY, help="Path to state directory")
    parser.add_argument("--json",        action="store_true",     help="Output raw JSON instead of formatted text")
    parser.add_argument("--failed_only", action="store_true",     help="Only print failure sections")
    parser.add_argument("--csv", default="/Users/ndaicnsarizona/Desktop/production_state_incremental/state/summary/summary.csv", help="Export per-table CSV to this path")
    args = parser.parse_args()

    today = date.today().isoformat()
    states = load_all_states(args.state_dir)
    summary = build_summary(states, today)

    if args.json:
        print(json.dumps(summary, indent=4))
    else:
        print_summary(summary, failed_only=args.failed_only)

    export_csv(states, args.csv)


if __name__ == "__main__":
    main()
