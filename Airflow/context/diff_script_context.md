# Diff Script — Context Document

## Purpose

Compares a named SQL Server table between two separate database instances (**OldDB** and **NewDB**) and writes the differences into a third database (**DiffDB**). This is a Python port of the T-SQL stored procedure `[dbo].[sp_run_diff]`.

---

## Key Constraint

**OldDB and NewDB are on different SQL Server instances.** No linked-server or cross-database joins are possible. All comparison logic must be orchestrated from Python.

---

## Files

| File | Purpose |
|---|---|
| `scripts/diff_script_duplicate_pk_handled.py` | Original script — correct but slow |
| `scripts/diff_script_optimised.py` | Optimised version — see below |

---

## Why the Original Was Slow

`read_table_full()` pulled **every column of every row** from both OldDB and NewDB into pandas RAM. Then Python hashed each row individually via `df.apply(hash_row, axis=1)` — a row-by-row Python loop that is ~100x slower than any vectorised operation.

For a 500k-row table with 40 columns this means transferring ~gigabytes of data across the network before doing any comparison work.

---

## Optimisation Strategy

Push `HASHBYTES('SHA2_256', CONCAT_WS('|', ...))` to SQL Server. Python only receives small intermediate frames then fetches delta rows.

### PK-based diff — 3 phases

```
Phase 1  SELECT [pk], HASHBYTES(...) AS _row_hash FROM table   (both DBs)
         Transfer: pk + 32 bytes per row  (vs. all columns per row)

Phase 2  Python set / merge ops on the tiny hash DataFrames
         → NEW_ONLY pks (in new, not in old)
         → CHANGED pks (in both, but hash differs)

Phase 3  SELECT all_cols FROM table WHERE pk IN (delta_pks)   (NewDB only)
         Transfer: full rows for delta only
         If 5% of rows changed → transfer 5% of the table, not 100%
```

### No-PK diff — 2 phases

```
Phase 1a  OldDB:  SELECT HASHBYTES(...) AS _row_hash FROM table
          Transfer: 32 bytes per row only

Phase 1b  NewDB:  SELECT all_cols, HASHBYTES(...) AS _row_hash FROM table
          Transfer: full rows + hash in a single query

Phase 2   Counter comparison in Python
          new rows whose hash is not matched in old Counter → NEW_OR_CHANGED
          Full data already in hand from Phase 1b — no further fetch needed
```

---

## Function Map

### `diff_script_optimised.py`

| Function | What it does |
|---|---|
| `make_engine(conn_str, fast_executemany)` | Creates SQLAlchemy engine; set `fast_executemany=True` for DiffDB |
| `get_primary_key(engine, table, schema)` | Returns first PK column from `sys.indexes`; None if no PK |
| `get_all_columns(engine, table, schema)` | All column names in `column_id` order from NewDB |
| `get_comparable_columns(new_eng, old_eng, ...)` | Intersect of non-LOB, non-computed cols present in both DBs |
| `get_table_ddl_info(engine, table, schema)` | Column metadata needed to recreate table in DiffDB |
| `has_column(engine, table, schema, col_name)` | Generic column existence check |
| `has_active_flag(engine, table, schema)` | Checks if `nd_ActiveFlag` exists in OldDB |
| `ensure_diff_table(diff_engine, ...)` | DROP + CREATE the diff table in DiffDB |
| `build_hash_sql(table, schema, comparable_cols, select_cols, where)` | Builds `SELECT [select_cols], HASHBYTES(...) AS _row_hash FROM table` |
| `fetch_hashes(engine, ...)` | Executes `build_hash_sql` and returns a DataFrame |
| `fetch_rows_by_pk(engine, table, schema, cols, pk, pk_values)` | Phase 3 — batch-fetches full rows by PK from NewDB |
| `dedup_old_df(old_df, pk, has_extracted_date)` | Keeps one row per PK (latest by `nd_extracted_date` if present) |
| `insert_to_diff(diff_engine, df, table, schema, diff_type)` | Appends rows to DiffDB; `fast_executemany` handles bulk perf |
| `diff_with_pk(...)` | Orchestrates 3-phase PK-based diff |
| `diff_without_pk(...)` | Orchestrates 2-phase hash-based diff |
| `run_diff(table_name, old_conn, new_conn, diff_conn, schema)` | Main entry point |

---

## Diff Output Labels

| Label | Meaning | Mode |
|---|---|---|
| `NEW_ONLY` | PK exists in NewDB but not OldDB | PK-based |
| `CHANGED` | PK in both DBs but comparable columns differ | PK-based |
| `NEW_OR_CHANGED` | Row hash not found in OldDB hash set | No-PK |

---

## Key Constants / Settings

| Constant | Value | Notes |
|---|---|---|
| `HASH_DELIM` | `\|` | Must match `CONCAT_WS` delimiter in the original SP |
| `EXCLUDE_TYPE_IDS` | `{34, 35, 99}` | Skips `text`, `ntext`, `image` columns |
| `PK_BATCH_SIZE` | `500` | Max PKs per `IN (...)` clause in Phase 3 |
| `fast_executemany` | `True` on diff engine | pyodbc driver-level batch insert optimisation |

---

## SQL Server HASHBYTES Notes

- SQL Server `HASHBYTES('SHA2_256', CONCAT_WS('|', ...))` returns `VARBINARY(32)`.
- pandas reads this as Python `bytes` or `memoryview` depending on the driver version.
- The no-PK Counter comparison normalises with `bytes(h)` to handle both.
- `CAST(col AS NVARCHAR(MAX))` is applied to every column before `CONCAT_WS` to handle numeric/date types correctly.
- `CONCAT_WS` skips `NULL` values between delimiters — this matches the original `hash_row` behaviour (NULL → empty string).

---

## nd_ActiveFlag Logic

In the PK-based diff, OldDB is filtered with `WHERE nd_ActiveFlag = 'Y'` **only if** that column exists in the OldDB table. This is checked via `has_active_flag()` and passed as `active_flag_filter` to `diff_with_pk`.

---

## Data Flow Diagram

```
NewDB instance                  OldDB instance
─────────────                   ─────────────
Phase 1: SELECT pk, hash        Phase 1: SELECT pk, [nd_ext_date], hash
         ↓                               ↓
         new_hashes df                   old_hashes df  (tiny, ~33 bytes/row)
                \                       /
                 Python set / merge ops
                 → new_only_pks
                 → changed_pks
                        ↓
Phase 3: SELECT * WHERE pk IN (delta_pks)
         ↓
         delta_df (full rows for changed/new only)
                        ↓
                DiffDB instance
                INSERT INTO [dbo].[table] (diff_type, ...)
```

---

## How to Run

```bash
python diff_script_optimised.py \
    --table MyTable \
    --old_conn  "mssql+pyodbc://sa:pass@old-server:1433/OldDB?driver=ODBC+Driver+17+for+SQL+Server" \
    --new_conn  "mssql+pyodbc://sa:pass@new-server:1433/NewDB?driver=ODBC+Driver+17+for+SQL+Server" \
    --diff_conn "mssql+pyodbc://sa:pass@localhost:1433/DiffDB?driver=ODBC+Driver+17+for+SQL+Server"
```

---

## Verification Checklist

1. Row counts in DiffDB match between old and new script for the same table.
2. Log shows Phase 3 fetching far fewer rows than the total table size (confirms delta-only fetch).
3. Wall-clock time on a large table (>100k rows) is significantly lower.
4. `nd_extracted_date` is populated in DiffDB rows.
5. `diff_type` values are correct: `NEW_ONLY` / `CHANGED` for PK tables, `NEW_OR_CHANGED` for no-PK tables.
