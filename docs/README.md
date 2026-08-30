# Deidentification Pipelines — Data Flow Docs

How patient data moves **from which DB to which DB** across every pipeline: where it is
**queried**, **written**, **deleted**, and **when**. One document per client family:

| Doc | Pipelines | DAG file(s) |
|---|---|---|
| [ecw_greenway_dataflow.md](ecw_greenway_dataflow.md) | **eCW** + **Greenway** (share the `shared.tasks` factories) | `dags/ecw/ecw_pipeline_dag.py`, `dags/greenway/greenway_pipeline_dag.py` |
| [mind_dataflow.md](mind_dataflow.md) | **MIND** (inline tasks) | `dags/mind/mind_pipeline_dag.py` |
| [dent_dataflow.md](dent_dataflow.md) | **dent** (MySQL binlog CDC) | `dags/dent/cdc_automation_dag.py` |

---

## The shared mental model

Every pipeline does the same five things, differing only in *how*:

```
1. CAPTURE   get today's changed rows out of the live EHR (restore+diff, or binlog CDC)
2. IDENTITY  build/refresh nd_patient_id maps (mapping) + master PII/insurance tables
3. DEIDENTIFY  run the deid engine → produce a de-identified copy
4. MERGE     fold the de-identified delta into the de-identified base (SCD-2)
5. SHIP      dump the de-identified data to S3, then trim source accumulators
```

The **real PHI** lives on the **source side** (MSSQL for eCW/Greenway/MIND, MySQL for
dent). The **safe, de-identified** data lives on the **MySQL de-id side** and is the only
thing shipped to S3.

## The databases (naming convention — same across eCW / Greenway / MIND)

| Role | DB name | Engine | Meaning |
|---|---|---|---|
| **BASE** | `mobiledoc_base_0601` | MSSQL | Accumulating historical snapshot (SCD-2 via `nd_ActiveFlag`) |
| **PROD** | `mobiledoc_latest` | MSSQL | Full current snapshot, rehydrated daily by `restore` |
| **DIFF** | `mobiledoc_diff_staging` | MSSQL | Today's delta (changed rows only) |
| **DIFFMERGED** | `mobiledoc_diff_merged` | MSSQL | Original merged accumulator (trimmed at the end) |
| **MAPPING** | `mapping_pg` | MySQL | `patient_mapping_table`, `encounter_mapping_table` (source id → `nd_patient_id`) |
| **MASTER** | `master_pg` | MySQL | `pii_data_table`, `master_insurance_table` (keyed on `nd_patient_id`) |
| **DEID STAGING** | `deidentified_mobiledoc_diff_staging` | MySQL | De-identified delta (deid engine output) |
| **DEID BASE** | `deidentified_mobiledoc_base` | MySQL | De-identified accumulator (merge target) |
| **DEID MERGED** | `deidentified_mobiledoc_diff_merged` | MySQL | De-identified merged snapshot |
| **STATE / FAILED** | `deid_state` / `deid_failed` (MySQL) or `state.db` / `failed_rows.db` (SQLite) | — | Deid engine bookkeeping |

*dent uses different physical schema names — see [dent_dataflow.md](dent_dataflow.md).*

## Conventions used everywhere

- **SCD-2, not in-place update.** "Updating" a row = mark the old version `nd_ActiveFlag='N'`
  and **append** the new version. History is preserved in BASE / DEID BASE. True `DELETE`s are rare.
- **Per-table JSON state.** Each table has `{STATE_DIR}/{table}.json` with a `deidentification`
  block: `deidentification_status` (success/failed), `is_merged`, `is_deleted`,
  `last_processed_date`. Stages are gated on these flags (e.g. delete only runs after `is_merged`).
- **Source id → nd_patient_id.** All master/deid data is keyed on the synthetic `nd_patient_id`
  from `mapping_pg`, never the raw patient id.

## The four pipelines at a glance

| Aspect | eCW | Greenway | MIND | dent |
|---|---|---|---|---|
| Source engine | MSSQL | MSSQL | MSSQL | **MySQL** |
| Change capture | restore + diff-merge | restore + diff-merge | restore + diff-merge | **binlog CDC** |
| Task construction | shared factories | shared factories | **inline** | **inline** (CDC scripts) |
| Deid **source** DB | `mobiledoc_diff_staging` (DIFF) | `mobiledoc_diff_merged` (MERGED) | `mobiledoc_diff_merged` (MERGED) | `staging` schema |
| Extra decrypt stage | `decrypt_pnotes` (progressnotes) | `decrypt_cnotes` + `clinicalbin_deid` | `decrypt_cnotes` + `clinicalbin_deid` | `decrypt_pnotes` (progressnotes) |
| Mapping code | v5 (`incremental_mapping_update_v5.py`) | v5 | legacy `scripts/mind/mapping_delta.py` | `CDC/MySQL/mapping_delta.py` |
| S3 **source** schema | `deidentified_mobiledoc_diff_merged` | `deidentified…diff_staging` | `deidentified…diff_staging` | `deid_staging` |
| Terminal delete | `delete_original_diffmerged` (trims MSSQL DIFFMERGED) | same | same | none (schema drops only) |
| Validate stages | validate_diff_merge, validate_mapping, check_deid_status | same | same | `check_continue` short-circuit only |

## Where data is deleted (across all pipelines)

1. **`master_insurance` refresh** — per-patient `DELETE … WHERE nd_patient_id IN (delta)` then re-insert.
2. **decrypt** — `DROP TABLE IF EXISTS …_decryptfinal` then recreate.
3. **`delete_original_diffmerged`** (eCW/Greenway/MIND) — trims already-processed rows from the
   MSSQL `mobiledoc_diff_merged` accumulator (cutoff = per-table `last_processed_date`).
4. **schema resets** (dent, and greenway/mind clinicalbin staging) — `DROP SCHEMA` / `TRUNCATE`
   before rebuilding a staging schema.

Everything else is logical deactivation (`nd_ActiveFlag='N'`), not deletion.
