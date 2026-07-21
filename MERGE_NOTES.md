# Merge Notes — `mind` → `main` (airflow-automation)

Review companion for the `mind-into-main` PR. **TL;DR:** adopt `mind`'s consolidated,
EHR-based pipeline architecture as the new `main`, while **preserving every existing client**
(including the athena/veradigm ones that only lived on `main`) and hardening security/config.
Nothing functional was dropped — superseded files were **reorganized, not deleted**, and the
legacy clients keep working on their existing code path.

This is the **orchestration** side. The **deid engine** changes live in the companion
`deidentification_v2` PR (mind + dent CDC + engine optimizations) — review them together.

---

## Why this approach

`main` had been onboarding clients in the *old* flat/versioned structure; `mind` refactored the
whole repo into an **EHR-based architecture** (per-EHR script folders + shared modules + one
pipeline DAG per EHR). Two facts shaped the merge:

1. `mind` only implemented eCW / Greenway / MIND in the new architecture. Clients that existed
   only on `main` — **noran, raleigh, dcnd, athenaone (AthenaOne/Athenapractice), ashville
   (Veradigm)** — were absent from `mind`.
2. Those clients run on the older v3/v4/v5 engine scripts that `mind` removed.

So instead of a blind merge (which would have **deleted those clients**), we did a **hybrid**:
new EHR architecture from `mind` **+** legacy clients preserved on their existing code path,
to be re-ported to the delta architecture later.

---

## What changed

### 1. EHR-based architecture (from `mind`)
- Per-EHR script folders: `scripts/{ecw,greenway,mind}/` (daily_dump, mapping_delta,
  master_*_delta, decrypt_*notes, restore_incremental_logs, clients/`*.yaml`).
- Shared DAG modules: `dags/shared/` (`tasks.py` task factories, `notifications.py`).
- One pipeline DAG per EHR: `ecw_pipeline`, `greenway_pipeline`, `mind_pipeline`, plus the
  `dent` CDC DAGs (`dags/dent/`, pulled from the `dent` branch).

### 2. Legacy clients preserved — hybrid (nothing deleted)
- athena/veradigm clients (noran, raleigh, dcnd, athenaone, ashville) keep their existing
  configs, SQL, and v3/v4/v5 engine scripts so they run unchanged.
- A **`db_config` compatibility shim** provides the role-based constants those legacy scripts
  expect (`MAPPING_SOURCE_*`, `MASTER_SOURCE/DEST/MAPPING_CONN`, `AWS_TRANSFER_*`), read lazily
  so new EHR clients are unaffected.
- Superseded per-client configs/SQL were **moved into `scripts/{ehr}/legacy/`** (and
  `dags/legacy/`) — reorganized, not removed.

### 3. deid invocation routed through one wrapper
All pipelines invoke the deid engine through a single
`scripts/manual_deidentification_run_v2.py` wrapper (instead of calling the `deid` CLI inline
in each DAG), so the CLI surface is touched in exactly one place.

### 4. Stale Django code removed
The engine moved to the `deid` CLI; the old Django `deIdentification/` path is gone. Removed:
`deidentification_runner.py`, `deid_post_process_runner.py`, `state_deidentification_run_polar.py`,
`manual_deidentification_run_polar.py`, `state_deid_status_check.py`, and the
`manage.py`-based `start_workers_*.sh`.

### 5. Transfer DAGs
`gcp_transfer.py` (misleadingly named — it was already pure S3) renamed to `s3_transfer_dag.py`;
the older `aws_transfer.py` archived to `dags/legacy/` with `_legacy`-suffixed dag_ids (no
collision with the active S3 DAG).

### 6. Security hardening
- **Credentials scrubbed** to `os.environ` reads — repo-wide secret scan: **0** (including
  legacy configs).
- **Notebooks → scripts**: all `.ipynb` converted to `.py` and removed.

### 7. Config driven by env (per-client portability)
- pii `replace_value` (per-client facility-name redactions) externalized to a single
  **`REPLACE_VALUE_JSON`** env var across the active pii_configs. *(Fixes a latent bug: the
  shared eCW pii_config hardcoded one client's facility names.)*
- Hardcoded machine paths / DB creds in the adhoc + arizona + jwm configs replaced with `${VAR}`.

### 8. `.env.example` partitioned
Active mind-branch pipeline vars on top; a `░░░` divider; then **legacy** (athena/veradigm shim
vars) and **manual-utility** sections. New vars documented (`REPLACE_VALUE_JSON`, restore vars,
legacy shim vars). Manual-script vars live in `manual.env.example`.

---

## Notes for reviewers

- **Legacy clients are not yet re-ported** to the delta architecture — they intentionally run on
  the preserved legacy code path via the `db_config` shim. Re-porting each (AthenaOne,
  Athenapractice, Veradigm) is scoped follow-up work.
- **Duplicate dag_ids are intentional**: `dags/X.py` vs `dags/mind/X.py` (e.g. `provider_dashboard`,
  `deidentification_runner_v2`) share ids on purpose — each machine points `dags_folder` at its
  own subfolder, so they never load together.
- **`dent` CDC DAGs** here invoke the deid engine's `CDC/MySQL/` scripts (in the
  `deidentification_v2` PR) via `DEID_REPO_DIR` — that PR must land for the dent pipeline to run.

### What to verify
- Legacy clients still resolve their configs/SQL/scripts (preserved + shim env vars present).
- No secrets (scan returns 0); active vs legacy/manual env split is correct in `.env.example`.
- Every deid run goes through `manual_deidentification_run_v2.py`.

### Files to focus on
- `dags/shared/tasks.py`, `dags/{ecw,greenway,mind}/*_pipeline_dag.py` — the per-EHR pipelines
- `dags/dent/` — CDC DAGs (from `dent`)
- `scripts/db_config.py` — legacy compatibility shim
- `scripts/{ecw,greenway,mind}/legacy/`, `dags/legacy/` — preserved superseded files
- `Airflow/.env.example` — partitioned config template

---

## Companion PR
`deidentification_v2` `mind-into-main`: the deid engine — `mind` security/config hardening +
`dent` production CDC pipeline (`CDC/MySQL/`) + worker robustness/perf optimizations, with
`main`'s `nd_patient_id` mapping rework preserved. See that repo's `MERGE_NOTES.md`.
