-- =============================================================================
-- Migration: add the exclusion flag to mapping_pg.patient_mapping_table (DENT)
--
-- Before: patients matching the exclusion criteria were filtered out of the
-- mapping delta entirely, so they had no mapping row at all. That made the
-- exclusion invisible (no record of who was excluded, or when) and meant a
-- patient who resurfaced in a later CDC window without exclusion criteria was
-- minted a brand-new nd_patient_id.
--
-- After: excluded patients are mapped like everyone else and flagged here.
-- `excluded_at` records when the flag was first raised. The flag is sticky —
-- CDC/MySQL/mapping_delta.py only ever raises it, never clears it, because the
-- criteria are evaluated against the staging delta (one CDC window) rather than
-- the full source.
--
-- Consumers that must filter `excluded = 0` (already updated in code):
--   - CDC/MySQL/master_pii_delta.py        load_patient_map
--   - CDC/MySQL/master_insurance_delta.py  load_patient_map
--   - deid/tasks/celery_app.py             mapping preload
--   - deid/core/process_df/main.py         JoinMapping (per-batch SQL path)
--
-- Idempotent and safe to re-run. mapping_delta.py applies the same ALTERs on
-- startup, so running this by hand is optional — it exists so the schema change
-- can be applied and reviewed ahead of the pipeline run.
--
-- Usage:  mysql -h <host> -u <user> -p mapping_pg < add_patient_exclusion_flag_dent.sql
-- =============================================================================

USE mapping_pg;

SET @db := DATABASE();

-- --- excluded ----------------------------------------------------------------
SET @exists := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = @db
    AND TABLE_NAME   = 'patient_mapping_table'
    AND COLUMN_NAME  = 'excluded'
);
SET @sql := IF(@exists = 0,
  'ALTER TABLE patient_mapping_table ADD COLUMN excluded TINYINT(1) NOT NULL DEFAULT 0',
  'SELECT "column excluded already present" AS status'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- --- excluded_at -------------------------------------------------------------
SET @exists := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = @db
    AND TABLE_NAME   = 'patient_mapping_table'
    AND COLUMN_NAME  = 'excluded_at'
);
SET @sql := IF(@exists = 0,
  'ALTER TABLE patient_mapping_table ADD COLUMN excluded_at DATETIME NULL DEFAULT NULL',
  'SELECT "column excluded_at already present" AS status'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- --- idx_excluded ------------------------------------------------------------
SET @exists := (
  SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = @db
    AND TABLE_NAME   = 'patient_mapping_table'
    AND INDEX_NAME   = 'idx_excluded'
);
SET @sql := IF(@exists = 0,
  'CREATE INDEX idx_excluded ON patient_mapping_table (excluded)',
  'SELECT "index idx_excluded already present" AS status'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- =============================================================================
-- Verification
-- =============================================================================

SELECT excluded, COUNT(*) AS patients, MIN(excluded_at) AS first_excluded_at
FROM patient_mapping_table
GROUP BY excluded;

-- Patients excluded, most recent first:
--   SELECT nd_patient_id, patientid, excluded_at
--   FROM patient_mapping_table
--   WHERE excluded = 1
--   ORDER BY excluded_at DESC;

-- =============================================================================
-- Backfill note (manual, deliberate)
-- =============================================================================
-- Every existing row lands on excluded = 0. Patients excluded by earlier runs of
-- the pipeline simply have no mapping row, so there is nothing to backfill from.
-- Going forward they are flagged as their exclusion criteria surface in the CDC
-- windows. To flag the full historical set in one pass, run the exclusion query
-- against the FULL source database (not the staging delta) and:
--
--   UPDATE mapping_pg.patient_mapping_table
--   SET excluded = 1, excluded_at = NOW(), updated_at = NOW()
--   WHERE excluded = 0 AND patientid IN ( <full-source exclusion query> );
--
-- Patients already written to master_pg before being flagged keep their rows
-- there; purge them separately if that is required.
