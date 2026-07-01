-- =============================================================================
-- Migration: mapping → mapping_pg  and  master → master_pg   (DENT / mobiledoc)
-- Creates the *_pg databases with the cleaned schema and migrates all existing
-- data for the dent (mobiledoc, MySQL-source CDC) project.
--
-- Mirrors mapping_migration_to_new_structure_greenway_ecw.sql. The only dent
-- difference on the mapping side: dent's OLD encounter_mapping_table has NO
-- nd_ActiveFlag column (the greenway/ecw source already had one), so it is
-- defaulted to 'Y' during migration here.
--
-- Changes vs mapping:
--   patient_mapping_table:
--     - patient_id (INT) renamed to patientid (BIGINT)
--     - nd_patient_id: AUTO_INCREMENT removed (plain BIGINT, unique via PK)
--     - Dropped: reference_mapping, created_by, updated_by
--     - TIMESTAMP → DATETIME
--
--   encounter_mapping_table:
--     - Added nd_patient_id BIGINT NOT NULL (bridges to patient via nd_patient_id)
--     - Removed patient_id (replaced by nd_patient_id bridge; patientid kept denormalised)
--     - id INT → BIGINT AUTO_INCREMENT
--     - encounter_id INT → BIGINT
--     - nd_encounter_id DECIMAL(20,0) → BIGINT
--     - encounter_date: nullable DATETIME
--     - nd_ActiveFlag CHAR(1) added (defaulted to 'Y'; absent in dent's old table)
--     - Dropped: created_by, updated_by
--     - TIMESTAMP → DATETIME
--     - Removed unique keys uq_encounter_id / uq_patient_encounter_id (now plain indexes)
--
-- master → master_pg (pii_data_table, master_insurance_table):
--     - Re-keyed from raw patient_id to nd_patient_id (resolved via mapping),
--       matching what the de-id reader expects (pii_table.c.nd_patient_id).
--     - patient_id column dropped.
--     - Column-set-agnostic migration via CREATE TABLE ... LIKE so this file does
--       not need to enumerate every PII / insurance column.
-- =============================================================================


-- =============================================================================
-- SECTION 1: Create mapping_pg database
-- =============================================================================

CREATE DATABASE IF NOT EXISTS mapping_pg
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE mapping_pg;

-- =============================================================================
-- SECTION 2: Create patient_mapping_table
-- =============================================================================

CREATE TABLE IF NOT EXISTS patient_mapping_table (
  nd_patient_id     BIGINT    NOT NULL,
  patientid         BIGINT    NOT NULL,
  `offset`          INT       NOT NULL,
  registration_date DATETIME  DEFAULT NULL,
  created_at        DATETIME  NOT NULL,
  updated_at        DATETIME  NOT NULL,
  PRIMARY KEY (nd_patient_id),
  UNIQUE KEY uq_patientid (patientid),
  INDEX idx_patientid (patientid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- SECTION 3: Create encounter_mapping_table
-- =============================================================================

CREATE TABLE IF NOT EXISTS encounter_mapping_table (
  id              BIGINT   NOT NULL AUTO_INCREMENT,
  nd_patient_id   BIGINT   NOT NULL,
  encounter_id    BIGINT   NOT NULL,
  nd_encounter_id BIGINT   NOT NULL,
  encounter_date  DATETIME DEFAULT NULL,
  nd_ActiveFlag   CHAR(1)  NOT NULL DEFAULT 'Y',
  created_at      DATETIME NOT NULL,
  updated_at      DATETIME NOT NULL,
  patientid       BIGINT   NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_nd_encounter_id (nd_encounter_id),
  INDEX idx_nd_patient_id (nd_patient_id),
  INDEX idx_source_id (encounter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- SECTION 4: Migrate patient data
--   - Rename patient_id → patientid (INT → BIGINT cast is implicit)
--   - Drop: reference_mapping, created_by, updated_by
-- =============================================================================

INSERT INTO mapping_pg.patient_mapping_table
  (nd_patient_id, patientid, `offset`, registration_date, created_at, updated_at)
SELECT
  nd_patient_id,
  CAST(patient_id AS SIGNED),
  `offset`,
  registration_date,
  created_at,
  updated_at
FROM mapping.patient_mapping_table;

-- =============================================================================
-- SECTION 5: Migrate encounter data
--   - Join with mapping.patient_mapping_table to derive nd_patient_id
--   - Drop: patient_id (kept as denormalised patientid), created_by, updated_by
--   - nd_ActiveFlag literal 'Y' (dent's old encounter table has no such column)
--   - DECIMAL(20,0) nd_encounter_id → BIGINT via CAST
-- =============================================================================

INSERT INTO mapping_pg.encounter_mapping_table
  (nd_patient_id, encounter_id, nd_encounter_id, encounter_date, nd_ActiveFlag, created_at, updated_at, patientid)
SELECT
  p.nd_patient_id,
  CAST(e.encounter_id    AS SIGNED),
  CAST(e.nd_encounter_id AS SIGNED),
  e.encounter_date,
  'Y',
  e.created_at,
  e.updated_at,
  CAST(e.patient_id      AS SIGNED)
FROM mapping.encounter_mapping_table e
JOIN mapping.patient_mapping_table   p ON e.patient_id = p.patient_id;

-- =============================================================================
-- SECTION 6: Mapping verification queries
-- =============================================================================

-- Row count parity
SELECT 'mapping.patient'    AS source_table, COUNT(*) AS row_count FROM mapping.patient_mapping_table
UNION ALL
SELECT 'mapping_pg.patient',                 COUNT(*)              FROM mapping_pg.patient_mapping_table;

SELECT 'mapping.encounter'  AS source_table, COUNT(*) AS row_count FROM mapping.encounter_mapping_table
UNION ALL
SELECT 'mapping_pg.encounter',               COUNT(*)              FROM mapping_pg.encounter_mapping_table;

-- Spot-check: nd_patient_id bridge is correctly populated
SELECT
  e.id,
  e.nd_patient_id,
  e.encounter_id,
  e.nd_encounter_id,
  e.patientid,
  p.patientid AS patient_side_patientid
FROM mapping_pg.encounter_mapping_table e
JOIN mapping_pg.patient_mapping_table   p ON e.nd_patient_id = p.nd_patient_id
LIMIT 10;


-- =============================================================================
-- SECTION 7: Create master_pg database
-- =============================================================================

CREATE DATABASE IF NOT EXISTS master_pg
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

-- =============================================================================
-- SECTION 8: Migrate pii_data_table  (master → master_pg, re-keyed to nd_patient_id)
--   Column-set-agnostic: clone the old table structure, copy rows, add
--   nd_patient_id, resolve it from the mapping, then drop the raw patient_id.
-- =============================================================================

-- 8.1 clone structure + data (keeps whatever PII columns exist, incl. patient_id)
CREATE TABLE IF NOT EXISTS master_pg.pii_data_table LIKE master.pii_data_table;
INSERT INTO master_pg.pii_data_table SELECT * FROM master.pii_data_table;

-- 8.2 add the de-identified key and resolve it via the (pre-migration) mapping
ALTER TABLE master_pg.pii_data_table
  ADD COLUMN nd_patient_id BIGINT NULL FIRST;

UPDATE master_pg.pii_data_table t
JOIN mapping.patient_mapping_table m ON t.patient_id = m.patient_id
SET t.nd_patient_id = m.nd_patient_id;

-- 8.3 drop the raw identifier and enforce uniqueness on nd_patient_id
--     (the delta upsert relies on this unique key). NULL nd_patient_id rows are
--     orphan PII with no mapped patient — MySQL permits multiple NULLs here.
ALTER TABLE master_pg.pii_data_table
  DROP COLUMN patient_id,
  ADD UNIQUE KEY uq_nd_patient_id (nd_patient_id);

-- =============================================================================
-- SECTION 9: Migrate master_insurance_table  (master → master_pg, re-keyed)
--   Same pattern. Insurance is one row per patient (the old delta updated by
--   patient_id only), so nd_patient_id is unique here too.
-- =============================================================================

CREATE TABLE IF NOT EXISTS master_pg.master_insurance_table LIKE master.master_insurance_table;
INSERT INTO master_pg.master_insurance_table SELECT * FROM master.master_insurance_table;

ALTER TABLE master_pg.master_insurance_table
  ADD COLUMN nd_patient_id BIGINT NULL FIRST;

UPDATE master_pg.master_insurance_table t
JOIN mapping.patient_mapping_table m ON t.patient_id = m.patient_id
SET t.nd_patient_id = m.nd_patient_id;

ALTER TABLE master_pg.master_insurance_table
  DROP COLUMN patient_id,
  ADD UNIQUE KEY uq_nd_patient_id (nd_patient_id);

-- =============================================================================
-- SECTION 10: Master verification queries
-- =============================================================================

SELECT 'master.pii'    AS source_table, COUNT(*) AS row_count FROM master.pii_data_table
UNION ALL
SELECT 'master_pg.pii',                 COUNT(*)              FROM master_pg.pii_data_table
UNION ALL
SELECT 'master_pg.pii (unmapped nd)',   COUNT(*)              FROM master_pg.pii_data_table WHERE nd_patient_id IS NULL;

SELECT 'master.insurance'    AS source_table, COUNT(*) AS row_count FROM master.master_insurance_table
UNION ALL
SELECT 'master_pg.insurance',                 COUNT(*)              FROM master_pg.master_insurance_table
UNION ALL
SELECT 'master_pg.insurance (unmapped nd)',   COUNT(*)              FROM master_pg.master_insurance_table WHERE nd_patient_id IS NULL;
