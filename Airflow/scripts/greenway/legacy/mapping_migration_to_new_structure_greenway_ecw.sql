-- =============================================================================
-- Migration: mapping → mapping_pg
-- Creates mapping_pg database with cleaned schema and migrates all existing data.
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
--     - Removed patient_id (replaced by nd_patient_id bridge)
--     - id INT → BIGINT AUTO_INCREMENT
--     - encounter_id INT → BIGINT
--     - nd_encounter_id DECIMAL(20,0) → BIGINT
--     - encounter_date: nullable DATETIME
--     - Dropped: created_by, updated_by
--     - TIMESTAMP → DATETIME
--     - Removed unique keys uq_nd_encounter_id, uq_encounter_id (now plain indexes)
-- =============================================================================

-- =============================================================================
-- SECTION 1: Create database
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
  CAST(patient_id       AS SIGNED),
  `offset`,
  registration_date,
  created_at,
  updated_at
FROM mapping.patient_mapping_table;

-- =============================================================================
-- SECTION 5: Migrate encounter data
--   - Join with mapping.patient_mapping_table to derive nd_patient_id
--   - Drop: patient_id, created_by, updated_by
--   - DECIMAL(20,0) nd_encounter_id → BIGINT via CAST
-- =============================================================================

INSERT INTO mapping_pg.encounter_mapping_table
  (nd_patient_id, encounter_id, nd_encounter_id, encounter_date, nd_ActiveFlag, created_at, updated_at, patientid)
SELECT
  p.nd_patient_id,
  CAST(e.encounter_id    AS SIGNED),
  CAST(e.nd_encounter_id AS SIGNED),
  e.encounter_date,
  e.nd_ActiveFlag,
  e.created_at,
  e.updated_at,
  CAST(e.patient_id      AS SIGNED)
FROM mapping.encounter_mapping_table e
JOIN mapping.patient_mapping_table   p ON e.patient_id = p.patient_id;

-- =============================================================================
-- SECTION 6: Verification queries
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
  p.patientid
FROM mapping_pg.encounter_mapping_table e
JOIN mapping_pg.patient_mapping_table   p ON e.nd_patient_id = p.nd_patient_id
LIMIT 10;


CREATE DATABASE IF NOT EXISTS master_pg;
