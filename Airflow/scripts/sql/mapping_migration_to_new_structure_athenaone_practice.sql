-- =============================================================================
-- Migration: mapping_prod → mapping_pg
-- Creates mapping_pg database with cleaned schema and migrates all existing data
-- for the athenaone_practice (noran) project.
--
-- Changes vs mapping_prod:
--   patient_mapping_table:
--     - nd_patient_id: AUTO_INCREMENT removed (plain BIGINT, unique via PK)
--     - patientid, profileid, pid: each gets its own UNIQUE KEY
--     - Dropped: created_by, updated_by
--     - TIMESTAMP → DATETIME
--
--   encounter_mapping_table:
--     - nd_patient_id already present — no JOIN required during migration
--     - id INT → BIGINT AUTO_INCREMENT
--     - encounter_id, nd_encounter_id: cast to BIGINT via CAST
--     - encounter_date: nullable DATETIME
--     - Dropped: created_by, updated_by
--     - TIMESTAMP → DATETIME
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
  nd_patient_id     BIGINT   NOT NULL,
  patientid         varchar(64)   NOT NULL,
  profileid         BIGINT   NOT NULL,
  pid               BIGINT   NOT NULL,
  `offset`          INT      NOT NULL,
  registration_date DATETIME DEFAULT NULL,
  created_at        DATETIME NOT NULL,
  updated_at        DATETIME NOT NULL,
  PRIMARY KEY (nd_patient_id),
  UNIQUE KEY uq_patientid (patientid),
  UNIQUE KEY uq_profileid (profileid),
  UNIQUE KEY uq_pid       (pid),
  INDEX idx_patientid (patientid),
  INDEX idx_profileid (profileid),
  INDEX idx_pid       (pid)
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
  PRIMARY KEY (id),
  INDEX idx_nd_patient_id (nd_patient_id),
  INDEX idx_source_id     (encounter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- SECTION 4: Migrate patient data
--   - Carry all three patient identifiers: patientid, profileid, pid
--   - Drop: created_by, updated_by
-- =============================================================================

INSERT INTO mapping_pg.patient_mapping_table
  (nd_patient_id, patientid, profileid, pid, `offset`, registration_date, created_at, updated_at)
SELECT
  nd_patient_id,
  patientid,
  CAST(profileid  AS SIGNED),
  CAST(pid        AS SIGNED),
  `offset`,
  registration_date,
  created_at,
  updated_at
FROM mapping_prod.patient_mapping_table;

-- =============================================================================
-- SECTION 5: Migrate encounter data
--   - nd_patient_id already present in source — no JOIN required
--   - Drop: created_by, updated_by
-- =============================================================================

INSERT INTO mapping_pg.encounter_mapping_table
  (nd_patient_id, encounter_id, nd_encounter_id, encounter_date, nd_ActiveFlag, created_at, updated_at)
SELECT
  nd_patient_id,
  CAST(encounter_id    AS SIGNED),
  CAST(nd_encounter_id AS SIGNED),
  encounter_date,
  nd_ActiveFlag,
  created_at,
  updated_at
FROM mapping_prod.encounter_mapping_table;

-- =============================================================================
-- SECTION 6: Verification queries
-- =============================================================================

-- Row count parity
SELECT 'mapping_prod.patient'  AS source_table, COUNT(*) AS row_count FROM mapping_prod.patient_mapping_table
UNION ALL
SELECT 'mapping_pg.patient',                    COUNT(*)              FROM mapping_pg.patient_mapping_table;

SELECT 'mapping_prod.encounter' AS source_table, COUNT(*) AS row_count FROM mapping_prod.encounter_mapping_table
UNION ALL
SELECT 'mapping_pg.encounter',                   COUNT(*)              FROM mapping_pg.encounter_mapping_table;

-- Spot-check: nd_patient_id bridge is correctly populated
SELECT
  e.id,
  e.nd_patient_id,
  e.encounter_id,
  e.nd_encounter_id,
  p.patientid,
  p.profileid,
  p.pid
FROM mapping_pg.encounter_mapping_table e
JOIN mapping_pg.patient_mapping_table   p ON e.nd_patient_id = p.nd_patient_id
LIMIT 10;


CREATE DATABASE IF NOT EXISTS master_pg;
