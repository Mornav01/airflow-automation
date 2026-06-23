-- =============================================================================
-- Migration: mapping_prod → mapping_pg
-- Creates mapping_pg database with cleaned schema and migrates all existing data
-- for the athenaone project.
--
-- Changes vs mapping_prod:
--   patient_mapping_table:
--     - Deduplicated by patientid (keep row with lowest nd_patient_id per patientid)
--     - nd_patient_id: plain BIGINT, unique via PK
--     - patientid: BIGINT (was VARCHAR(64) in mapping_prod), gets its own UNIQUE KEY
--     - enterpriseid: BIGINT NOT NULL, same value as patientid, with UNIQUE KEY
--     - chartid: REMOVED from this table, moved to chart_mapping_table
--     - Dropped: created_by, updated_by
--     - TIMESTAMP → DATETIME
--
--   chart_mapping_table (NEW):
--     - Mirrors structure of encounter_mapping_table
--     - chart_id     : old chartid value from mapping_prod
--     - nd_chart_id  : old nd_patient_id (de-identified chart surrogate)
--     - nd_patient_id: new nd_patient_id from deduplicated patient_mapping_table
--     - chart_date   : old registration_date
--     - nd_ActiveFlag CHAR(1) DEFAULT 'Y'
--
--   encounter_mapping_table:
--     - nd_patient_id re-mapped to new patientid-based values from mapping_pg
--     - id INT → BIGINT AUTO_INCREMENT
--     - nd_ActiveFlag added (not present in mapping_prod), all rows default to 'Y'
--     - Dropped: created_by, updated_by
--     - TIMESTAMP → DATETIME
--
-- ASSUMPTION: encounter_mapping_table in mapping_prod links via nd_patient_id
--   (chartid-based surrogate). If it links via a raw patient_id column instead,
--   update the JOIN in SECTION 7 from:
--     e.nd_patient_id = old_p.nd_patient_id
--   to:
--     e.patient_id = old_p.patientid
--
-- ASSUMPTION: chartid column in mapping_prod is BIGINT.
--   If it is VARCHAR, update the chart_id column type in SECTION 3.
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
  nd_patient_id     BIGINT       NOT NULL,
  patientid         BIGINT       NOT NULL,
  enterpriseid      BIGINT       NOT NULL,
  `offset`          INT          NOT NULL,
  registration_date DATETIME     DEFAULT NULL,
  created_at        DATETIME     NOT NULL,
  updated_at        DATETIME     NOT NULL,
  PRIMARY KEY (nd_patient_id),
  UNIQUE KEY uq_patientid    (patientid),
  UNIQUE KEY uq_enterpriseid (enterpriseid),
  INDEX idx_patientid    (patientid),
  INDEX idx_enterpriseid (enterpriseid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
-- SECTION 3: Create chart_mapping_table
-- =============================================================================

CREATE TABLE IF NOT EXISTS chart_mapping_table (
  id              BIGINT   NOT NULL AUTO_INCREMENT,
  nd_patient_id   BIGINT   NOT NULL,
  chart_id        BIGINT   NOT NULL,
  nd_chart_id     BIGINT   NOT NULL,
  chart_date      DATETIME DEFAULT NULL,
  nd_ActiveFlag   CHAR(1)  NOT NULL DEFAULT 'Y',
  created_at      DATETIME NOT NULL,
  updated_at      DATETIME NOT NULL,
  enterpriseid    BIGINT   NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_nd_chart_id   (nd_chart_id),
  INDEX idx_nd_patient_id     (nd_patient_id),
  INDEX idx_chart_id          (chart_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
-- SECTION 4: Create encounter_mapping_table
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
  INDEX idx_nd_patient_id       (nd_patient_id),
  INDEX idx_encounter_id        (encounter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
-- SECTION 5: Migrate patient data
--   - Deduplicate by patientid: keep the row with the lowest nd_patient_id
--     per patientid (same logic as the row_number dedup query)
--   - Drop chartid, created_by, updated_by
-- =============================================================================

INSERT INTO mapping_pg.patient_mapping_table
  (nd_patient_id, patientid, enterpriseid, `offset`, registration_date, created_at, updated_at)
SELECT
  nd_patient_id,
  CAST(patientid AS SIGNED),
  CAST(patientid AS SIGNED),
  `offset`,
  registration_date,
  created_at,
  updated_at
FROM (
  SELECT
    nd_patient_id,
    patientid,
    `offset`,
    registration_date,
    created_at,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY patientid ORDER BY nd_patient_id ASC) AS _rn
  FROM mapping_prod.patient_mapping_table
) x
WHERE x._rn = 1;


-- =============================================================================
-- SECTION 6: Migrate chart data to chart_mapping_table
--   - One row per chartid from mapping_prod
--   - chart_id     = old chartid
--   - nd_chart_id  = old nd_patient_id (was assigned per chartid)
--   - nd_patient_id = new nd_patient_id from deduplicated mapping_pg table
--                     (resolved via patientid join)
-- =============================================================================

INSERT INTO mapping_pg.chart_mapping_table
  (nd_patient_id, chart_id, nd_chart_id, chart_date, nd_ActiveFlag, created_at, updated_at, enterpriseid)
SELECT
  new_p.nd_patient_id,
  CAST(old_p.chartid    AS SIGNED) AS chart_id,
  old_p.nd_patient_id              AS nd_chart_id,
  old_p.registration_date          AS chart_date,
  'Y',
  old_p.created_at,
  old_p.updated_at,
  new_p.enterpriseid
FROM mapping_prod.patient_mapping_table old_p
JOIN mapping_pg.patient_mapping_table   new_p ON CAST(old_p.patientid AS SIGNED) = new_p.patientid;


-- =============================================================================
-- SECTION 7: Migrate encounter data
--   - Bridge: encounter.nd_patient_id (chartid-based) → patientid → new nd_patient_id
--   - nd_encounter_id values are kept as-is (opaque surrogates, no reassignment)
--   - See ASSUMPTION note at top if encounter table uses patient_id instead
-- =============================================================================

INSERT INTO mapping_pg.encounter_mapping_table
  (nd_patient_id, encounter_id, nd_encounter_id, encounter_date, nd_ActiveFlag, created_at, updated_at, patientid)
SELECT
  new_p.nd_patient_id,
  CAST(e.encounter_id    AS SIGNED),
  CAST(e.nd_encounter_id AS SIGNED),
  e.encounter_date,
  'Y',
  e.created_at,
  e.updated_at,
  new_p.patientid
FROM mapping_prod.encounter_mapping_table e
JOIN mapping_prod.patient_mapping_table   old_p ON e.nd_patient_id = old_p.nd_patient_id
JOIN mapping_pg.patient_mapping_table     new_p ON CAST(old_p.patientid AS SIGNED) = new_p.patientid;


-- =============================================================================
-- SECTION 8: Verification queries
-- =============================================================================

-- ── 8.1 Row count parity ─────────────────────────────────────────────────────

-- Patient: raw vs distinct vs migrated (distinct patientid count must equal mapping_pg count)
SELECT 'mapping_prod.patient (total rows)'      AS label, COUNT(*)                   AS cnt FROM mapping_prod.patient_mapping_table
UNION ALL
SELECT 'mapping_prod.patient (distinct patientid)',       COUNT(DISTINCT patientid)         FROM mapping_prod.patient_mapping_table
UNION ALL
SELECT 'mapping_pg.patient',                              COUNT(*)                          FROM mapping_pg.patient_mapping_table;

-- Chart: every row in mapping_prod becomes exactly one chart row
SELECT 'mapping_prod.patient (total rows)' AS label, COUNT(*) AS cnt FROM mapping_prod.patient_mapping_table
UNION ALL
SELECT 'mapping_pg.chart',                             COUNT(*)      FROM mapping_pg.chart_mapping_table;

-- Encounter: must match mapping_prod exactly (no rows dropped)
SELECT 'mapping_prod.encounter' AS label, COUNT(*) AS cnt FROM mapping_prod.encounter_mapping_table
UNION ALL
SELECT 'mapping_pg.encounter',             COUNT(*)      FROM mapping_pg.encounter_mapping_table;


-- ── 8.2 Duplicate patientids in mapping_prod (pre-migration audit) ────────────
-- These are the patientids that had multiple chartids — confirm they exist before migration.

SELECT
  patientid,
  COUNT(*)                                   AS chart_count,
  GROUP_CONCAT(chartid ORDER BY chartid)     AS chartids,
  GROUP_CONCAT(nd_patient_id ORDER BY chartid) AS nd_patient_ids_in_prod
FROM mapping_prod.patient_mapping_table
GROUP BY patientid
HAVING COUNT(*) > 1
ORDER BY chart_count DESC;


-- ── 8.3 Core migration correctness: same patientid → same nd_patient_id, different nd_chart_id ──
-- For every patientid that had multiple chartids, verify in chart_mapping_table:
--   - All its charts resolve to the SAME nd_patient_id  (consolidated correctly)
--   - All its charts have a DIFFERENT nd_chart_id       (no collisions)

SELECT
  p.patientid,
  COUNT(c.chart_id)                              AS chart_count,
  COUNT(DISTINCT c.nd_patient_id)                AS distinct_nd_patient_ids,   -- must be 1
  COUNT(DISTINCT c.nd_chart_id)                  AS distinct_nd_chart_ids,     -- must equal chart_count
  GROUP_CONCAT(c.chart_id    ORDER BY c.chart_id) AS chart_ids,
  GROUP_CONCAT(c.nd_chart_id ORDER BY c.chart_id) AS nd_chart_ids,
  MIN(c.nd_patient_id)                            AS nd_patient_id
FROM mapping_pg.patient_mapping_table   p
JOIN mapping_pg.chart_mapping_table     c ON c.nd_patient_id = p.nd_patient_id
GROUP BY p.patientid
HAVING COUNT(c.chart_id) > 1                      -- only show multi-chart patients
ORDER BY chart_count DESC;

-- Expected: distinct_nd_patient_ids = 1, distinct_nd_chart_ids = chart_count for every row.


-- ── 8.4 Flag any violations ──────────────────────────────────────────────────

-- Patients where charts did NOT consolidate to a single nd_patient_id (should return 0 rows)
SELECT
  p.patientid,
  COUNT(DISTINCT c.nd_patient_id) AS distinct_nd_patient_ids
FROM mapping_pg.patient_mapping_table   p
JOIN mapping_pg.chart_mapping_table     c ON c.nd_patient_id = p.nd_patient_id
GROUP BY p.patientid
HAVING COUNT(DISTINCT c.nd_patient_id) > 1;

-- nd_chart_id collisions: same nd_chart_id on two different charts (should return 0 rows)
SELECT
  nd_chart_id,
  COUNT(*) AS collision_count,
  GROUP_CONCAT(chart_id) AS chart_ids
FROM mapping_pg.chart_mapping_table
GROUP BY nd_chart_id
HAVING COUNT(*) > 1;


-- ── 8.5 Orphaned encounters (would have been dropped by migration JOINs) ─────
-- Run BEFORE migration to check for data issues. Should return 0.

SELECT COUNT(*) AS orphaned_encounters
FROM mapping_prod.encounter_mapping_table e
WHERE NOT EXISTS (
  SELECT 1
  FROM mapping_prod.patient_mapping_table old_p
  WHERE old_p.nd_patient_id = e.nd_patient_id
);


-- ── 8.6 Deep dive: inspect a specific patientid end-to-end ───────────────────
-- Replace 31351 with any patientid from query 8.2 to trace it through all tables.

-- Step 1: all chartids and their nd_patient_ids in mapping_prod for this patientid
SELECT patientid, chartid, nd_patient_id, registration_date
FROM mapping_prod.patient_mapping_table
ORDER BY chartid;

-- Step 2: chart_mapping_table rows for those chartids (verify consolidation)
SELECT c.chart_id, c.nd_chart_id, c.nd_patient_id, c.chart_date, c.nd_ActiveFlag
FROM mapping_pg.chart_mapping_table c
WHERE c.chart_id IN (
  SELECT CAST(chartid AS SIGNED)
  FROM mapping_prod.patient_mapping_table
)
ORDER BY c.chart_id;

-- Step 3: encounters in mapping_pg for this patientid (all charts consolidated)
SELECT e.nd_patient_id, e.encounter_id, e.nd_encounter_id, e.encounter_date, e.nd_ActiveFlag
FROM mapping_pg.encounter_mapping_table e
JOIN mapping_pg.patient_mapping_table   p ON e.nd_patient_id = p.nd_patient_id
ORDER BY e.encounter_id;


-- ── 8.7 Spot check: full bridge for a sample of patients ─────────────────────
SELECT
  p.patientid,
  p.nd_patient_id,
  c.chart_id,
  c.nd_chart_id,
  e.encounter_id,
  e.nd_encounter_id
FROM mapping_pg.patient_mapping_table   p
JOIN mapping_pg.chart_mapping_table     c ON c.nd_patient_id = p.nd_patient_id
JOIN mapping_pg.encounter_mapping_table e ON e.nd_patient_id = p.nd_patient_id
LIMIT 20;


CREATE DATABASE IF NOT EXISTS master_pg;
