-- Monthly slim: create isolated encounters slim table for this month
-- DDL-only (no DML): CREATE OR REPLACE TABLE creates a standalone per-month table
-- Source: monthly raw staging table encounters_{{END_DATE_SAFE}}
-- Filters: valid encounter classes + alive patients
CREATE OR REPLACE TABLE {{DATASET_RAW}}.encounters_slim_{{END_DATE_SAFE}}
AS
WITH
  p AS (
    SELECT
      id AS patient_id,
      deathdate
    FROM {{DATASET_SLIM}}.patients_slim
  )
SELECT
  e.id,
  e.start,
  e.stop,
  e.patient,
  e.organization,
  e.encounterclass,
  e.base_encounter_cost,
  e.total_claim_cost,
  e.description
FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}} e
LEFT JOIN p
  ON e.patient = p.patient_id
WHERE
  e.encounterclass IN (
    'inpatient',
    'emergency',
    'urgentcare',
    'outpatient',
    'ambulatory',
    'virtual',
    'wellness')
  AND e.stop < COALESCE(TIMESTAMP(p.deathdate), TIMESTAMP '9999-12-31')
