-- Monthly INSERT: append new month's encounters into encounters_slim
-- Source: monthly raw staging table encounters_{{END_DATE_SAFE}}
-- No 30-day cutoff applied — monthly simulation includes all encounters in the window
INSERT INTO {{DATASET_SLIM}}.encounters_slim
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
