-- Slim encounters: strip unused columns and filter to clinically relevant encounter classes
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.encounters_slim
  PARTITION BY DATE_TRUNC(DATE(stop), MONTH)
  CLUSTER BY patient, encounterclass
AS (
  WITH
    -- Anchor point: latest encounter stop in raw data, used to exclude the most recent 30-day window (outcomes not yet known)
    end_date AS (
      SELECT MAX(stop) AS end_ts
      FROM {{DATASET_RAW}}.encounters
    ),
    -- Pull patient deathdate for filtering out post-death encounters
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
  FROM {{DATASET_RAW}}.encounters e
  LEFT JOIN p
    ON e.patient = p.patient_id
  CROSS JOIN end_date
  WHERE
    -- Keep only encounter classes used in the readmission model
    e.encounterclass IN (
      'inpatient',
      'emergency',
      'urgentcare',
      'outpatient',
      'ambulatory',
      'virtual')
    -- Exclude encounters ending in the last 30 days: their readmission outcome is not yet observable
    AND e.stop <= TIMESTAMP_SUB(end_ts, INTERVAL 30 DAY)
    -- Exclude encounters that occurred after the patient died
    AND e.stop < COALESCE(TIMESTAMP(deathdate), end_ts)
)