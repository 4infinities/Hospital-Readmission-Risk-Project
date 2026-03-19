CREATE OR REPLACE TABLE {{DATASET_SLIM}}.encounters_slim
  CLUSTER BY id, patient, stop
AS (
  WITH
    end_date AS (
      SELECT MAX(stop) AS end_ts
      FROM {{DATASET_RAW}}.encounters
    ),
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
    e.encounterclass IN (
      'inpatient',
      'emergency',
      'urgentcare',
      'outpatient',
      'ambulatory',
      'virtual')
    AND e.stop <= TIMESTAMP_SUB(end_ts, INTERVAL 30 DAY)
    AND e.stop < COALESCE(TIMESTAMP(deathdate), end_ts)
)