CREATE OR REPLACE TABLE {{DATASET_SLIM}}.{{PROFILE}}conditions_slim
  CLUSTER BY code
AS
SELECT
  cond.start,
  cond.stop,
  cond.patient,
  cond.encounter,
  cond.code,
  TRIM(SPLIT(cond.description, '(')[OFFSET(0)]) AS diagnosis_name,
  REPLACE(
    TRIM(SPLIT(cond.description, '(')[safe_OFFSET(1)]),
    ')',
    '') AS diagnosis_type
FROM  {{DATASET_RAW}}.conditions cond
JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  ON cond.encounter = e.id
WHERE
  coalesce(
    REPLACE(
      TRIM(SPLIT(cond.description, '(')[safe_OFFSET(1)]),
      ')',
      ''))
  != 'situation'
