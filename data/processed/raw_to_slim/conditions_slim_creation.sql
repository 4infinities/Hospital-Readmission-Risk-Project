CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.conditions_slim
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
FROM hospital-readmission-4.raw_data.conditions cond
JOIN hospital-readmission-4.data_slim.encounters_slim e
  ON cond.encounter = e.id
WHERE
  coalesce(
    REPLACE(
      TRIM(SPLIT(cond.description, '(')[safe_OFFSET(1)]),
      ')',
      ''))
  != 'situation'
