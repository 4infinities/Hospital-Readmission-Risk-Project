create or replace table healthcare-test-486920.Raw_csvs_test.conditions_slim
partition by stop
cluster by encounter, patient
as
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
FROM healthcare-test-486920.Raw_csvs_test.conditions cond
JOIN healthcare-test-486920.Raw_csvs_test.encounters_slim e
ON cond.encounter = e.id
where coalesce(REPLACE(
    TRIM(SPLIT(cond.description, '(')[safe_OFFSET(1)]),
    ')',
    '')) != 'situation'
