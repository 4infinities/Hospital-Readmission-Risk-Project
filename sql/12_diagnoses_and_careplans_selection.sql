WITH
end_date AS (
  SELECT MAX(stop) AS end_ts
  FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim
)
SELECT
  DISTINCT
  main.id AS stay_id,
  main.main_diagnosis_code AS code,
  care.code AS sec_code
FROM {{DATASET_HELPERS}}.{{PROFILE}}main_diagnoses main
LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  ON main.id = e.id
LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}careplans_slim care
  ON e.patient = care.patient
WHERE
  DATE(e.start) > care.start
  AND DATE(e.start) < COALESCE(
    care.stop,
    DATE((SELECT end_ts FROM end_date))
  )