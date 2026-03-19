SELECT
  DISTINCT
  main.id AS stay_id,
  main.main_diagnosis_code AS code,
  care.code AS sec_code
FROM {{DATASET_HELPERS}}.main_diagnoses main
LEFT JOIN {{DATASET_SLIM}}.encounters_slim e
  ON main.id = e.id
LEFT JOIN {{DATASET_SLIM}}.careplans_slim care
  ON e.patient = care.patient
WHERE
  DATE(e.start) > care.start
  AND DATE(e.start) < COALESCE(
    care.stop,
    {{END_DATE}}
  )