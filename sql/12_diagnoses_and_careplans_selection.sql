-- Feed query for DictionaryBuilder (careplans_related_diagnoses): for each stay, find any active care plan at the time of the encounter
-- A care plan is "active" if the encounter started after the plan started and before the plan stopped (or END_DATE if plan is ongoing)
-- Output feeds build_careplans_related_diagnoses to identify which encounters are readmissions under an active care plan
SELECT
  DISTINCT
  main.id AS stay_id,
  -- Main diagnosis code of the index encounter
  main.main_diagnosis_code AS code,
  -- Reason code of the active care plan (used to classify careplan-related readmissions)
  care.code AS sec_code
FROM {{DATASET_HELPERS}}.main_diagnoses main
LEFT JOIN {{DATASET_SLIM}}.encounters_slim e
  ON main.id = e.id
LEFT JOIN {{DATASET_SLIM}}.careplans_slim care
  ON e.patient = care.patient
WHERE
  -- Care plan was already active before the encounter started
  DATE(e.start) > care.start
  -- Care plan had not yet ended at encounter start; use END_DATE for plans with no recorded stop
  AND DATE(e.start) < COALESCE(
    care.stop,
    {{END_DATE}}
  )