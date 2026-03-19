-- Feed query for DictionaryBuilder (related_diagnoses): pairs each index stay with its following (readmission) stay's main diagnosis
-- Runs AFTER helper_utilization is built; output feeds build_related_diagnoses to classify careplan-related readmissions
WITH
  -- Get the following stay's main diagnosis code alongside the index stay's readmit flags
  sec_codes AS (
    SELECT
      stay_id,
      following_stay_id AS fol_id,
      readmit_30d,
      readmit_90d,
      -- Main diagnosis of the FOLLOWING (readmission) stay
      main.main_diagnosis_code sec_code
    FROM {{DATASET_HELPERS}}.helper_utilization hu
    LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main
      ON hu.following_stay_id = main.id
  )
-- Output: index stay_id, its main diagnosis code, following stay id and diagnosis, readmit flags
SELECT
  sec.stay_id,
  -- Main diagnosis of the INDEX stay
  main.main_diagnosis_code AS code,
  sec.fol_id,
  sec.sec_code,
  readmit_30d,
  readmit_90d
FROM sec_codes sec
LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main
  ON main.id = sec.stay_id
