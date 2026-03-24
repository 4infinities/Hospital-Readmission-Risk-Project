-- related_diagnoses delta: return index stay data for new window encounters
-- Feed for DictionaryBuilder: compute is_related via SNOMED and append-only to related_diagnoses
-- Full patient history used for SNOMED classification; output scoped to new window index stays
-- Depends on: helper_utilization (updated), main_diagnoses (updated)
WITH
  sec_codes AS (
    SELECT
      stay_id,
      following_stay_id AS fol_id,
      readmit_30d,
      readmit_90d,
      -- Main diagnosis of the FOLLOWING (readmission) stay
      main.main_diagnosis_code AS sec_code
    FROM {{DATASET_HELPERS}}.helper_utilization hu
    LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main
      ON hu.following_stay_id = main.id
    -- Scope to new window index stays only
    WHERE hu.stay_id IN (
      SELECT id FROM {{DATASET_SLIM}}.encounters_slim
      WHERE start > LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH) AND stop <= {{END_DATE}}
    )
  )
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
  ON main.id = sec.stay_id;
