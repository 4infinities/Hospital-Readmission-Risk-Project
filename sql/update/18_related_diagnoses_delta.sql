-- related_diagnoses delta: return index stay data for current month window encounters
-- Feed for DictionaryBuilder: compute is_related via SNOMED and append-only to related_diagnoses
-- Window encounter ids sourced from monthly staging tables — no encounters_slim scan
-- Depends on: helper_utilization (updated), main_diagnoses (updated),
--             encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}}
WITH
  bounds AS (
    SELECT
      LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH) AS window_start,
      DATE({{END_DATE}}) AS window_end
  ),
  -- Window encounter ids from monthly staging tables (current + prior month)
  -- These are the index stays whose related_diagnoses entries need to be appended
  window_encounter_ids AS (
    SELECT id FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) > bounds.window_start AND DATE(stop) <= bounds.window_end
    UNION DISTINCT
    SELECT id FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) > bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  sec_codes AS (
    SELECT
      hu.stay_id,
      hu.following_stay_id AS fol_id,
      hu.readmit_30d,
      hu.readmit_90d,
      -- Main diagnosis of the FOLLOWING (readmission) stay
      main.main_diagnosis_code AS sec_code
    FROM {{DATASET_HELPERS}}.helper_utilization hu
    LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main
      ON hu.following_stay_id = main.id
    -- Scope to new window index stays only
    WHERE hu.stay_id IN (SELECT id FROM window_encounter_ids)
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
