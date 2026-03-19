-- related_diagnoses incremental update: DELETE rows for the two-month window, then reinsert fresh calculations
-- Post-helper dictionary: must run AFTER helper_utilization is updated
-- Depends on: helper_utilization, main_diagnoses
DECLARE window_start DATE DEFAULT DATE_TRUNC({{START_DATE}}, MONTH) - INTERVAL 2 MONTH;
DECLARE window_end   DATE DEFAULT {{END_DATE}};

-- Remove window rows before recalculation
DELETE FROM {{DATASET_HELPERS}}.related_diagnoses
WHERE stay_id IN (
  SELECT id FROM {{DATASET_SLIM}}.encounters_slim
  WHERE start >= window_start AND stop <= window_end
);

-- Reinsert recalculated rows for the two-month window
INSERT INTO {{DATASET_HELPERS}}.related_diagnoses
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
    -- Restrict to index stays within the two-month window
    WHERE hu.stay_id IN (
      SELECT id FROM {{DATASET_SLIM}}.encounters_slim
      WHERE start >= window_start AND stop <= window_end
    )
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
  ON main.id = sec.stay_id;
