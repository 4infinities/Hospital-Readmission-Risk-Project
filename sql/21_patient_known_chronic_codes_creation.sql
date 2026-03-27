-- patient_known_chronic_codes: one row per (patient_id, code) — earliest encounter date this chronic code
-- appeared for each patient. Used by helper_clinical update to count num_chronic_conditions efficiently
-- without scanning full claims_slim history.
-- Depends on: claims_slim, encounters_slim, diagnoses_dictionary
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.patient_known_chronic_codes
AS
WITH
  -- Unpivot all 8 claim diagnosis columns into one row per (stay_id, code)
  claims_long AS (
    SELECT DISTINCT stay_id, code
    FROM (
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis1 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis1 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis2 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis2 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis3 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis3 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis4 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis4 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis5 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis5 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis6 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis6 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis7 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis7 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
      UNION ALL
      SELECT encounter AS stay_id, CAST(SAFE_CAST(diagnosis8 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis8 IS NOT NULL AND currentillnessdate <= {{END_DATE}}
    ) t
  ),
  -- Join to encounters to get patient and encounter start date; filter to chronic codes only
  chronic_onsets AS (
    SELECT
      e.patient AS patient_id,
      cl.code,
      MIN(e.start) AS first_seen_date
    FROM claims_long cl
    JOIN {{DATASET_SLIM}}.encounters_slim e ON e.id = cl.stay_id
    JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict ON cl.code = dict.code
    WHERE dict.is_chronic = 1
      AND e.stop <= {{END_DATE}}
    GROUP BY e.patient, cl.code
  )
SELECT patient_id, code, first_seen_date
FROM chronic_onsets
