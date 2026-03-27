-- patient_known_chronic_codes: DDL-only CREATE OR REPLACE preserving existing rows + adding new pairs
-- New (patient_id, code) pairs from current month's claims not already in the table
-- Must run after D1 (diagnoses_dictionary), before H1 (helper_clinical update)
-- Depends on: claims_{{END_DATE_SAFE}}, encounters_{{END_DATE_SAFE}}, diagnoses_dictionary,
--             patient_known_chronic_codes (existing rows)
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.patient_known_chronic_codes AS
WITH
  -- Unpivot diagnosis columns from current month's staging claims
  new_claims_long AS (
    SELECT DISTINCT stay_id, code
    FROM (
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis1 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis1 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis2 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis2 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis3 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis3 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis4 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis4 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis5 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis5 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis6 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis6 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis7 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis7 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis8 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis8 IS NOT NULL
    ) t
  ),
  -- Join to encounters staging to get patient and encounter start date; filter chronic codes only
  new_chronic_onsets AS (
    SELECT
      e.patient AS patient_id,
      ncl.code,
      MIN(e.start) AS first_seen_date
    FROM new_claims_long ncl
    JOIN {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}} e ON e.id = ncl.stay_id
    JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict ON ncl.code = dict.code
    WHERE dict.is_chronic = 1
    GROUP BY e.patient, ncl.code
  )
-- Preserve all existing rows, then append only pairs not already tracked
SELECT
  patient_id,
  code,
  first_seen_date
FROM {{DATASET_HELPERS}}.patient_known_chronic_codes
UNION ALL
SELECT
  nco.patient_id,
  nco.code,
  nco.first_seen_date
FROM new_chronic_onsets nco
LEFT JOIN {{DATASET_HELPERS}}.patient_known_chronic_codes pkcc
  ON nco.patient_id = pkcc.patient_id AND nco.code = pkcc.code
WHERE pkcc.patient_id IS NULL
