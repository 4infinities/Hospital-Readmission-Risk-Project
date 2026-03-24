-- patient_known_chronic_codes delta: INSERT new (patient_id, code) pairs discovered in
-- current month's claims that are not already in the table.
-- Must run after D1 (diagnoses_dictionary), before H1 (helper_clinical update).
-- Depends on: claims_{{END_DATE_SAFE}}, encounters_{{END_DATE_SAFE}}, diagnoses_dictionary,
--             patient_known_chronic_codes (existing rows)
WITH
  -- Unpivot diagnosis columns from current month's staging claims
  new_claims_long AS (
    SELECT DISTINCT stay_id, code
    FROM (
      SELECT encounter AS stay_id, CAST(diagnosis1 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis1 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis2 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis2 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis3 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis3 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis4 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis4 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis5 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis5 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis6 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis6 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis7 AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis7 IS NOT NULL
      UNION ALL
      SELECT encounter AS stay_id, CAST(diagnosis8 AS INT64) AS code
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
-- Only insert pairs not already tracked in the table
INSERT INTO {{DATASET_HELPERS}}.patient_known_chronic_codes
SELECT nco.patient_id, nco.code, nco.first_seen_date
FROM new_chronic_onsets nco
LEFT JOIN {{DATASET_HELPERS}}.patient_known_chronic_codes pkcc
  ON nco.patient_id = pkcc.patient_id AND nco.code = pkcc.code
WHERE pkcc.patient_id IS NULL
