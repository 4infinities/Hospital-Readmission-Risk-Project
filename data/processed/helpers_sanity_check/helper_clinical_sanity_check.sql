
-- 1 row per stay_id? (no duplicates)
SELECT
  COUNT(*) AS rows_total,
  COUNT(DISTINCT stay_id) AS stays_distinct
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical`;

-- Any NULL stay_id?
SELECT COUNT(*) AS null_stay_ids
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical`
WHERE stay_id IS NULL;

-- Helper covers all acute encounters (urgentcare/emergency/inpatient)?
SELECT
  (SELECT COUNT(*) FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim`
   WHERE encounterclass IN ('urgentcare','emergency','inpatient')) AS encounters_acute,
  (SELECT COUNT(*) FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical`) AS helper_clinical_rows;

-- num_diagnoses vs claims_long
-- num_diagnoses vs DISTINCT (stay_id, code) from claims_slim

WITH claims_long_distinct AS (
  SELECT DISTINCT
    stay_id,
    code
  FROM (
    SELECT encounter AS stay_id, CAST(diagnosis1 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis1 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis2 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis2 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis3 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis3 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis4 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis4 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis5 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis5 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis6 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis6 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis7 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis7 IS NOT NULL

    UNION ALL
    SELECT encounter AS stay_id, CAST(diagnosis8 AS INT64) AS code
    FROM `healthcare-test-486920.Raw_csvs_test.claims_slim`
    WHERE diagnosis8 IS NOT NULL
  )
),

claims_prepared_recalc AS (
  SELECT
    stay_id,
    COUNT(*) AS num_diagnoses_recalc
  FROM claims_long_distinct
  GROUP BY stay_id
)

SELECT
  hc.stay_id,
  hc.num_diagnoses,
  cpr.num_diagnoses_recalc
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical` hc
JOIN claims_prepared_recalc cpr
  ON hc.stay_id = cpr.stay_id
WHERE hc.num_diagnoses != cpr.num_diagnoses_recalc
LIMIT 50;


-- main_code joins to code_dictionary and matches a disorder where expected
SELECT
  hc.stay_id,
  hc.main_code,
  hc.main_diagnosis_name,
  hc.main_diagnosis_type,
  cd.diagnosis_type AS dict_type
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical` hc
LEFT JOIN (
  SELECT code,
         ANY_VALUE(diagnosis_type) AS diagnosis_type
  FROM `healthcare-test-486920.Raw_csvs_test.conditions_slim`
  GROUP BY code
) cd
ON hc.main_code = cd.code
WHERE cd.code IS NULL
LIMIT 50;

-- num_chronic_conditions never negative; distribution overview
SELECT
  MIN(num_chronic_conditions) AS min_num_chronic,
  MAX(num_chronic_conditions) AS max_num_chronic,
  APPROX_QUANTILES(num_chronic_conditions, 5) AS quantiles
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical`;


-- planned vs non-planned rates
SELECT
  is_planned,
  COUNT(*) AS n
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical`
GROUP BY is_planned;

-- had_surgery should be 1 only if there is at least one is_surgery=1 procedure
WITH patient_surgeries AS (
  SELECT 
    proc.stop,
    proc.patient,
    dict.is_surgery
  FROM `healthcare-test-486920.Raw_csvs_test.procedures_slim` proc
  LEFT JOIN `healthcare-test-486920.Raw_csvs_test.procedures_dictionary` dict
    ON proc.code = dict.code
  WHERE dict.is_surgery = 1
),

recalc_had_surgery AS (
  SELECT
    e.id AS stay_id,
    MAX(
      CASE
        WHEN DATE_DIFF(DATE(e.start), DATE(ps.stop), MONTH) >= 0
         AND DATE_DIFF(DATE(e.start), DATE(ps.stop), MONTH) < 24
        THEN 1 ELSE 0
      END
    ) AS had_surgery_recalc
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  LEFT JOIN patient_surgeries ps
    ON ps.patient = e.patient
  WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  GROUP BY e.id
)

SELECT
  hc.stay_id,
  hc.had_surgery,
  rs.had_surgery_recalc
FROM `healthcare-test-486920.Raw_csvs_test.helper_clinical` hc
JOIN recalc_had_surgery rs
  ON hc.stay_id = rs.stay_id
WHERE hc.had_surgery != rs.had_surgery_recalc
LIMIT 50;
