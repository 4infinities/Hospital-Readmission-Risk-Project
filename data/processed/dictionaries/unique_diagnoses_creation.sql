
create or replace table healthcare-test-486920.Raw_csvs_test.unique_diagnoses as 
WITH all_codes AS (
  SELECT DISTINCT CAST(diagnosis1 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis1 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis2 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis2 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis3 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis3 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis4 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis4 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis5 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis5 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis6 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis6 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis7 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis7 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis8 AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis8 IS NOT NULL

  UNION DISTINCT

  SELECT DISTINCT CAST(code AS INT64) AS code
  FROM `healthcare-test-486920.Raw_csvs_test.conditions_slim`
)

SELECT
  ac.code,
  c.diagnosis_name
FROM all_codes ac
LEFT JOIN (
  SELECT DISTINCT CAST(code AS INT64) AS code, diagnosis_name
  FROM `healthcare-test-486920.Raw_csvs_test.conditions_slim`
) c
ON ac.code = c.code
ORDER BY ac.code;

