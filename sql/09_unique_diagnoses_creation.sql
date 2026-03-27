-- Feed query for DictionaryBuilder: collect every distinct SNOMED diagnosis code seen up to END_DATE
-- across all 8 claim diagnosis columns and conditions_slim, then attach a human-readable name where available
-- Note: claims diagnosis columns are STRING with float notation (e.g. "157265008.0"); cast via FLOAT64 first.
WITH all_codes AS (
  -- Unpivot all 8 diagnosis columns from claims into a single code column, unioning with conditions codes
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis1 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis1 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis2 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis2 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis3 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis3 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis4 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis4 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis5 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis5 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis6 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis6 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis7 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis7 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis8 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis8 IS NOT NULL and currentillnessdate <= {{END_DATE}}

  UNION DISTINCT

  -- Also include codes from conditions_slim (may have codes not present in claims)
  SELECT DISTINCT CAST(SAFE_CAST(code AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_SLIM}}.conditions_slim
  where stop <= {{END_DATE}}
)

-- Attach the human-readable name from conditions_slim; NULL where code exists only in claims
SELECT
  ac.code,
  c.diagnosis_name as name
FROM all_codes ac
LEFT JOIN (
  SELECT DISTINCT CAST(SAFE_CAST(code AS FLOAT64) AS INT64) AS code, diagnosis_name
  FROM {{DATASET_SLIM}}.conditions_slim
  where stop <= {{END_DATE}}
) c
ON ac.code = c.code
ORDER BY ac.code;
