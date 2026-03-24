-- Feed query for DictionaryBuilder: collect every distinct SNOMED diagnosis code seen up to END_DATE
-- across all 8 claim diagnosis columns and conditions_slim, then attach a human-readable name where available
WITH all_codes AS (
  -- Unpivot all 8 diagnosis columns from claims into a single code column, unioning with conditions codes
  SELECT DISTINCT CAST(diagnosis1 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis1 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis2 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis2 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis3 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis3 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis4 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis4 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis5 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis5 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis6 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis6 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis7 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis7 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis8 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis8 IS NOT NULL and currentillnessdate <= {{END_DATE}}

  UNION DISTINCT

  -- Also include codes from conditions_slim (may have codes not present in claims)
  SELECT DISTINCT CAST(code AS INT64) AS code
  FROM {{DATASET_SLIM}}.conditions_slim
  where stop <= {{END_DATE}}
)

-- Attach the human-readable name from conditions_slim; NULL where code exists only in claims
SELECT
  ac.code,
  c.diagnosis_name as name
FROM all_codes ac
LEFT JOIN (
  SELECT DISTINCT CAST(code AS INT64) AS code, diagnosis_name
  FROM {{DATASET_SLIM}}.conditions_slim
  where stop <= {{END_DATE}}
) c
ON ac.code = c.code
ORDER BY ac.code;