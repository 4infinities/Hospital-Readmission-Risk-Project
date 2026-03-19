-- diagnoses_dictionary delta: return new SNOMED diagnosis codes seen in the new window not yet in diagnoses_dictionary
-- Feed for DictionaryBuilder: classify and append-only; no DELETE needed
-- Depends on: claims_slim, conditions_slim, diagnoses_dictionary
WITH new_window_codes AS (
  -- Unpivot all 8 diagnosis columns from claims in the new window
  SELECT DISTINCT CAST(diagnosis1 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis1 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis2 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis2 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis3 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis3 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis4 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis4 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis5 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis5 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis6 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis6 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis7 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis7 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
  UNION DISTINCT
  SELECT DISTINCT CAST(diagnosis8 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis8 IS NOT NULL AND currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}

  UNION DISTINCT

  -- Also include codes from conditions_slim in the new window
  SELECT DISTINCT CAST(code AS INT64) AS code
  FROM {{DATASET_SLIM}}.conditions_slim
  WHERE stop > {{START_DATE}} AND stop <= {{END_DATE}}
)

-- Return only codes not yet in diagnoses_dictionary; attach name from conditions_slim where available
SELECT
  nc.code,
  c.diagnosis_name
FROM new_window_codes nc
LEFT JOIN (
  SELECT DISTINCT CAST(code AS INT64) AS code, diagnosis_name
  FROM {{DATASET_SLIM}}.conditions_slim
  WHERE stop > {{START_DATE}} AND stop <= {{END_DATE}}
) c ON nc.code = c.code
WHERE nc.code NOT IN (
  SELECT code FROM {{DATASET_HELPERS}}.diagnoses_dictionary
)
ORDER BY nc.code;
