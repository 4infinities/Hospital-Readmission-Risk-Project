-- diagnoses_dictionary delta: return new SNOMED diagnosis codes seen in the current month not yet in diagnoses_dictionary
-- Feed for DictionaryBuilder: classify and append-only; no DELETE needed
-- Uses current month staging only — previous months were processed in prior iterations; NOT IN check deduplicates
-- Depends on: claims_{{END_DATE_SAFE}}, conditions_{{END_DATE_SAFE}}, diagnoses_dictionary
WITH new_window_codes AS (
  -- Unpivot all 8 diagnosis columns from current month's staging claims
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis1 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis1 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis2 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis2 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis3 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis3 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis4 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis4 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis5 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis5 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis6 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis6 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis7 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis7 IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT CAST(SAFE_CAST(diagnosis8 AS FLOAT64) AS INT64) AS code
  FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis8 IS NOT NULL

  UNION DISTINCT

  -- Also include codes from current month's conditions staging
  SELECT DISTINCT CAST(code AS INT64) AS code
  FROM {{DATASET_RAW}}.conditions_{{END_DATE_SAFE}}
)

-- Return only codes not yet in diagnoses_dictionary; attach name from conditions staging where available
-- conditions staging uses 'description' column (raw Synthea) rather than 'diagnosis_name' (slim alias)
SELECT
  nc.code,
  c.description AS name
FROM new_window_codes nc
LEFT JOIN (
  SELECT DISTINCT CAST(code AS INT64) AS code, description
  FROM {{DATASET_RAW}}.conditions_{{END_DATE_SAFE}}
) c ON nc.code = c.code
WHERE nc.code NOT IN (
  SELECT code FROM {{DATASET_HELPERS}}.diagnoses_dictionary
)
ORDER BY nc.code;
