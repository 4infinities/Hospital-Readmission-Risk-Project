-- Monthly slim: create isolated conditions slim table for this month
-- DDL-only (no DML): CREATE OR REPLACE TABLE creates a standalone per-month table
-- Source: monthly raw staging table conditions_{{END_DATE_SAFE}}
-- JOIN to encounters_slim_{{END_DATE_SAFE}} scopes to valid encounters for this month only
CREATE OR REPLACE TABLE {{DATASET_RAW}}.conditions_slim_{{END_DATE_SAFE}}
AS
SELECT
  cond.start,
  cond.stop,
  cond.patient,
  cond.encounter,
  cond.code,
  TRIM(SPLIT(cond.description, '(')[OFFSET(0)]) AS diagnosis_name,
  REPLACE(
    TRIM(SPLIT(cond.description, '(')[SAFE_OFFSET(1)]),
    ')',
    '') AS diagnosis_type
FROM {{DATASET_RAW}}.conditions_{{END_DATE_SAFE}} cond
JOIN {{DATASET_RAW}}.encounters_slim_{{END_DATE_SAFE}} e
  ON cond.encounter = e.id
WHERE
  COALESCE(
    REPLACE(
      TRIM(SPLIT(cond.description, '(')[SAFE_OFFSET(1)]),
      ')',
      ''))
  != 'situation'
