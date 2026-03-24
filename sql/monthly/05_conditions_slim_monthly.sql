-- Monthly INSERT: append new month's conditions into conditions_slim
-- Source: monthly raw staging table conditions_{{END_DATE_SAFE}}
-- JOIN to encounters_slim scopes to valid encounters only (this month's encounters already inserted)
INSERT INTO {{DATASET_SLIM}}.conditions_slim
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
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON cond.encounter = e.id
WHERE
  COALESCE(
    REPLACE(
      TRIM(SPLIT(cond.description, '(')[SAFE_OFFSET(1)]),
      ')',
      ''))
  != 'situation'
