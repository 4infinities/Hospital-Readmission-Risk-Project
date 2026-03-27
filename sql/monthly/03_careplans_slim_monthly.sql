-- Monthly append: new month's careplans into careplans_slim
-- DDL-only (no DML): CREATE OR REPLACE preserves existing rows via UNION ALL
-- Source: monthly raw staging table careplans_{{END_DATE_SAFE}}
-- JOIN to encounters_{{END_DATE_SAFE}} scopes to valid encounters for this month only
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.careplans_slim
  CLUSTER BY patient, encounter
AS
SELECT
  Start,
  stop,
  patient,
  encounter,
  description,
  code
FROM {{DATASET_SLIM}}.careplans_slim
UNION ALL
SELECT
  care.Start,
  care.stop,
  care.patient,
  care.encounter,
  care.description,
  care.reasoncode AS code
FROM {{DATASET_RAW}}.careplans_{{END_DATE_SAFE}} care
JOIN {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}} e
  ON care.encounter = e.id
