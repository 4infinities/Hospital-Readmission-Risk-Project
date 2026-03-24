-- Monthly INSERT: append new month's careplans into careplans_slim
-- Source: monthly raw staging table careplans_{{END_DATE_SAFE}}
-- JOIN to encounters_slim scopes to valid encounters only (this month's encounters already inserted)
INSERT INTO {{DATASET_SLIM}}.careplans_slim
SELECT
  care.Start,
  care.stop,
  care.patient,
  care.encounter,
  care.description,
  care.reasoncode AS code
FROM {{DATASET_RAW}}.careplans_{{END_DATE_SAFE}} care
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON care.encounter = e.id
