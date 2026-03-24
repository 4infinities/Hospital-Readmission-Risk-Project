-- Monthly INSERT: append new month's medications into medications_slim
-- Source: monthly raw staging table medications_{{END_DATE_SAFE}}
-- JOIN to encounters_slim scopes to valid encounters only (this month's encounters already inserted)
INSERT INTO {{DATASET_SLIM}}.medications_slim
SELECT
  m.start,
  m.stop,
  m.encounter,
  m.code,
  m.description,
  m.base_cost,
  m.dispenses,
  m.totalcost
FROM {{DATASET_RAW}}.medications_{{END_DATE_SAFE}} m
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON m.encounter = e.id
