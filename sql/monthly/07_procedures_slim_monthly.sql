-- Monthly INSERT: append new month's procedures into procedures_slim
-- Source: monthly raw staging table procedures_{{END_DATE_SAFE}}
-- JOIN to encounters_slim scopes to valid encounters only (this month's encounters already inserted)
INSERT INTO {{DATASET_SLIM}}.procedures_slim
SELECT
  proc.start,
  proc.stop,
  proc.patient,
  proc.encounter,
  proc.code,
  proc.description,
  proc.base_cost
FROM {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}} proc
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON proc.encounter = e.id
