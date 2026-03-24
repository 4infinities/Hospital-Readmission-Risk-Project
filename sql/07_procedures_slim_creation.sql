-- Slim procedures: retain code, timing, and cost columns; inner join to encounters_slim scopes to valid encounters only
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.procedures_slim
  PARTITION BY TIMESTAMP_TRUNC(stop, MONTH)
  CLUSTER BY patient, encounter
AS
SELECT
  proc.start,
  proc.stop,
  proc.patient,
  proc.encounter,
  proc.code,
  proc.description,
  proc.base_cost
FROM {{DATASET_RAW}}.procedures proc
-- Only keep procedures linked to encounters that passed encounters_slim filters
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON proc.encounter = e.id
