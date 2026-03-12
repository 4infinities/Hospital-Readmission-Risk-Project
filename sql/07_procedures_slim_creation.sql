CREATE OR REPLACE TABLE {{DATASET_SLIM}}.{{PROFILE}}procedures_slim
  CLUSTER BY encounter
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
JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  ON proc.encounter = e.id
