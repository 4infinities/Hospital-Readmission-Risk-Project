CREATE OR REPLACE TABLE {{DATASET_SLIM}}.{{PROFILE}}medications_slim
  CLUSTER BY encounter
AS
SELECT
  m.start,
  m.stop,
  m.encounter,
  m.code,
  m.description,
  m.base_cost,
  m.dispenses,
  m.totalcost
FROM {{DATASET_RAW}}.medications m
JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  ON m.encounter = e.id
