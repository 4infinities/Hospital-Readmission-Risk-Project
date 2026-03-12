CREATE OR REPLACE TABLE {{DATASET_SLIM}}.{{PROFILE}}patients_slim
  CLUSTER BY id
AS
SELECT
  id,
  birthdate,
  deathdate,
  race,
  gender
FROM {{DATASET_RAW}}.patients