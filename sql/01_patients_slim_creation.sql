-- Slim patients: retain only demographic columns needed downstream; cluster by id for join performance
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.patients_slim
  CLUSTER BY id
AS
SELECT
  id,
  birthdate,
  deathdate,
  race,
  gender
FROM {{DATASET_RAW}}.patients