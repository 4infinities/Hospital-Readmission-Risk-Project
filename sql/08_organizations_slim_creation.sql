CREATE OR REPLACE TABLE {{DATASET_SLIM}}.{{PROFILE}}organizations_slim
AS
SELECT
  org.id,
  org.name,
  org.utilization
FROM {{DATASET_RAW}}.organizations org
JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  ON org.id = e.organization
