-- Slim organizations: deduplicate to only organizations that appear in encounters_slim
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.organizations_slim
AS
SELECT
  org.id,
  org.name,
  org.utilization
FROM {{DATASET_RAW}}.organizations org
-- Only keep organizations referenced by at least one valid encounter
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON org.id = e.organization
