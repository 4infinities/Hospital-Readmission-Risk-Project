-- Slim medications: retain cost and dispensing columns; inner join to encounters_slim scopes to valid encounters only
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.medications_slim
  PARTITION BY DATE_TRUNC(stop, MONTH)
  CLUSTER BY encounter, code
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
-- Only keep medications linked to encounters that passed encounters_slim filters
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON m.encounter = e.id
