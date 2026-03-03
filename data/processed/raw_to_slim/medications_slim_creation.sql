CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.medications_slim
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
FROM hospital-readmission-4.raw_data.medications m
JOIN hospital-readmission-4.data_slim.encounters_slim e
  ON m.encounter = e.id
