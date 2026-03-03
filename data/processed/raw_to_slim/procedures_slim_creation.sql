CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.procedures_slim
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
FROM hospital-readmission-4.raw_data.procedures proc
JOIN hospital-readmission-4.data_slim.encounters_slim e
  ON proc.encounter = e.id
