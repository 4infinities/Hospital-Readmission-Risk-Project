CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.patients_slim
  CLUSTER BY id
AS
SELECT
  id,
  birthdate,
  deathdate,
  race,
  gender
FROM hospital-readmission-4.raw_data.patients
