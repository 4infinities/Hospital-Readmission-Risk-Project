CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.careplans_slim
  CLUSTER BY encounter, patient
AS
SELECT
  care.Start,
  care.stop,
  care.patient,
  care.encounter,
  care.description
FROM hospital-readmission-4.raw_data.careplans care
JOIN hospital-readmission-4.data_slim.encounters_slim e
  ON care.encounter = e.id
