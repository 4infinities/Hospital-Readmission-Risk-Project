CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.claims_slim
  CLUSTER BY encounter
AS
SELECT
  cl.appointmentid AS encounter,
  cl.patientid,
  cl.diagnosis1,
  cl.diagnosis2,
  cl.diagnosis3,
  cl.diagnosis4,
  cl.diagnosis5,
  cl.diagnosis6,
  cl.diagnosis7,
  cl.diagnosis8,
  cl.currentillnessdate,
FROM hospital-readmission-4.raw_data.claims cl
JOIN hospital-readmission-4.data_slim.encounters_slim e
  ON cl.appointmentid = e.id
