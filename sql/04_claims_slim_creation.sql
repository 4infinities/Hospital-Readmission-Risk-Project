CREATE OR REPLACE TABLE {{DATASET_SLIM}}.claims_slim
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
FROM {{DATASET_RAW}}.claims cl
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON cl.appointmentid = e.id