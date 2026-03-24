-- Slim claims: retain diagnosis codes (SNOMED) and illness date; inner join to encounters_slim scopes to valid encounters only
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.claims_slim
  PARTITION BY TIMESTAMP_TRUNC(currentillnessdate, MONTH)
  CLUSTER BY patientid, encounter
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
-- Only keep claims linked to encounters that passed encounters_slim filters
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON cl.appointmentid = e.id