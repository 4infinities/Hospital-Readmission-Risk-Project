-- Monthly INSERT: append new month's claims into claims_slim
-- Source: monthly raw staging table claims_{{END_DATE_SAFE}}
-- JOIN to encounters_slim scopes to valid encounters only (this month's encounters already inserted)
INSERT INTO {{DATASET_SLIM}}.claims_slim
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
  cl.currentillnessdate
FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} cl
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON cl.appointmentid = e.id
