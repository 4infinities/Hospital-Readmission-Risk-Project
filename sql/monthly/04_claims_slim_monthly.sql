-- Monthly slim: create isolated claims slim table for this month
-- DDL-only (no DML): CREATE OR REPLACE TABLE creates a standalone per-month table
-- Source: monthly raw staging table claims_{{END_DATE_SAFE}}
-- JOIN to encounters_slim_{{END_DATE_SAFE}} scopes to valid encounters for this month only
CREATE OR REPLACE TABLE {{DATASET_RAW}}.claims_slim_{{END_DATE_SAFE}}
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
  cl.currentillnessdate
FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} cl
JOIN {{DATASET_RAW}}.encounters_slim_{{END_DATE_SAFE}} e
  ON cl.appointmentid = e.id
