CREATE OR REPLACE TABLE {{DATASET_SLIM}}.{{PROFILE}}careplans_slim
  CLUSTER BY encounter, patient
AS
SELECT
  care.Start,
  care.stop,
  care.patient,
  care.encounter,
  care.description,
  care.reasoncode as code
FROM {{DATASET_RAW}}.careplans care
JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  ON care.encounter = e.id