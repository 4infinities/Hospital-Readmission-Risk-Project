-- Slim careplans: inner join to encounters_slim to retain only careplans linked to valid encounters
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.careplans_slim
  CLUSTER BY patient, encounter
AS
SELECT
  care.Start,
  care.stop,
  care.patient,
  care.encounter,
  care.description,
  care.reasoncode as code
FROM {{DATASET_RAW}}.careplans care
-- Only keep careplans whose triggering encounter passed the encounters_slim filters
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON care.encounter = e.id