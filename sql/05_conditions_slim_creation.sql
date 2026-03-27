-- Slim conditions: parse diagnosis_name and diagnosis_type out of the description string; exclude 'situation' type entries
CREATE OR REPLACE TABLE {{DATASET_SLIM}}.conditions_slim
  CLUSTER BY patient, code
AS
SELECT
  cond.start,
  cond.stop,
  cond.patient,
  cond.encounter,
  cond.code,
  -- Extract human-readable name: text before the first '('
  TRIM(SPLIT(cond.description, '(')[OFFSET(0)]) AS diagnosis_name,
  -- Extract SNOMED concept type: text inside the first '(...)'
  REPLACE(
    TRIM(SPLIT(cond.description, '(')[safe_OFFSET(1)]),
    ')',
    '') AS diagnosis_type
FROM  {{DATASET_RAW}}.conditions cond
-- Only keep conditions linked to encounters that passed encounters_slim filters
JOIN {{DATASET_SLIM}}.encounters_slim e
  ON cond.encounter = e.id
WHERE
  -- Exclude 'situation' concept type — not clinically meaningful as a diagnosis
  coalesce(
    REPLACE(
      TRIM(SPLIT(cond.description, '(')[safe_OFFSET(1)]),
      ')',
      ''))
  != 'situation'
