WITH
  enc AS (
    SELECT
      COUNT(*) AS encounters_count
    FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim
  ),
  hc AS (
    SELECT
      COUNT(*) AS helper_clinical_count
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical
  ),
  missing_in_helper AS (
    SELECT
      COUNT(*) AS missing_in_helper
    FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
    LEFT JOIN {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical h
      ON e.id = h.stay_id
    WHERE h.stay_id IS NULL
  ),
  extra_in_helper AS (
    SELECT
      COUNT(*) AS extra_in_helper
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical h
    LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      ON h.stay_id = e.id
    WHERE e.id IS NULL
  )
SELECT
  enc.encounters_count,
  hc.helper_clinical_count,
  enc.encounters_count - hc.helper_clinical_count AS difference,
  missing_in_helper.missing_in_helper,
  extra_in_helper.extra_in_helper
FROM enc
CROSS JOIN hc
CROSS JOIN missing_in_helper
CROSS JOIN extra_in_helper;
