WITH
  counts AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNT(DISTINCT stay_id) AS distinct_stay_ids
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical_grouped
  )
SELECT
  total_rows,
  distinct_stay_ids,
  total_rows - distinct_stay_ids AS duplicate_rows
FROM counts;

