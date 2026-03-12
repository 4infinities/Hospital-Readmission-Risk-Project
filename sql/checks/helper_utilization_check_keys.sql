WITH
  stats AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNTIF(stay_id IS NULL) AS null_stay_id,
      COUNTIF(encounterclass IS NULL) AS null_encounterclass,
      COUNTIF(start IS NULL) AS null_start,
      COUNTIF(stop IS NULL) AS null_stop
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_utilization
  )
SELECT
  total_rows,
  null_stay_id,
  null_encounterclass,
  null_start,
  null_stop
FROM stats;
