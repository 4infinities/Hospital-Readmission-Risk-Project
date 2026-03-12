WITH
  stats AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNTIF(readmit_90d = 0 AND following_unplanned_admission_flag = 1)
        AS flag_inconsistent,
      COUNTIF(readmit_90d = 1 AND following_stay_id IS NULL)
        AS missing_follow_stay_for_readmit90,
      COUNTIF(readmit_90d = 1 AND days_to_readmit IS NULL)
        AS missing_days_for_readmit90
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_utilization
  )
SELECT
  total_rows,
  flag_inconsistent,
  missing_follow_stay_for_readmit90,
  missing_days_for_readmit90
FROM stats;
