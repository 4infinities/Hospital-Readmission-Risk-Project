WITH
  stats AS (
    SELECT
      COUNT(*) AS total_rows,

      -- non-negative counts
      COUNTIF(admissions_365d < 0) AS neg_admissions_365d,
      COUNTIF(tot_length_of_stay_365d < 0) AS neg_tot_los_365d,

      -- flag sanity
      COUNTIF(readmit_30d NOT IN (0,1) OR readmit_30d IS NULL) AS bad_readmit_30d,
      COUNTIF(readmit_90d NOT IN (0,1) OR readmit_90d IS NULL) AS bad_readmit_90d,
      COUNTIF(following_unplanned_admission_flag NOT IN (0,1)
              OR following_unplanned_admission_flag IS NULL) AS bad_follow_flag,

      -- days_to_readmit
      COUNTIF(days_to_readmit < 0) AS neg_days_to_readmit,
      COUNTIF(readmit_90d = 1 AND (days_to_readmit IS NULL OR days_to_readmit > 90))
        AS bad_days_for_readmit90
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_utilization
  )
SELECT
  total_rows,
  neg_admissions_365d,
  neg_tot_los_365d,
  bad_readmit_30d,
  bad_readmit_90d,
  bad_follow_flag,
  neg_days_to_readmit,
  bad_days_for_readmit90
FROM stats;
