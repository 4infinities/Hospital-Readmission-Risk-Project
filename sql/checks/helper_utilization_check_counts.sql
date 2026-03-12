WITH
  clinical_ids AS (
    SELECT DISTINCT
      stay_id
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical_grouped
  ),
  util_ids AS (
    SELECT DISTINCT
      stay_id
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_utilization
  ),
  missing_in_util AS (
    SELECT
      COUNT(*) AS cnt
    FROM clinical_ids c
    LEFT JOIN util_ids u
      ON c.stay_id = u.stay_id
    WHERE u.stay_id IS NULL
  ),
  extra_in_util AS (
    SELECT
      COUNT(*) AS cnt
    FROM util_ids u
    LEFT JOIN clinical_ids c
      ON u.stay_id = c.stay_id
    WHERE c.stay_id IS NULL
  ),
  dupes AS (
    SELECT
      COUNT(*) AS cnt
    FROM (
      SELECT stay_id, COUNT(*) AS c
      FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_utilization
      GROUP BY stay_id
      HAVING COUNT(*) > 1
    )
  )
SELECT
  (SELECT COUNT(*) FROM clinical_ids) AS clinical_grouped_stays,
  (SELECT COUNT(*) FROM util_ids) AS util_stays,
  missing_in_util.cnt AS missing_in_util,
  extra_in_util.cnt AS extra_in_util,
  dupes.cnt AS duplicate_stay_ids
FROM missing_in_util
CROSS JOIN extra_in_util
CROSS JOIN dupes;

