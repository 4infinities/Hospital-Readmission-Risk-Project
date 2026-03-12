WITH
  clinical_ids AS (
    SELECT DISTINCT
      stay_id
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical_grouped
  ),
  cost_ids AS (
    SELECT DISTINCT
      stay_id
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation_grouped
  ),
  missing_in_cost AS (
    SELECT
      COUNT(*) AS cnt
    FROM clinical_ids c
    LEFT JOIN cost_ids k
      ON c.stay_id = k.stay_id
    WHERE k.stay_id IS NULL
  ),
  extra_in_cost AS (
    SELECT
      COUNT(*) AS cnt
    FROM cost_ids k
    LEFT JOIN clinical_ids c
      ON k.stay_id = c.stay_id
    WHERE c.stay_id IS NULL
  ),
  dupes AS (
    SELECT
      COUNT(*) AS cnt
    FROM (
      SELECT stay_id, COUNT(*) AS c
      FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation_grouped
      GROUP BY stay_id
      HAVING COUNT(*) > 1
    )
  )
SELECT
  (SELECT COUNT(*) FROM clinical_ids) AS clinical_grouped_stays,
  (SELECT COUNT(*) FROM cost_ids) AS cost_grouped_stays,
  missing_in_cost.cnt AS missing_in_cost,
  extra_in_cost.cnt AS extra_in_cost,
  dupes.cnt AS duplicate_stay_ids
FROM missing_in_cost
CROSS JOIN extra_in_cost
CROSS JOIN dupes;

