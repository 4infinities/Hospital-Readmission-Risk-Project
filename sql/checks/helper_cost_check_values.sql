WITH
  base AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNTIF(stay_id IS NULL) AS null_stay_id_count,
      COUNTIF(admission_cost IS NULL) AS null_admission_cost,
      COUNTIF(total_stay_cost IS NULL) AS null_total_stay_cost,
      COUNTIF(cost_per_day_stay IS NULL) AS null_cost_per_day,
      COUNTIF(admission_cost < 0) AS neg_admission_cost,
      COUNTIF(total_procedure_costs < 0) AS neg_proc_cost,
      COUNTIF(total_medication_costs < 0) AS neg_med_cost,
      COUNTIF(total_stay_cost < 0) AS neg_total_stay_cost,
      COUNTIF(cost_per_day_stay < 0) AS neg_cost_per_day
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation
  ),
  consistency AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNTIF(
        total_stay_cost <
          admission_cost + total_procedure_costs + total_medication_costs - 0.01
      ) AS total_stay_less_than_sum
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation
  )
SELECT
  base.total_rows,
  base.null_stay_id_count,
  base.null_admission_cost,
  base.null_total_stay_cost,
  base.null_cost_per_day,
  base.neg_admission_cost,
  base.neg_proc_cost,
  base.neg_med_cost,
  base.neg_total_stay_cost,
  base.neg_cost_per_day,
  consistency.total_stay_less_than_sum
FROM base
CROSS JOIN consistency;

