WITH
  base AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNTIF(stay_id IS NULL) AS null_stay_id,
      COUNTIF(length_of_encounter IS NULL OR length_of_encounter <= 0) AS bad_length,
      COUNTIF(admission_cost < 0) AS neg_admission_cost,
      COUNTIF(total_procedure_costs < 0) AS neg_proc_cost,
      COUNTIF(total_medication_costs < 0) AS neg_med_cost,
      COUNTIF(total_stay_cost < 0) AS neg_total_stay_cost,
      COUNTIF(cost_per_day_stay < 0) AS neg_cost_per_day
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation_grouped
  )
SELECT
  total_rows,
  null_stay_id,
  bad_length,
  neg_admission_cost,
  neg_proc_cost,
  neg_med_cost,
  neg_total_stay_cost,
  neg_cost_per_day
FROM base;
