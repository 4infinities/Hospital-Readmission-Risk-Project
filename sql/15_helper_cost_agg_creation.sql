CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation
AS
WITH
  procedure_costs AS (
    SELECT
      e.id,
      coalesce(round(sum(proc.base_cost), 2), 0) AS total_procedure_costs
    FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
    LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}procedures_slim proc
      ON e.id = proc.encounter
    GROUP BY e.id
  ),
  medication_costs AS (
    SELECT
      e.id,
      coalesce(round(sum(med.totalcost), 2), 0) AS total_medication_costs
    FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
    LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}medications_slim med
      ON e.id = med.encounter
    GROUP BY e.id
  )
SELECT
  e.id AS stay_id,
  e.base_encounter_cost AS admission_cost,
  proc.total_procedure_costs,
  med.total_medication_costs,
  round(
    greatest(
      e.total_claim_cost,
      e.base_encounter_cost
        + proc.total_procedure_costs
        + med.total_medication_costs),
    2) AS total_stay_cost,
  round(
    greatest(
      e.total_claim_cost - e.base_encounter_cost,
        proc.total_procedure_costs
        + med.total_medication_costs)
      / greatest(date_diff(e.stop, e.start, day), 1),
    2) AS cost_per_day_stay,
FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
LEFT JOIN procedure_costs proc
  ON e.id = proc.id
LEFT JOIN medication_costs med
  ON e.id = med.id
