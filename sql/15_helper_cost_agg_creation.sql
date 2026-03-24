-- helper_cost_aggregation: one row per encounter with summed procedure costs, medication costs,
-- total stay cost (max of claim cost vs component sum), and cost per day
-- Depends on: encounters_slim, procedures_slim, medications_slim (no dictionary dependency)
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_cost_aggregation
AS
WITH
  -- Sum all procedure base costs per encounter
  procedure_costs AS (
    SELECT
      e.id,
      coalesce(round(sum(proc.base_cost), 2), 0) AS total_procedure_costs
    FROM {{DATASET_SLIM}}.encounters_slim e
    LEFT JOIN {{DATASET_SLIM}}.procedures_slim proc
      ON e.id = proc.encounter
    where e.stop <= {{END_DATE}}
    GROUP BY e.id
  ),
  -- Sum all medication total costs per encounter
  medication_costs AS (
    SELECT
      e.id,
      coalesce(round(sum(med.totalcost), 2), 0) AS total_medication_costs
    FROM {{DATASET_SLIM}}.encounters_slim e
    LEFT JOIN {{DATASET_SLIM}}.medications_slim med
      ON e.id = med.encounter
    where e.stop <= {{END_DATE}}
    GROUP BY e.id
  )
-- Final output: combine costs; total_stay_cost = max(claim cost, component sum) to handle billing discrepancies
SELECT
  e.id AS stay_id,
  e.patient AS patient_id,
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
  -- cost_per_day_stay = non-admission costs divided by length of stay (floored at 1 day)
  round(
    greatest(
      e.total_claim_cost - e.base_encounter_cost,
        proc.total_procedure_costs
        + med.total_medication_costs)
      / greatest(date_diff(e.stop, e.start, day), 1),
    2) AS cost_per_day_stay,
FROM {{DATASET_SLIM}}.encounters_slim e
LEFT JOIN procedure_costs proc
  ON e.id = proc.id
LEFT JOIN medication_costs med
  ON e.id = med.id
  where e.stop <= {{END_DATE}}
