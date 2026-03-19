-- helper_cost_aggregation incremental update: DELETE rows for the two-month window, then reinsert fresh calculations
-- Window: DATE_TRUNC(window_start_date, MONTH) - INTERVAL 2 MONTH to window_end_date
-- Depends on: encounters_slim, procedures_slim, medications_slim (no dictionary dependency)
DECLARE window_start DATE DEFAULT DATE_TRUNC({{START_DATE}}, MONTH) - INTERVAL 2 MONTH;
DECLARE window_end   DATE DEFAULT {{END_DATE}};

-- Remove window rows before recalculation
DELETE FROM {{DATASET_HELPERS}}.helper_cost_aggregation
WHERE stay_id IN (
  SELECT id FROM {{DATASET_SLIM}}.encounters_slim
  WHERE start >= window_start AND stop <= window_end
);

-- Reinsert recalculated rows for the two-month window
INSERT INTO {{DATASET_HELPERS}}.helper_cost_aggregation
WITH
  -- Sum all procedure base costs per encounter in the window
  procedure_costs AS (
    SELECT
      e.id,
      coalesce(round(sum(proc.base_cost), 2), 0) AS total_procedure_costs
    FROM {{DATASET_SLIM}}.encounters_slim e
    LEFT JOIN {{DATASET_SLIM}}.procedures_slim proc
      ON e.id = proc.encounter
    WHERE e.start >= window_start AND e.stop <= window_end
    GROUP BY e.id
  ),
  -- Sum all medication total costs per encounter in the window
  medication_costs AS (
    SELECT
      e.id,
      coalesce(round(sum(med.totalcost), 2), 0) AS total_medication_costs
    FROM {{DATASET_SLIM}}.encounters_slim e
    LEFT JOIN {{DATASET_SLIM}}.medications_slim med
      ON e.id = med.encounter
    WHERE e.start >= window_start AND e.stop <= window_end
    GROUP BY e.id
  )
-- Final output: combine costs; total_stay_cost = max(claim cost, component sum) to handle billing discrepancies
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
WHERE e.start >= window_start AND e.stop <= window_end;
