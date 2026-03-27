-- helper_cost_aggregation DDL-only update: CREATE OR REPLACE preserving pre-window rows + fresh window recalculation
-- Self-contained against monthly staging tables — no encounters_slim, procedures_slim, or medications_slim scan
-- window_encounters, procedure_costs, and medication_costs all built from monthly staging unions
-- Depends on: encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}},
--             procedures_{{END_DATE_SAFE}},  procedures_{{PREV_END_DATE_SAFE}},
--             medications_{{END_DATE_SAFE}},  medications_{{PREV_END_DATE_SAFE}}
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_cost_aggregation AS
WITH
  bounds AS (
    SELECT
      DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH AS window_start,
      DATE({{END_DATE}}) AS window_end
  ),
  window_ids AS (
    SELECT id
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
    UNION DISTINCT
    SELECT id
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  existing AS (
    SELECT
      stay_id,
      patient_id,
      admission_cost,
      total_procedure_costs,
      total_medication_costs,
      total_stay_cost,
      cost_per_day_stay
    FROM {{DATASET_HELPERS}}.helper_cost_aggregation
    WHERE stay_id NOT IN (SELECT id FROM window_ids)
  ),
  -- Window encounters from monthly staging tables (current + prior month)
  window_encounters AS (
    SELECT id, patient, start, stop, base_encounter_cost, total_claim_cost
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
    UNION ALL
    SELECT id, patient, start, stop, base_encounter_cost, total_claim_cost
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  -- All procedures across both monthly staging tables
  window_procedures AS (
    SELECT encounter, base_cost
    FROM {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}}
    UNION ALL
    SELECT encounter, base_cost
    FROM {{DATASET_RAW}}.procedures_{{PREV_END_DATE_SAFE}}
  ),
  -- All medications across both monthly staging tables
  window_medications AS (
    SELECT encounter, totalcost
    FROM {{DATASET_RAW}}.medications_{{END_DATE_SAFE}}
    UNION ALL
    SELECT encounter, totalcost
    FROM {{DATASET_RAW}}.medications_{{PREV_END_DATE_SAFE}}
  ),
  -- Sum all procedure base costs per encounter in the window
  procedure_costs AS (
    SELECT
      we.id,
      COALESCE(ROUND(SUM(proc.base_cost), 2), 0) AS total_procedure_costs
    FROM window_encounters we
    LEFT JOIN window_procedures proc ON we.id = proc.encounter
    GROUP BY we.id
  ),
  -- Sum all medication total costs per encounter in the window
  medication_costs AS (
    SELECT
      we.id,
      COALESCE(ROUND(SUM(med.totalcost), 2), 0) AS total_medication_costs
    FROM window_encounters we
    LEFT JOIN window_medications med ON we.id = med.encounter
    GROUP BY we.id
  ),
  -- Final output: combine costs; total_stay_cost = max(claim cost, component sum) to handle billing discrepancies
  new_rows AS (
    SELECT
      we.id                                     AS stay_id,
      we.patient                                AS patient_id,
      we.base_encounter_cost                    AS admission_cost,
      proc.total_procedure_costs                AS total_procedure_costs,
      med.total_medication_costs                AS total_medication_costs,
      ROUND(
        GREATEST(
          we.total_claim_cost,
          we.base_encounter_cost
            + proc.total_procedure_costs
            + med.total_medication_costs),
        2)                                      AS total_stay_cost,
      ROUND(
        GREATEST(
          we.total_claim_cost - we.base_encounter_cost,
          proc.total_procedure_costs
            + med.total_medication_costs)
          / GREATEST(DATE_DIFF(we.stop, we.start, DAY), 1),
        2)                                      AS cost_per_day_stay
    FROM window_encounters we
    LEFT JOIN procedure_costs proc ON we.id = proc.id
    LEFT JOIN medication_costs med  ON we.id = med.id
  )
SELECT
  stay_id,
  patient_id,
  admission_cost,
  total_procedure_costs,
  total_medication_costs,
  total_stay_cost,
  cost_per_day_stay
FROM existing
UNION ALL
SELECT
  stay_id,
  patient_id,
  admission_cost,
  total_procedure_costs,
  total_medication_costs,
  total_stay_cost,
  cost_per_day_stay
FROM new_rows
