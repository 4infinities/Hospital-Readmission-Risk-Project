-- 1 row per stay_id
SELECT
  COUNT(*) AS rows_total,
  COUNT(DISTINCT stay_id) AS stays_distinct
FROM `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation`;

-- Coverage of acute encounters
SELECT
  (SELECT COUNT(*) FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim`
   WHERE encounterclass IN ('urgentcare','emergency','inpatient')) AS encounters_acute,
  (SELECT COUNT(*) FROM `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation`) AS cost_rows;

-- Recompute procedure costs from raw and compare
WITH proc_agg AS (
  SELECT
    e.id AS stay_id,
    COALESCE(ROUND(SUM(proc.base_cost), 2), 0) AS total_procedure_costs_recalc
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  LEFT JOIN `healthcare-test-486920.Raw_csvs_test.procedures_slim` proc
    ON e.id = proc.encounter
  WHERE e.encounterclass IN ('urgentcare','emergency','inpatient')
  GROUP BY e.id
)
SELECT
  hc.stay_id,
  hc.total_procedure_costs,
  pa.total_procedure_costs_recalc
FROM `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation` hc
JOIN proc_agg pa USING (stay_id)
WHERE hc.total_procedure_costs != pa.total_procedure_costs_recalc
LIMIT 50;

-- Recompute medication costs from raw and compare
WITH med_agg AS (
  SELECT
    e.id AS stay_id,
    COALESCE(ROUND(SUM(med.totalcost), 2), 0) AS total_medication_costs_recalc
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  LEFT JOIN `healthcare-test-486920.Raw_csvs_test.medications_slim` med
    ON e.id = med.encounter
  WHERE e.encounterclass IN ('urgentcare','emergency','inpatient')
  GROUP BY e.id
)
SELECT
  hc.stay_id,
  hc.total_medication_costs,
  ma.total_medication_costs_recalc
FROM `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation` hc
JOIN med_agg ma USING (stay_id)
WHERE hc.total_medication_costs != ma.total_medication_costs_recalc
LIMIT 50;

-- Check relationship between components and total_stay_cost (before GREATEREST)
SELECT
  stay_id,
  admission_cost,
  total_procedure_costs,
  total_medication_costs,
  total_stay_cost,
  ROUND(admission_cost + total_procedure_costs + total_medication_costs, 2) AS sum_components,
  total_stay_cost - ROUND(admission_cost + total_procedure_costs + total_medication_costs, 2) AS diff_vs_components
FROM `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation`
ORDER BY ABS(diff_vs_components) DESC
LIMIT 50;

-- cost_per_day_stay should be total_stay_cost / LOS (except LOS=0 -> 1)
WITH with_los AS (
  SELECT
    hc.stay_id,
    hc.total_stay_cost,
    hc.cost_per_day_stay,
    GREATEST(DATE_DIFF(e.stop, e.start, DAY), 1) AS los_days
  FROM `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation` hc
  JOIN `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
    ON hc.stay_id = e.id
)
SELECT
  stay_id,
  total_stay_cost,
  cost_per_day_stay,
  los_days,
  ROUND(total_stay_cost / los_days, 2) AS recomputed_cpd,
  cost_per_day_stay - ROUND(total_stay_cost / los_days, 2) AS diff
FROM with_los
WHERE ABS(cost_per_day_stay - ROUND(total_stay_cost / los_days, 2)) > 0.01
LIMIT 50;
