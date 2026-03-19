-- helper_cost_aggregation_grouped incremental update: DELETE rows for the two-month window, then reinsert fresh calculations
-- Full patient history is used for group boundary detection; output restricted to window group representatives
-- Depends on: encounters_slim, helper_cost_aggregation
DECLARE window_start DATE DEFAULT DATE_TRUNC({{START_DATE}}, MONTH) - INTERVAL 2 MONTH;
DECLARE window_end   DATE DEFAULT {{END_DATE}};

-- Remove window group rows before recalculation (stay_id = group representative encounter id)
DELETE FROM {{DATASET_HELPERS}}.helper_cost_aggregation_grouped
WHERE stay_id IN (
  SELECT id FROM {{DATASET_SLIM}}.encounters_slim
  WHERE start >= window_start AND stop <= window_end
);

-- Reinsert recalculated group rows whose representative encounter falls in the two-month window
INSERT INTO {{DATASET_HELPERS}}.helper_cost_aggregation_grouped
WITH
  -- Assign type_flag rank and detect group boundaries (same logic as clinical grouping)
  -- Full history up to window_end required for correct cumulative group_number assignment
  group_flags AS (
    SELECT
      id,
      patient,
      start,
      stop,
      CASE encounterclass
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual' THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency' THEN 5
        WHEN 'inpatient' THEN 6
        ELSE 99
        END type_flag,
      CASE
        WHEN
          date_diff(
            start,
            lag(stop, 1) OVER (PARTITION BY patient ORDER BY start ASC),
            hour)
          < 12
          THEN 0
        ELSE 1
        END AS group_change
    FROM {{DATASET_SLIM}}.encounters_slim
    WHERE stop <= window_end
  ),
  -- Cumulative sum yields a monotonically increasing group_number per patient
  clusters AS (
    SELECT
      id,
      patient,
      start,
      stop,
      type_flag,
      sum(group_change)
        OVER (
          PARTITION BY patient
          ORDER BY start ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) group_number
    FROM group_flags
  ),
  -- Elect the representative encounter per group (highest type_flag, then earliest start)
  best_stay_per_group AS (
    SELECT
      patient,
      group_number,
      id AS group_id,
      type_flag,
      ROW_NUMBER()
        OVER (
          PARTITION BY patient, group_number
          ORDER BY
            type_flag DESC,  -- highest type_flag wins
            start ASC,  -- tie-breaker: earliest start
            id ASC  -- final tie-breaker
        ) AS rn
    FROM clusters
  ),
  -- Compute total group length as span from earliest start to latest stop (floored at 1 day)
  starts_and_stops AS (
    SELECT
      patient,
      group_number,
      greatest(date_diff(max(stop), min(start), day), 1) length_of_encounter
    FROM clusters
    GROUP BY patient, group_number
  ),
  -- Map each member encounter to its group_id, length, and class label
  final_groups AS (
    SELECT
      clust.id,
      best.group_id,
      sas.length_of_encounter,
      CASE best.type_flag
        WHEN 1 THEN 'ambulatory'
        WHEN 2 THEN 'outpatient'
        WHEN 3 THEN 'virtual'
        WHEN 4 THEN 'urgentcare'
        WHEN 5 THEN 'emergency'
        WHEN 6 THEN 'inpatient'
        ELSE 'unknown'
        END AS encounterclass
    FROM clusters clust
    LEFT JOIN best_stay_per_group best
      ON
        best.patient = clust.patient
        AND best.group_number = clust.group_number
        AND best.rn = 1
    LEFT JOIN starts_and_stops sas
      ON clust.patient = sas.patient
      AND clust.group_number = sas.group_number
  )
-- Aggregate costs from helper_cost_aggregation across all clinical group members; cost_per_day uses group-level length
-- Only output groups whose representative encounter falls within the two-month window
SELECT
  group_id AS stay_id,
  max(final.length_of_encounter) AS length_of_encounter,
  max(hc.admission_cost) AS admission_cost,
  sum(hc.total_procedure_costs) AS total_procedure_costs,
  sum(hc.total_medication_costs) AS total_medication_costs,
  sum(hc.total_stay_cost) AS total_stay_cost,
  round(
    (sum(hc.total_procedure_costs) + sum(hc.total_medication_costs))
    / max(final.length_of_encounter),
    2) AS cost_per_day_stay
FROM final_groups final
LEFT JOIN {{DATASET_HELPERS}}.helper_cost_aggregation hc
  ON final.id = hc.stay_id
WHERE
  final.encounterclass IN ('urgentcare', 'inpatient', 'emergency')
  AND final.group_id IN (
    SELECT id FROM {{DATASET_SLIM}}.encounters_slim
    WHERE start >= window_start AND stop <= window_end
  )
GROUP BY final.group_id;
