-- helper_cost_aggregation_grouped incremental update: DELETE rows for the two-month window, then reinsert
-- Self-referential architecture: grouping runs on staging-table window encounters only.
-- prior_group_anchor derived from helper_clinical_grouped + helper_utilization (no encounters_slim scan).
-- Groups that continue a prior group (gap < 12h to prior stop) are skipped — prior row is retained.
-- Depends on: encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}},
--             helper_cost_aggregation (freshly updated by H2), helper_clinical_grouped, helper_utilization
DECLARE window_start DATE DEFAULT DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH;
DECLARE window_end   DATE DEFAULT {{END_DATE}};

-- Remove window group rows before recalculation
DELETE FROM {{DATASET_HELPERS}}.helper_cost_aggregation_grouped
WHERE stay_id IN (
  SELECT id FROM {{DATASET_SLIM}}.encounters_slim
  WHERE start >= window_start AND stop <= window_end
);

-- Reinsert recalculated group rows for new groups in the two-month window
INSERT INTO {{DATASET_HELPERS}}.helper_cost_aggregation_grouped
WITH
  -- Window encounters with type_flags: union of current and prior month staging tables
  window_encounters AS (
    SELECT
      id, patient, start, stop, encounterclass,
      CASE encounterclass
        WHEN 'wellness'   THEN 0
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual'    THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency'  THEN 5
        WHEN 'inpatient'  THEN 6
        ELSE 99
      END AS type_flag
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}
    WHERE start >= window_start AND stop <= window_end
    UNION ALL
    SELECT
      id, patient, start, stop, encounterclass,
      CASE encounterclass
        WHEN 'wellness'   THEN 0
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual'    THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency'  THEN 5
        WHEN 'inpatient'  THEN 6
        ELSE 99
      END AS type_flag
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}
    WHERE start >= window_start AND stop <= window_end
  ),
  -- Most recent prior group stop per window patient, from helper_utilization
  prior_group_anchor AS (
    SELECT
      wp.patient,
      MAX(hu.stop) AS last_prior_stop
    FROM (SELECT DISTINCT patient FROM window_encounters) wp
    JOIN {{DATASET_HELPERS}}.helper_clinical_grouped hcg ON hcg.patient_id = wp.patient
    JOIN {{DATASET_HELPERS}}.helper_utilization hu ON hcg.stay_id = hu.stay_id
    GROUP BY wp.patient
  ),
  -- Group boundary detection for window encounters only
  group_flags AS (
    SELECT
      we.id, we.patient, we.start, we.stop, we.type_flag,
      CASE
        WHEN DATE_DIFF(we.start,
          COALESCE(
            LAG(we.stop) OVER (PARTITION BY we.patient ORDER BY we.start ASC),
            pga.last_prior_stop
          ), hour) < 12
        THEN 0
        ELSE 1
      END AS group_change
    FROM window_encounters we
    LEFT JOIN prior_group_anchor pga ON we.patient = pga.patient
  ),
  -- Cumulative group number per patient within window
  clusters AS (
    SELECT
      id, patient, start, stop, type_flag,
      SUM(group_change) OVER (
        PARTITION BY patient ORDER BY start ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS group_number
    FROM group_flags
  ),
  -- Detect patients whose first window encounter continues a prior group
  first_group_change_per_patient AS (
    SELECT patient, group_change AS first_group_change
    FROM group_flags
    QUALIFY ROW_NUMBER() OVER (PARTITION BY patient ORDER BY start ASC) = 1
  ),
  -- Elect the representative encounter per group
  best_stay_per_group AS (
    SELECT
      patient,
      group_number,
      id AS group_id,
      type_flag AS best_type_flag,
      ROW_NUMBER() OVER (
        PARTITION BY patient, group_number
        ORDER BY type_flag DESC, start ASC, id ASC
      ) AS rn
    FROM clusters
  ),
  -- Compute group-level length from window encounter min/max dates
  starts_and_stops AS (
    SELECT
      patient,
      group_number,
      GREATEST(DATE_DIFF(MAX(stop), MIN(start), DAY), 1) AS length_of_encounter
    FROM clusters
    GROUP BY patient, group_number
  ),
  -- Map each window encounter to its group representative, length, and class label
  final_groups AS (
    SELECT
      clust.id,
      clust.patient,
      best.group_id,
      sas.length_of_encounter,
      CASE best.best_type_flag
        WHEN 0 THEN 'wellness'
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
      ON best.patient = clust.patient
      AND best.group_number = clust.group_number
      AND best.rn = 1
    LEFT JOIN starts_and_stops sas
      ON clust.patient = sas.patient
      AND clust.group_number = sas.group_number
  )
-- Aggregate costs from helper_cost_aggregation across window group members; exclude continuation groups
SELECT
  final.group_id AS stay_id,
  ANY_VALUE(final.patient) AS patient_id,
  MAX(final.length_of_encounter) AS length_of_encounter,
  MAX(hc.admission_cost) AS admission_cost,
  SUM(hc.total_procedure_costs) AS total_procedure_costs,
  SUM(hc.total_medication_costs) AS total_medication_costs,
  SUM(hc.total_stay_cost) AS total_stay_cost,
  ROUND(
    (SUM(hc.total_procedure_costs) + SUM(hc.total_medication_costs))
    / MAX(final.length_of_encounter),
    2) AS cost_per_day_stay
FROM final_groups final
LEFT JOIN {{DATASET_HELPERS}}.helper_cost_aggregation hc ON final.id = hc.stay_id
WHERE final.encounterclass IN ('urgentcare', 'inpatient', 'emergency')
  -- Exclude groups that continue a prior group (prior hcg row is retained as-is)
  AND final.group_id NOT IN (
    SELECT best.group_id
    FROM best_stay_per_group best
    JOIN first_group_change_per_patient fgc ON best.patient = fgc.patient
    WHERE best.group_number = 0
      AND fgc.first_group_change = 0
      AND best.rn = 1
  )
GROUP BY final.group_id;
