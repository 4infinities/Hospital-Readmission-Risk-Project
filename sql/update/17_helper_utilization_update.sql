-- helper_utilization DDL-only update: CREATE OR REPLACE preserving pre-window rows + fresh window recalculation
-- Self-referential architecture:
--   - Window grouping runs on staging-table encounters only (no full encounters_slim scan)
--   - 365d lookback: self-join to existing helper_utilization rows (pre-window fixed base)
--   - Following-stay lookup: self-join to window inpatient encounters + existing post-window hu rows
--   - Continuation groups (gap < 12h to prior stop) are skipped — prior row is retained
-- prior_group_anchor fix: hcg.stay_id NOT IN window_ids excludes current window hcg rows (from H3)
-- whose hu joins would otherwise pull M-1 stop dates from the unmodified hu table
-- Depends on: encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}},
--             helper_utilization (pre-window base), helper_clinical_grouped (for is_planned),
--             helper_cost_aggregation_grouped (for costs)
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_utilization AS
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
      encounterclass,
      start,
      stop,
      admissions_365d,
      tot_length_of_stay_365d,
      avg_cost_of_prev_stays,
      prev_stay_id,
      prev_stay_date,
      following_stay_id,
      following_stay_date,
      days_to_readmit,
      readmit_30d,
      readmit_90d,
      total_stay_cost,
      following_unplanned_admission_flag
    FROM {{DATASET_HELPERS}}.helper_utilization
    WHERE stay_id NOT IN (SELECT id FROM window_ids)
  ),
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
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
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
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  -- Most recent prior group stop per window patient, for group boundary detection
  -- hcg.stay_id NOT IN window_ids: excludes current window hcg rows (present after H3 CREATE OR REPLACE)
  -- whose hu joins would otherwise return M-1 stop dates from the still-unmodified hu table
  prior_group_anchor AS (
    SELECT
      wp.patient,
      MAX(hu.stop) AS last_prior_stop
    FROM (SELECT DISTINCT patient FROM window_encounters) wp
    JOIN {{DATASET_HELPERS}}.helper_clinical_grouped hcg
      ON hcg.patient_id = wp.patient
    JOIN {{DATASET_HELPERS}}.helper_utilization hu ON hcg.stay_id = hu.stay_id
    WHERE hcg.stay_id NOT IN (SELECT id FROM window_ids)
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
  -- Compute group-level start, stop, and length from window encounters
  starts_and_stops AS (
    SELECT
      patient,
      group_number,
      MIN(start) AS start,
      MAX(stop) AS stop,
      GREATEST(DATE_DIFF(MAX(stop), MIN(start), DAY), 1) AS length_of_encounter
    FROM clusters
    GROUP BY patient, group_number
  ),
  -- Map each window encounter to its group representative, dates, and class label
  final_groups AS (
    SELECT
      clust.id,
      clust.patient,
      best.group_id,
      sas.start,
      sas.stop,
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
  ),
  -- Deduplicate to one row per clinical group in the window (urgentcare/emergency/inpatient only)
  -- Excludes groups that continue a prior group (prior row retained as-is via existing CTE)
  encounters_pure AS (
    SELECT DISTINCT
      group_id AS id, patient, start, stop, length_of_encounter, encounterclass
    FROM final_groups
    WHERE encounterclass IN ('urgentcare', 'emergency', 'inpatient')
      AND group_id NOT IN (
        SELECT best.group_id
        FROM best_stay_per_group best
        JOIN first_group_change_per_patient fgc ON best.patient = fgc.patient
        WHERE best.group_number = 0
          AND fgc.first_group_change = 0
          AND best.rn = 1
      )
  ),
  -- Window inpatient groups only (for cross-window readmission detection)
  window_inpatient AS (
    SELECT id, patient, start, stop, length_of_encounter AS length_of_stay
    FROM encounters_pure
    WHERE encounterclass = 'inpatient'
  ),
  -- Pre-window inpatient history from helper_utilization base rows (stop < window_start)
  -- Used for 365d lookback without scanning full encounters_slim
  prior_inpatient_base AS (
    SELECT
      hu.stay_id                                      AS id,
      hu.patient_id                                   AS patient,
      hu.start                                        AS start,
      hu.stop                                         AS stop,
      GREATEST(DATE_DIFF(hu.stop, hu.start, DAY), 1) AS length_of_stay
    FROM {{DATASET_HELPERS}}.helper_utilization hu, bounds
    WHERE hu.encounterclass = 'inpatient'
      AND DATE(hu.stop) < bounds.window_start
  ),
  -- All inpatient stays available for lookback: prior base + window inpatient
  all_inpatient AS (
    SELECT id, patient, start, stop, length_of_stay FROM prior_inpatient_base
    UNION ALL
    SELECT id, patient, start, stop, length_of_stay FROM window_inpatient
  ),
  -- Cross-join each window clinical encounter against all prior inpatient stays for that patient
  pairwise AS (
    SELECT
      pure.id                                                         AS stay_id,
      pure.patient                                                    AS patient,
      pure.start                                                      AS index_start,
      inp.id                                                          AS prev_inp_id,
      inp.stop                                                        AS prev_inp_stop,
      DATE_DIFF(pure.start, inp.stop, DAY)                           AS days_since_prev_inp,
      inp.length_of_stay                                              AS length_of_stay,
      help_cost.total_stay_cost                                       AS prev_stay_cost,
      ROW_NUMBER() OVER (PARTITION BY pure.id ORDER BY inp.stop DESC) AS rn_prev
    FROM encounters_pure pure
    LEFT JOIN all_inpatient inp
      ON pure.patient = inp.patient
      AND inp.stop < pure.start
    LEFT JOIN {{DATASET_HELPERS}}.helper_cost_aggregation_grouped help_cost
      ON inp.id = help_cost.stay_id
  ),
  -- Aggregate 365-day lookback utilization features per window encounter
  prev_data AS (
    SELECT
      pair.stay_id                                                                      AS stay_id,
      COUNTIF(pair.days_since_prev_inp BETWEEN 0 AND 365)                              AS admissions_365d,
      SUM(IF(pair.days_since_prev_inp BETWEEN 0 AND 365, pair.length_of_stay, 0))      AS tot_length_of_stay_365d,
      ROUND(AVG(IF(pair.days_since_prev_inp BETWEEN 0 AND 365, pair.prev_stay_cost, NULL)), 2) AS avg_cost_of_prev_stays,
      MAX(IF(pair.rn_prev = 1, pair.prev_inp_id,   NULL))                              AS prev_stay_id,
      MAX(IF(pair.rn_prev = 1, pair.prev_inp_stop, NULL))                              AS prev_stay_date
    FROM pairwise pair
    GROUP BY pair.stay_id
  ),
  -- Cross-join each window clinical encounter against all following inpatient stays (within window only)
  -- Post-window readmissions will be captured in future iterations
  pairwise_follow AS (
    SELECT
      pure.id                                                          AS stay_id,
      pure.patient                                                     AS patient,
      pure.stop                                                        AS index_stop,
      inp.id                                                           AS fol_inp_id,
      inp.start                                                        AS fol_inp_start,
      DATE_DIFF(inp.start, pure.stop, DAY)                            AS days_to_readmit,
      ROW_NUMBER() OVER (PARTITION BY pure.id ORDER BY inp.start ASC) AS rn_fol
    FROM encounters_pure pure
    LEFT JOIN window_inpatient inp
      ON pure.patient = inp.patient
      AND inp.start > pure.stop
  ),
  -- Derive readmit flags from the next inpatient stay's days_to_readmit
  follow_data AS (
    SELECT
      pair.stay_id                                              AS stay_id,
      MAX(IF(pair.rn_fol = 1, pair.fol_inp_start, NULL))       AS following_stay_date,
      MAX(IF(pair.rn_fol = 1, pair.fol_inp_id,    NULL))       AS following_stay_id,
      MAX(IF(pair.rn_fol = 1, pair.days_to_readmit, NULL))     AS days_to_readmit,
      CASE
        WHEN MAX(IF(pair.rn_fol = 1, pair.days_to_readmit, NULL)) <= 30 THEN 1 ELSE 0
      END AS readmit_30d,
      CASE
        WHEN MAX(IF(pair.rn_fol = 1, pair.days_to_readmit, NULL)) <= 90 THEN 1 ELSE 0
      END AS readmit_90d
    FROM pairwise_follow pair
    GROUP BY pair.stay_id
  ),
  -- Final output: combine prev_data and follow_data; suppress readmit flags if following stay is planned
  new_rows AS (
    SELECT
      pre.stay_id                                                           AS stay_id,
      e.patient                                                             AS patient_id,
      e.encounterclass                                                      AS encounterclass,
      e.start                                                               AS start,
      e.stop                                                                AS stop,
      pre.admissions_365d                                                   AS admissions_365d,
      pre.tot_length_of_stay_365d                                           AS tot_length_of_stay_365d,
      pre.avg_cost_of_prev_stays                                            AS avg_cost_of_prev_stays,
      pre.prev_stay_id                                                      AS prev_stay_id,
      pre.prev_stay_date                                                    AS prev_stay_date,
      fol.following_stay_id                                                 AS following_stay_id,
      fol.following_stay_date                                               AS following_stay_date,
      fol.days_to_readmit                                                   AS days_to_readmit,
      IF(help_clin.is_planned = 1, 0, fol.readmit_30d)                     AS readmit_30d,
      IF(help_clin.is_planned = 1, 0, fol.readmit_90d)                     AS readmit_90d,
      help_cost.total_stay_cost                                             AS total_stay_cost,
      IF(IF(help_clin.is_planned = 1, 0, fol.readmit_90d) = 0, 0, 1)      AS following_unplanned_admission_flag
    FROM prev_data pre
    LEFT JOIN (
      SELECT id AS stay_id, patient, encounterclass, start, stop
      FROM encounters_pure
    ) e ON pre.stay_id = e.stay_id
    LEFT JOIN follow_data fol ON pre.stay_id = fol.stay_id
    LEFT JOIN {{DATASET_HELPERS}}.helper_clinical_grouped help_clin
      ON help_clin.stay_id = fol.following_stay_id
    LEFT JOIN {{DATASET_HELPERS}}.helper_cost_aggregation_grouped help_cost
      ON help_cost.stay_id = fol.following_stay_id
  )
SELECT
  stay_id,
  patient_id,
  encounterclass,
  start,
  stop,
  admissions_365d,
  tot_length_of_stay_365d,
  avg_cost_of_prev_stays,
  prev_stay_id,
  prev_stay_date,
  following_stay_id,
  following_stay_date,
  days_to_readmit,
  readmit_30d,
  readmit_90d,
  total_stay_cost,
  following_unplanned_admission_flag
FROM existing
UNION ALL
SELECT
  stay_id,
  patient_id,
  encounterclass,
  start,
  stop,
  admissions_365d,
  tot_length_of_stay_365d,
  avg_cost_of_prev_stays,
  prev_stay_id,
  prev_stay_date,
  following_stay_id,
  following_stay_date,
  days_to_readmit,
  readmit_30d,
  readmit_90d,
  total_stay_cost,
  following_unplanned_admission_flag
FROM new_rows
