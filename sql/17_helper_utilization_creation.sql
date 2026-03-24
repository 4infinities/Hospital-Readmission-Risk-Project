-- helper_utilization: one row per clinical encounter group; computes 365-day lookback utilization,
-- previous stay details, readmission flags (30d/90d), and the following unplanned admission flag
-- Depends on: encounters_slim, helper_cost_aggregation_grouped, helper_clinical_grouped
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_utilization
AS
WITH
  -- Assign type_flag rank and detect group boundaries (same logic as other grouped tables)
  group_flags AS (
    SELECT
      id,
      patient,
      start,
      stop,
      CASE encounterclass
        when 'wellness' then 0
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
  where stop <= {{END_DATE}}
  ),
  -- Cumulative sum yields monotonically increasing group_number per patient
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
  -- Elect the representative encounter per group
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
  -- Compute group-level start, stop, and length (earliest start to latest stop across all members)
  starts_and_stops AS (
    SELECT
      patient,
      group_number,
      min(start) AS start,
      max(stop) AS stop,
      greatest(date_diff(max(stop), min(start), day), 1) length_of_encounter
    FROM clusters
    GROUP BY patient, group_number
  ),
  -- Map each member encounter to its group representative, group dates, and class label
  final_groups AS (
    SELECT
      clust.id,
      clust.patient,
      best.group_id,
      sas.start,
      sas.stop,
      sas.length_of_encounter,
      CASE best.type_flag
        when 0 then 'wellness'
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
      ON
        clust.patient = sas.patient
        AND clust.group_number = sas.group_number
  ),
  -- Deduplicate to one row per clinical group (urgentcare/emergency/inpatient only)
  encounters_pure AS (
    SELECT DISTINCT
      group_id AS id, patient, start, stop, length_of_encounter, encounterclass
    FROM final_groups
    WHERE encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  ),
  -- Subset of clinical encounters that are strictly inpatient (used for 365d lookback and readmission chains)
  encounters_inpatient AS (
    SELECT
      e.id,
      e.patient,
      e.start,
      e.stop,
      length_of_encounter AS length_of_stay
    FROM encounters_pure e
    WHERE e.encounterclass = 'inpatient'
  ),
  -- Cross-join each clinical encounter against all prior inpatient stays for that patient
  pairwise AS (
    SELECT
      pure.id AS stay_id,
      pure.patient,
      pure.start AS index_start,
      inp.id AS prev_inp_id,
      inp.stop AS prev_inp_stop,
      DATE_DIFF(pure.start, inp.stop, DAY) AS days_since_prev_inp,
      inp.length_of_stay,
      help_cost.total_stay_cost AS prev_stay_cost,
      -- Rank prior inpatient stays descending by stop date (rn_prev = 1 = most recent prior stay)
      row_number() OVER (PARTITION BY pure.id ORDER BY inp.stop DESC) AS rn_prev
    FROM encounters_pure pure
    LEFT JOIN encounters_inpatient inp
      ON
        pure.patient = inp.patient
        AND inp.stop < pure.start
    LEFT JOIN
      {{DATASET_HELPERS}}.helper_cost_aggregation_grouped
        help_cost
      ON inp.id = help_cost.stay_id
  ),
  -- Aggregate 365-day lookback utilization features per encounter from the pairwise cross-join
  prev_data AS (
    SELECT
      pair.stay_id,
      -- Count prior inpatient stays within 365 days
      COUNTIF(pair.days_since_prev_inp BETWEEN 0 AND 365) AS admissions_365d,
      -- Total inpatient length of stay within 365 days
      SUM(
        IF(pair.days_since_prev_inp BETWEEN 0 AND 365, pair.length_of_stay, 0))
        AS tot_length_of_stay_365d,
      -- Average cost of prior stays within 365 days (NULL if no prior stays in window)
      round(
        avg(
          IF(
            pair.days_since_prev_inp BETWEEN 0 AND 365,
            pair.prev_stay_cost,
            NULL)),
        2) AS avg_cost_of_prev_stays,
      MAX(IF(pair.rn_prev = 1, pair.prev_inp_id, NULL)) AS prev_stay_id,
      MAX(IF(pair.rn_prev = 1, pair.prev_inp_stop, NULL)) AS prev_stay_date
    FROM pairwise pair
    GROUP BY stay_id
  ),
  -- Cross-join each clinical encounter against all subsequent inpatient stays for that patient
  pairwise_follow AS (
    SELECT
      pure.id AS stay_id,
      pure.patient,
      pure.stop AS index_stop,
      inp.id AS fol_inp_id,
      inp.start AS fol_inp_start,
      DATE_DIFF(inp.start, pure.stop, DAY) AS days_to_readmit,
      -- Rank following inpatient stays ascending by start date (rn_fol = 1 = next inpatient stay)
      row_number() OVER (PARTITION BY pure.id ORDER BY inp.start ASC) AS rn_fol
    FROM encounters_pure pure
    LEFT JOIN encounters_inpatient inp
      ON
        pure.patient = inp.patient
        AND inp.start > pure.stop
  ),
  -- Derive readmit_30d and readmit_90d flags from the next inpatient stay's days_to_readmit
  follow_data AS (
    SELECT
      pair.stay_id,
      max(IF(pair.rn_fol = 1, pair.fol_inp_start, NULL)) AS following_stay_date,
      max(IF(pair.rn_fol = 1, pair.fol_inp_id, NULL)) AS following_stay_id,
      max(IF(pair.rn_fol = 1, days_to_readmit, NULL)) AS days_to_readmit,
      CASE
        WHEN max(IF(pair.rn_fol = 1, days_to_readmit, NULL)) <= 30
          THEN 1
        ELSE 0
        END readmit_30d,
      CASE
        WHEN max(IF(pair.rn_fol = 1, days_to_readmit, NULL)) <= 90
          THEN 1
        ELSE 0
        END readmit_90d,
    FROM pairwise_follow pair
    GROUP BY stay_id
  )
-- Final output: combine prev_data and follow_data; suppress readmit flags if following stay is planned;
-- following_unplanned_admission_flag = 1 only if the following stay was unplanned
SELECT
  pre.stay_id,
  e.patient AS patient_id,
  e.encounterclass,
  e.start,
  e.stop,
  pre.admissions_365d,
  pre.tot_length_of_stay_365d,
  pre.avg_cost_of_prev_stays,
  pre.prev_stay_id,
  pre.prev_stay_date,
  fol.following_stay_id,
  fol.following_stay_date,
  fol.days_to_readmit,
  -- Zero out readmit flags if the following stay was itself planned
  IF(help_clin.is_planned = 1, 0, fol.readmit_30d) AS readmit_30d,
  IF(help_clin.is_planned = 1, 0, fol.readmit_90d) AS readmit_90d,
  help_cost.total_stay_cost,
  IF(IF(help_clin.is_planned = 1, 0, fol.readmit_90d) = 0, 0, 1)
    AS following_unplanned_admission_flag
FROM prev_data pre
left join encounters_pure e
on pre.stay_id = e.id
LEFT JOIN follow_data fol
  ON pre.stay_id = fol.stay_id
-- Join to following stay's clinical flags to determine if it was planned
LEFT JOIN {{DATASET_HELPERS}}.helper_clinical_grouped help_clin
  ON help_clin.stay_id = fol.following_stay_id
-- Join to following stay's cost for total_readmission_cost
LEFT JOIN
  {{DATASET_HELPERS}}.helper_cost_aggregation_grouped help_cost
  ON help_cost.stay_id = fol.following_stay_id

