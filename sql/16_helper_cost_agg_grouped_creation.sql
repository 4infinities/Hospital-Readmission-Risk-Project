-- helper_cost_aggregation_grouped: one row per encounter GROUP; sums costs across all member encounters and computes group-level length and daily cost
-- Depends on: encounters_slim, helper_cost_aggregation
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_cost_aggregation_grouped as
WITH
  -- Assign type_flag rank and detect group boundaries (same logic as clinical grouping)
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
  starts_and_stops as(
    select
    patient,
    group_number,
    greatest(date_diff(max(stop), min(start), day), 1) length_of_encounter
    from clusters
    group by patient, group_number
  ),
  -- Map each member encounter to its group_id, length, and class label
  final_groups AS (
    SELECT
      clust.id,
      clust.patient,
      best.group_id,
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
    left join starts_and_stops sas
    on clust.patient = sas.patient
    and clust.group_number = sas.group_number
  )

-- Aggregate costs from helper_cost_aggregation across all clinical group members; cost_per_day uses group-level length
select
group_id as stay_id,
ANY_VALUE(final.patient) AS patient_id,
max(final.length_of_encounter) as length_of_encounter,
max(hc.admission_cost) as admission_cost,
sum(hc.total_procedure_costs) as total_procedure_costs,
sum(hc.total_medication_costs) as total_medication_costs,
sum(hc.total_stay_cost) as total_stay_cost,
round((sum(hc.total_procedure_costs) + sum(hc.total_medication_costs))/max(final.length_of_encounter), 2) as cost_per_day_stay
from final_groups final
left join {{DATASET_HELPERS}}.helper_cost_aggregation hc
on final.id = hc.stay_id
where final.encounterclass in ('urgentcare', 'inpatient', 'emergency')
group by final.group_id
