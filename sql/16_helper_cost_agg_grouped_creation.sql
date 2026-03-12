CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation_grouped as
WITH
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
    FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim
  ),
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
  starts_and_stops as(
    select
    patient, 
    group_number,
    greatest(date_diff(max(stop), min(start), day), 1) length_of_encounter
    from clusters
    group by patient, group_number
  ),
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
    left join starts_and_stops sas
    on clust.patient = sas.patient
    and clust.group_number = sas.group_number
  )

select
group_id as stay_id,
max(final.length_of_encounter) as length_of_encounter,
max(hc.admission_cost) as admission_cost,
sum(hc.total_procedure_costs) as total_procedure_costs,
sum(hc.total_medication_costs) as total_medication_costs,
sum(hc.total_stay_cost) as total_stay_cost,
round((sum(hc.total_procedure_costs) + sum(hc.total_medication_costs))/max(final.length_of_encounter), 2) as cost_per_day_stay
from final_groups final
left join {{DATASET_HELPERS}}.{{PROFILE}}helper_cost_aggregation hc
on final.id = hc.stay_id
where final.encounterclass in ('urgentcare', 'inpatient', 'emergency')
group by final.group_id
