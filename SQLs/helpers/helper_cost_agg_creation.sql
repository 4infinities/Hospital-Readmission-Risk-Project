create or replace table healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation as

with procedure_costs as
(
  select 
    e.id,
    coalesce(round(sum(proc.base_cost), 2), 0) as total_procedure_costs
  from healthcare-test-486920.Raw_csvs_test.encounters_slim e
  left join healthcare-test-486920.Raw_csvs_test.procedures_slim proc
  on e.id = proc.encounter
  where e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  group by e.id
),

medication_costs as
(
  select 
    e.id,
    coalesce(round(sum(med.totalcost), 2), 0) as total_medication_costs
  from healthcare-test-486920.Raw_csvs_test.encounters_slim e
  left join healthcare-test-486920.Raw_csvs_test.medications_slim med
  on e.id = med.encounter
  where e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  group by e.id
)

select
  e.id as stay_id,
  e.base_encounter_cost as admission_cost,
  proc.total_procedure_costs,
  med.total_medication_costs,
  round(greatest(e.total_claim_cost, e.base_encounter_cost + proc.total_procedure_costs + med.total_medication_costs), 2) as total_stay_cost,

  round(greatest(e.total_claim_cost, e.base_encounter_cost + proc.total_procedure_costs + med.total_medication_costs)/
  greatest(date_diff(e.stop, e.start, day), 1),2) as cost_per_day_stay,

from healthcare-test-486920.Raw_csvs_test.encounters_slim e
left join procedure_costs proc
on e.id = proc.id
left join medication_costs med
on e.id = med.id
where e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')