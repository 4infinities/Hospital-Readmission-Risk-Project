

create or replace table healthcare-test-486920.Raw_csvs_test.index_stay as 

select
  e.patient as                                   patient_id,
  date_diff(date(e.start), p.birthdate, year) as patient_age,
  p.gender as                                    gender,
  e.id as                                        stay_id,
  e.organization as                              hospital_id,
  e.start as                                     admission_datetime,
  e.stop as                                      discharge_datetime,
  date(e.stop) as                                discharge_date,
  extract(year from e.stop) as                   discharge_year,
  extract(month from e.stop) as                  discharge_month,
  clin.main_code,
  clin.main_diagnosis_name,
  clin.main_diagnosis_type,
  clin.num_diagnoses,
  clin.num_chronic_conditions,
  clin.num_procedures,
  clin.has_diabetes,
  clin.has_cancer,
  clin.has_hiv,
  clin.has_hf,
  clin.has_alz,
  clin.has_ckd,
  clin.had_surgery,
  cost.admission_cost,
  cost.total_procedure_costs,
  cost.total_medication_costs,
  cost.total_stay_cost,
  cost.cost_per_day_stay,
  util.admissions_365d,
  util.tot_length_of_stay_365d,
  util.avg_cost_of_prev_stays,
  clin.is_planned,
  util.readmit_30d,
  util.readmit_90d,
  util.days_to_readmit,
  util.following_stay_id,
  util.total_stay_cost as                       total_readmission_cost,
  util.following_unplanned_admission_flag
from healthcare-test-486920.Raw_csvs_test.encounters_slim              e
left join healthcare-test-486920.Raw_csvs_test.patients_slim           p
on e.patient = p.id
left join healthcare-test-486920.Raw_csvs_test.helper_clinical         clin
on clin.stay_id = e.id
left join healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation cost
on cost.stay_id = e.id
left join healthcare-test-486920.Raw_csvs_test.helper_utilization      util
on util.stay_id = e.id
WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')