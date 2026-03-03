CREATE OR REPLACE TABLE hospital-readmission-4.helper_tables.index_stay
AS
SELECT
  e.patient AS patient_id,
  date_diff(date(util.start), p.birthdate, year) AS patient_age,
  p.gender AS gender,
  clin.stay_id,
  e.organization AS hospital_id,
  util.start AS admission_datetime,
  util.stop AS discharge_datetime,
  date(util.stop) AS discharge_date,
  EXTRACT(year FROM util.stop) AS discharge_year,
  EXTRACT(month FROM util.stop) AS discharge_month,
  cost.length_of_encounter AS length_of_stay,
  util.encounterclass AS stay_type,
  clin.main_code,
  clin.main_name,
  clin.is_disorder,
  clin.is_symptom,
  clin.inflammation,
  clin.musculoskeletal,
  clin.nervous,
  clin.respiratory,
  clin.cardiac,
  clin.renal,
  clin.trauma,
  clin.intoxication,
  clin.num_disorders,
  clin.num_findings,
  clin.num_chronic_conditions,
  clin.num_procedures,
  clin.has_diabetes,
  clin.has_cancer,
  clin.has_hiv,
  clin.has_hf,
  clin.has_alz,
  clin.has_ckd,
  clin.has_lf,
  clin.had_surgery,
  clin.is_planned,
  cost.admission_cost,
  cost.total_procedure_costs,
  cost.total_medication_costs,
  cost.total_stay_cost,
  cost.cost_per_day_stay,
  util.admissions_365d,
  util.tot_length_of_stay_365d,
  util.avg_cost_of_prev_stays,
  util.readmit_30d,
  util.readmit_90d,
  rel.is_related,
  rel_readmit_30d,
  rel_readmit_90d,
  util.days_to_readmit,
  util.following_stay_id,
  util.total_stay_cost AS total_readmission_cost,
  util.following_unplanned_admission_flag
FROM hospital-readmission-4.data_slim.encounters_slim e
LEFT JOIN hospital-readmission-4.data_slim.patients_slim p
  ON e.patient = p.id
LEFT JOIN hospital-readmission-4.helper_tables.helper_clinical clin
  ON clin.stay_id = e.id
LEFT JOIN hospital-readmission-4.helper_tables.helper_cost_aggregation_grouped cost
  ON cost.stay_id = e.id
LEFT JOIN hospital-readmission-4.helper_tables.helper_utilization util
  ON util.stay_id = e.id
left join `hospital-readmission-4.data_slim.related_diagnoses` rel
on e.id = rel.stay_id
WHERE
  e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  AND util.start > timestamp("2018-01-01")

