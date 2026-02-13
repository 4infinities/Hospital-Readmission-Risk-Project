create or replace table healthcare-test-486920.Raw_csvs_test.helper_clinical as(
WITH encounters_pure AS (
  SELECT
    e.id,
    e.encounterclass
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
),


code_dictionary AS (
  SELECT
    code,
    ANY_VALUE(lower(diagnosis_name))  AS diagnosis_name,
    ANY_VALUE(diagnosis_type)  AS diagnosis_type,
    CASE WHEN
      ANY_VALUE(diagnosis_type) = 'disorder'
      AND NOT REGEXP_CONTAINS(
        LOWER(ANY_VALUE(diagnosis_name)),
        r'^(sprain|injury)\b|laceration|fracture')
      THEN 1 ELSE 0 END AS is_chronic,
    case when
      ANY_VALUE(lower(diagnosis_name)) like '%diabetes%'
      and ANY_VALUE(lower(diagnosis_name)) not like '%due%'
      and ANY_VALUE(lower(diagnosis_name)) not like '%prediabetes%'
      then 1 else 0 end as is_diabetes,
    case when REGEXP_CONTAINS(
      any_value(LOWER(diagnosis_name)),
      r'(\bcancer\b|\bcarcinoma\b|\bneoplasm\b|\bmalignan\w*|\bleukemia\b|\blymphoma\b|\bmyeloma\b|\bmelanoma\b)')
      THEN 1 ELSE 0 END AS is_cancer,
    CASE WHEN REGEXP_CONTAINS(
      LOWER(ANY_VALUE(diagnosis_name)),
      r'\bhiv\b|human immunodeficiency virus|immunodeficiency.*virus')
      THEN 1 ELSE 0 END AS is_hiv,
    case when LOWER(ANY_VALUE(diagnosis_name)) like '%heart failure%'
      then 1 else 0 end is_hf,
    CASE WHEN REGEXP_CONTAINS(
      LOWER(ANY_VALUE(diagnosis_name)),
      r'alzheimer|dementia')
      THEN 1 ELSE 0 END AS is_alz,
    case when LOWER(ANY_VALUE(diagnosis_name)) like '%chronic kidney%'
      then 1 else 0 end is_ckd,  
  FROM `healthcare-test-486920.Raw_csvs_test.conditions_slim`
  GROUP BY code
),


end_date AS (
  SELECT MAX(stop) AS end_ts
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim`
),


-- explode claims into one row per (stay, code) so we can see which of them are disorders
claims_long AS (
  Select distinct
  stay_id, code from (
  SELECT encounter as stay_id, CAST(diagnosis1 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis1 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis2 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis2 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis3 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis3 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis4 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis4 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis5 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis5 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis6 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis6 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis7 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis7 IS NOT NULL
  UNION ALL
  SELECT encounter as stay_id, CAST(diagnosis8 AS INT64) AS code FROM `healthcare-test-486920.Raw_csvs_test.claims_slim` WHERE diagnosis8 IS NOT NULL
)t),


claims_prepared as
(
  select
  stay_id,
  count(*) num_diagnoses,
  min(code) min_code_all
  from claims_long
  group by stay_id
),


-- per stay: smallest code that is a disorder (if any) (more precise with smaller codes)
disorder_codes AS (
  SELECT
    cl.stay_id,
    MIN(cl.code) AS min_disorder_code
  FROM claims_long cl
  JOIN code_dictionary cd
    ON cl.code = cd.code
  WHERE cd.diagnosis_type = 'disorder'
  GROUP BY cl.stay_id
),


--every row is a patient, their diagnosis and stay_id where they got this diagnosis, and what is the start of it


patient_diagnoses as
(
  SELECT
    e.patient,
    c.code,
    e.start AS encounter_start
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  JOIN claims_long c
    ON e.id = c.stay_id
),


procedures as
(
select
  proc.encounter as stay_id,
  count(proc.code) as num_procedures
from `healthcare-test-486920.Raw_csvs_test.procedures_slim` proc
group by proc.encounter
),


chronic_patient_codes AS (
  SELECT
    pd.patient,
    pd.code,
    pd.encounter_start
  FROM patient_diagnoses pd
  JOIN code_dictionary cd
    ON pd.code = cd.code
  WHERE cd.is_chronic = 1
),


first_chronic_per_code AS (
  SELECT
    patient,
    code,
    MIN(encounter_start) AS first_chronic_date
  FROM chronic_patient_codes
  GROUP BY patient, code
),


chronic_conditions as
(
SELECT
    e.id AS stay_id,
    e.patient,
    e.start AS encounter_start,
    (
      SELECT COUNT(*)
      FROM first_chronic_per_code f
      WHERE f.patient = e.patient
        AND f.first_chronic_date <= e.start
    ) AS num_chronic_conditions
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
),


patient_condition_starts AS (
  SELECT
    claims.patientid AS patient,
    MIN(IF(cd.is_diabetes = 1, claims.currentillnessdate, NULL)) AS diabetes_start,
    MIN(IF(cd.is_cancer   = 1, claims.currentillnessdate, NULL)) AS cancer_start,
    MIN(IF(cd.is_hiv      = 1, claims.currentillnessdate, NULL)) AS hiv_start,
    MIN(IF(cd.is_hf       = 1, claims.currentillnessdate, NULL)) AS hf_start,
    MIN(IF(cd.is_alz      = 1, claims.currentillnessdate, NULL)) AS alz_start,
    MIN(IF(cd.is_ckd      = 1, claims.currentillnessdate, NULL)) AS ckd_start
  FROM claims_long cl
  JOIN code_dictionary cd
    ON cl.code = cd.code
  JOIN `healthcare-test-486920.Raw_csvs_test.claims_slim` claims
    ON cl.stay_id = claims.encounter
  GROUP BY claims.patientid
),


patient_conditions as(
  SELECT
  e.id AS stay_id,
  -- “has condition at this stay” = stay start on/after first diagnosis
  CASE WHEN pcs.diabetes_start IS NOT NULL
        AND e.start >= pcs.diabetes_start
       THEN 1 ELSE 0 END AS has_diabetes,
  CASE WHEN pcs.cancer_start IS NOT NULL
        AND e.start >= pcs.cancer_start
       THEN 1 ELSE 0 END AS has_cancer,
  CASE WHEN pcs.hiv_start IS NOT NULL
        AND e.start >= pcs.hiv_start
       THEN 1 ELSE 0 END AS has_hiv,
  CASE WHEN pcs.hf_start IS NOT NULL
        AND e.start >= pcs.hf_start
       THEN 1 ELSE 0 END AS has_hf,
  CASE WHEN pcs.alz_start IS NOT NULL
        AND e.start >= pcs.alz_start
       THEN 1 ELSE 0 END AS has_alz,
  CASE WHEN pcs.ckd_start IS NOT NULL
        AND e.start >= pcs.ckd_start
       THEN 1 ELSE 0 END AS has_ckd
FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
LEFT JOIN patient_condition_starts pcs
  ON pcs.patient = e.patient
WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
),


patient_surgeries as(
select
  proc.stop,
  proc.patient,
  dict.is_surgery
from `healthcare-test-486920.Raw_csvs_test.procedures_slim` proc
left join healthcare-test-486920.Raw_csvs_test.procedures_dictionary dict
on proc.code = dict.code
where dict.is_surgery = 1
),


planned_stays_and_surgeries as
(
  select
  e.id as stay_id,
  max(case
    when care.start < date(e.start) and coalesce(care.stop, timestamp_add(care.start, interval 60 day)) > date(e.start)
    then 1 else 0 end) as is_planned,
  max(case
    when date_diff(date(e.start), date(ps.stop), month) >= 0 and date_diff(date(e.start), date(ps.stop), month) < 24
    then 1 else 0 end) as had_surgery
FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
left join `healthcare-test-486920.Raw_csvs_test.careplans_slim` care
on e.patient = care.patient
left join patient_surgeries ps
on  ps.patient = e.patient
WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
group by e.id
)



SELECT
  e.id AS stay_id,
  cp.num_diagnoses,


  -- prefer smallest disorder code; else smallest code overall for main diagnosis
  COALESCE(dc.min_disorder_code, cp.min_code_all) AS main_code,


  cd.diagnosis_name AS main_diagnosis_name,
  cd.diagnosis_type AS main_diagnosis_type,


  coalesce(proc.num_procedures, 0) as num_procedures,
  cc.num_chronic_conditions,


  pc.has_diabetes,
  pc.has_cancer,
  pc.has_hiv,
  pc.has_hf,
  pc.has_alz,
  pc.has_ckd,
  plan.is_planned,
  plan.had_surgery
FROM encounters_pure e
LEFT JOIN claims_prepared cp
  ON e.id = cp.stay_id
LEFT JOIN disorder_codes dc
  ON e.id = dc.stay_id
LEFT JOIN code_dictionary cd
  ON COALESCE(dc.min_disorder_code, cp.min_code_all) = cd.code
left join procedures proc
on e.id = proc.stay_id
left join chronic_conditions cc
on e.id = cc.stay_id
left join patient_conditions pc
on e.id = pc.stay_id
left join planned_stays_and_surgeries plan
on e.id = plan.stay_id
);