WITH
  sec_codes AS (
    SELECT
      stay_id,
      following_stay_id AS fol_id,
      readmit_30d,
      readmit_90d,
      main.main_diagnosis_code sec_code
    FROM `hospital-readmission-4.helper_tables.train_helper_utilization` hu
    LEFT JOIN `hospital-readmission-4.data_slim.train_main_diagnoses_nat` main
      ON hu.following_stay_id = main.id
  )
SELECT
  sec.stay_id,
  main.main_diagnosis_code AS code,
  sec.fol_id,
  sec.sec_code,
  readmit_30d,
  readmit_90d
FROM sec_codes sec
LEFT JOIN `hospital-readmission-4.data_slim.train_main_diagnoses_nat` main
  ON main.id = sec.stay_id
