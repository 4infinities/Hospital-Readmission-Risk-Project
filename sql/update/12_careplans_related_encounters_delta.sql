-- careplans_related_encounters delta: return new encounter-careplan pairs for encounters in the new window
-- Feed for DictionaryBuilder: append-only to careplans_related_encounters; no DELETE needed
-- Must run AFTER D3 is loaded to BQ (main_diagnoses must include new window encounters)
-- Depends on: main_diagnoses (updated), encounters_slim, careplans_slim
SELECT DISTINCT
  main.id AS stay_id,
  main.main_diagnosis_code AS code,
  care.code AS sec_code
FROM {{DATASET_HELPERS}}.main_diagnoses main
LEFT JOIN {{DATASET_SLIM}}.encounters_slim e
  ON main.id = e.id
LEFT JOIN {{DATASET_SLIM}}.careplans_slim care
  ON e.patient = care.patient
WHERE
  -- Care plan was already active before the encounter started
  DATE(e.start) > care.start
  -- Care plan had not yet ended at encounter start; use END_DATE for ongoing plans
  AND DATE(e.start) < COALESCE(
    care.stop,
    {{END_DATE}}
  )
  -- Scope to new window encounters only
  AND e.start > LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH) AND e.stop <= {{END_DATE}};
