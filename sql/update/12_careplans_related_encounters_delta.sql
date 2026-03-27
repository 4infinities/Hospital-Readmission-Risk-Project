-- careplans_related_encounters delta: return new encounter-careplan pairs for current month encounters
-- Feed for DictionaryBuilder: append-only to careplans_related_encounters; no DELETE needed
-- Uses current month staging for encounters — previous months processed in prior iterations; avoids duplication
-- careplans_slim is kept: active careplans span full history, cannot be scoped to monthly staging
-- Must run AFTER D3 is loaded to BQ (main_diagnoses must include new window encounters)
-- Depends on: main_diagnoses (updated), encounters_{{END_DATE_SAFE}}, careplans_slim
SELECT DISTINCT
  main.id AS stay_id,
  main.main_diagnosis_code AS code,
  care.code AS sec_code
FROM {{DATASET_HELPERS}}.main_diagnoses main
JOIN (
  -- Scope to current month encounters only (not full window) to avoid re-processing prior months
  SELECT id, patient, start, stop
  FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}
  WHERE DATE(start) > LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH)
    AND DATE(stop) <= {{END_DATE}}
) e ON main.id = e.id
LEFT JOIN {{DATASET_SLIM}}.careplans_slim care
  ON e.patient = care.patient
WHERE
  -- Care plan was already active before the encounter started
  DATE(e.start) > care.start
  -- Care plan had not yet ended at encounter start; use END_DATE for ongoing plans
  AND DATE(e.start) < COALESCE(
    care.stop,
    {{END_DATE}}
  );
