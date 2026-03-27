-- procedures_dictionary delta: return new procedure codes seen in the current month not yet in procedures_dictionary
-- Feed for DictionaryBuilder: classify and append-only; no DELETE needed
-- Uses current month staging only — previous months were processed in prior iterations; NOT IN check deduplicates
-- Depends on: procedures_{{END_DATE_SAFE}}, procedures_dictionary
SELECT DISTINCT
  code,
  description AS name
FROM {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}}
WHERE code NOT IN (
  SELECT code FROM {{DATASET_HELPERS}}.procedures_dictionary
);
