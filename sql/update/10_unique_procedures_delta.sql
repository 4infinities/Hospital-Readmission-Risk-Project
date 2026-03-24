-- procedures_dictionary delta: return new procedure codes seen in the new window not yet in procedures_dictionary
-- Feed for DictionaryBuilder: classify and append-only; no DELETE needed
-- Depends on: procedures_slim, procedures_dictionary
SELECT DISTINCT
  code,
  description AS name
FROM {{DATASET_SLIM}}.procedures_slim
WHERE stop > LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH) AND stop <= {{END_DATE}}
  AND code NOT IN (
    SELECT code FROM {{DATASET_HELPERS}}.procedures_dictionary
  );
