-- Monthly slim: create isolated procedures slim table for this month
-- DDL-only (no DML): CREATE OR REPLACE TABLE creates a standalone per-month table
-- Source: monthly raw staging table procedures_{{END_DATE_SAFE}}
-- JOIN to encounters_slim_{{END_DATE_SAFE}} scopes to valid encounters for this month only
CREATE OR REPLACE TABLE {{DATASET_RAW}}.procedures_slim_{{END_DATE_SAFE}}
AS
SELECT
  proc.start,
  proc.stop,
  proc.patient,
  proc.encounter,
  proc.code,
  proc.description,
  proc.base_cost
FROM {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}} proc
JOIN {{DATASET_RAW}}.encounters_slim_{{END_DATE_SAFE}} e
  ON proc.encounter = e.id
