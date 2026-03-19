-- Feed query for DictionaryBuilder: collect every distinct SNOMED procedure code and its description seen up to END_DATE
SELECT DISTINCT
code,
description as name
FROM {{DATASET_SLIM}}.procedures_slim
where stop <= {{END_DATE}}