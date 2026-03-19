SELECT DISTINCT 
code, 
description as name
FROM {{DATASET_SLIM}}.procedures_slim
where stop <= {{END_DATE}}