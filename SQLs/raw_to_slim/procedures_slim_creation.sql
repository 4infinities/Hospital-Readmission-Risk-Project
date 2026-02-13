create or replace table healthcare-test-486920.Raw_csvs_test.procedures_slim
cluster by encounter
as
Select 
  proc.start, 
  proc.stop, 
  proc.patient,
  proc.encounter, 
  proc.code, 
  proc.description, 
  proc.base_cost 
from healthcare-test-486920.Raw_csvs_test.procedures proc
join healthcare-test-486920.Raw_csvs_test.encounters_slim e
on proc.encounter = e.id
