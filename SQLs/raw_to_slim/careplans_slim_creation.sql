create or replace table healthcare-test-486920.Raw_csvs_test.careplans_slim
partition by stop
cluster by encounter, patient
as
Select 
  care.Start, 
  care.stop, 
  care.patient, 
  care.encounter, 
  care.description 
from healthcare-test-486920.Raw_csvs_test.careplans care
join healthcare-test-486920.Raw_csvs_test.encounters_slim e
on care.encounter = e.id