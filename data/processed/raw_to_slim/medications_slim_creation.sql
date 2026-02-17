create or replace table healthcare-test-486920.Raw_csvs_test.medications_slim
cluster by encounter
as
Select 
  m.start, 
  m.stop, 
  m.encounter, 
  m.code, 
  m.description, 
  m.base_cost, 
  m.dispenses, 
  m.totalcost
from healthcare-test-486920.Raw_csvs_test.medications m
join healthcare-test-486920.Raw_csvs_test.encounters_slim e
on m.encounter = e.id