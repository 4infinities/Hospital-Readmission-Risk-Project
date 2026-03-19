\## Problem Definition



1. An index admission is the hospital stay that is treated as the “starting point” for measuring whether a patient gets readmitted.



2\. For each patient, every eligible inpatient stay can be an index admission as long as the patient is discharged alive and can be observed at least 30 (or 90) days after discharge in the data.



3\. A readmission is any unplanned inpatient stay that starts within 30 (or 90) days after discharge from an index admission, at the same or another hospital.



4\. If there is at least one readmission within 30 days, readmit\_30d = 1 is set for that index admission; otherwise readmit\_30d = 0.

​

5\. If there is at least one readmission within 90 days, readmit\_90d = 1 is set for that index admission; otherwise readmit\_90d = 0.

​

6\. Whenever readmit\_30d = 1 is set, readmit\_90d = 1 is also set because 30 day-window is fully contained within the 90‑day window.



---



\## Column Types per Stay



Patient - patient\_id, age, sex (int)



Stay - stay\_id, admission\_date(datetime), discharge\_date(date), admission\_type(str elective/emergency), discharge\_destination/type(str), length\_of\_stay\_days, hospital\_id(int)



Labels - index\_admission, readmit\_30d, readmit\_90d, days\_to\_readmit, observed\_30d, observed\_90d (int)



Clinical - diagnosis\_code, secondary\_diagnosis\_code(s), procedure\_code(s) (str)



Costs - admission\_cost, readmission\_cost, cost\_per\_day\_stay, total\_med\_cost (float)



History - admissions\_365d(int), tot\_length\_of\_stay\_365d(int), last\_stay\_diagnosis(str)

---



\## First-pass feature set for the risk model



Patient - age, sex(dummy)



Stay - length\_of\_stay\_days, hospital\_id(dummy?), admission\_type(dummy), discharge\_disposition(dummy)



Clinical - diagnosis\_code\_group(dummies), secondary\_diagnosis\_code(s)(dummies) - mb cluster by diagnosis code, has\_chronical\_diseases, has\_diabetes, has\_cancer, has\_HVI, condition\_rarity, had\_associated\_surgery



Costs - total\_med\_cost



History - admissions\_365d, tot\_length\_of\_stay\_365d, avg\_cost\_of\_prev\_stays



---



\## Cost and Value-of-Reduction metric



value-of-reduction per patient:
value-of-reduction = readmission\_probability\_decrease \* readmission\_cost - (1 - readmission\_probability\_decrease) \* readmission\_cost - length\_of\_readmission\_stay\_days \* cost\_per\_day\_stay



\* Under the assumption that The length of Intervention days is the same As length of readmission days and that the average cost per day stays under intervention stays the same as it was for the length\_of\_stay\_days



---



\## Dataset generation in Synthea

java -jar synthea-with-dependencies.jar -s 42 -cs 42 -p 20000 --exporter.csv.export=true --exporter.years\_of\_history=8 California

java -jar synthea-with-dependencies.jar -s 100 -cs 100 -p 50000 --exporter.csv.export=true --exporter.years_of_history=10 California

CSV-modules for GCP



patients.csv (patient\_id, demographics for age/sex).

​

encounters.csv (stays, dates, organization/hospital, base and total costs).​



conditions.csv (diagnosis codes).​



procedures.csv (procedure codes and costs).​



medications.csv / immunizations.csv (additional medical costs).​



Optionally claims.csv and claims\_transactions.csv if claims-based costs are preferred.



---



\## Index Stay - Fact Table



Every row in the table is an inpatient stay that:



1. Is an inpatient encounter (exclude outpatient/ED‑only).



2\. Has discharge disposition not equal to “deceased” (patient alive at discharge).

​

3\. Has discharge\_date at least 30 days before dataset\_end\_date (so 30‑day outcomes are observable).



4\. Patient is observable in the data for at least 30 days after discharge.

===========================================

\## Columns:



Identification: 



patient\_id (int) - unique patient identifier (FK)



\*\*patient\_age (int) - age in years at admission\_datetime



patient\_sex (M or F) - patient gender



stay\_id (int) - unique stay identifier (PK)



hospital\_id (int) - unique institution id (FK)



admission\_datetime (datetime) - date and time of admission



discharge\_datetime (datetime) - date and time of discharge



\*\*discharge\_date (date) - date of discharge



\*\*discharge\_year (int) - year of discharge date



\*\*discharge\_month (int) - month of discharge date



\*\*length\_of\_stay\_days (int) - define as DATE\_DIFF(discharge\_datetime, admission\_datetime, DAY)



===========================================



Clinical:



primary\_diagnosis\_code (str) - main diagnosis unique identifier



\*\*num\_secondary\_diagnoses (int) - count of sec diagnoses



\*\*num\_procedures (int) - count of taken procedures



\*\*num\_chronic\_conditions (int) - count of chronic diseases of any form



\*\*has\_diabetes (bool)



\*\*has\_cancer (bool)



\*\*has\_hiv (bool)



\*\*comorbidity\_score (float) - summary score of comorbidity burden



\*\*had\_surgery (bool) - had a documented surgery associated with patient's current diagnosis at any time



===========================================



Costs:



admission\_cost (float) - initial admission cost in $



\*\*total\_procedure\_costs (float) - total cost of procedures during the stay in $



\*\*total\_medication\_costs (float) - total cost of medication drugs taken during the stay in $



total\_stay\_cost (float) - total expences on the stay in $



\*\*cost\_per\_day\_stay (float) - total expences per day during the stay in $/day



===========================================



History:



\*\*admissions\_365d (int) - count of inpatient admissions in the 365 days before admission\_datetime for this index stay



\*\*tot\_length\_of\_stay\_365d (int) - count of inpatient days within 365 days from the stay



\*\*avg\_cost\_of\_prev\_stays (float) - total cost of previous inpatient stays within 365 days from the 

stay/tot\_length\_of\_stay\_365d

===========================================

Outcomes and flags:

\*\*planned\_admission\_flag (bool) - 1 if this stay is elective/planned according to care plans and procedure/diagnosis categories (e.g. scheduled chemo, planned surgery).

\*\*readmit\_30d (bool) - 1 if there exists a next unplanned inpatient admission for the same patient where admit\_date > discharge\_date and admit\_date − discharge\_date ≤ 30 days; 0 otherwise.

\*\*readmit\_90d (bool) - 1 if there exists a next unplanned inpatient admission or the same patient where admit\_date > discharge\_date and admit\_date − discharge\_date ≤ 90 days; 0 otherwise.

\*\*days\_to\_readmit (int) - count of days from this stay to readmission date (if readmit\_90d is true)

\*\*readmission\_id (int) - readmission stay id (if readmit\_90d is true)

\*\*observed\_30d (bool) - if a patient was observed in the following 30 days after the discharge date - sanity check, must be 1 for every entrance

\*\*observed\_90d (bool) - if a patient was observed in the following 90 days after the discharge date

\*\*total\_readmission\_cost (float) - total expences on the readmission stay in $ (if readmit\_90d is true)

\*\*combined\_readmission\_cost (float) - cost of admission + cost of readmission (if readmit\_90d is true)

===========================================

Columns marked with \*\* are derived from helper tables down below.

---

\## Helper Tables

Clinical:

stay\_id (PK),

primary\_diagnosis\_code (str),

num\_secondary\_diagnoses (int) ,

num\_procedures (int) ,

num\_chronic\_conditions (int),

has\_diabetes (bool),

has\_cancer (bool),

has\_hiv (bool),

comorbidity\_score (float),

had\_surgery (bool),

planned\_admission\_flag (bool) - from careplans and conditions

===========================================

Cost Aggregation:

stay\_id (PK),

admission\_cost (float),

total\_procedure\_costs (float),

total\_medication\_costs (float),

total\_stay\_cost (float),

cost\_per\_day\_stay (float)

===========================================

Utilization:

stay\_id (PK),

admissions\_365d (int), 

tot\_length\_of\_stay\_365d (int),

avg\_cost\_of\_prev\_stays (float),

prev\_stay\_id (int) - id of a previous stay for this patient (FK),

prev\_stay\_date (date) - discharge\_date of a previous stay for this patient,

following\_stay\_id (int) - id of a following stay for this patient (readmission\_id for unplanned readmissions) (FK),

following\_stay\_date (date) - admission\_date of a following stay for this patient,

following\_unplanned\_admission\_flag (bool) - 1 if the next admission exists and is unplanned, 0 otherwise.

days\_to\_readmit (int),

readmit\_30d (bool),

readmit\_90d (bool)

---

TableID test query = chc-nih-chest-xray.nih_chest_xray.nih_chest_xray

---

## Environment & Infra – Week 1 Day 4 (2026‑02‑08)

- Created a project‑specific Python virtual environment (`.venv`) and installed core dependencies: `google-cloud-bigquery`, `pandas`, `ipykernel`.

- Set up Google Cloud authentication by creating a service account with `BigQuery User` and `BigQuery Data Viewer` roles, storing the JSON key under `.secrets/` (git‑ignored), and configuring `GOOGLE_APPLICATION_CREDENTIALS` for local runs.

- Verified BigQuery connectivity from Python by instantiating `bigquery.Client()` and confirming it returns the correct GCP project id.

- Registered the `.venv` as a Jupyter kernel (`Python (hospital-venv)`) so notebooks in the project run with the same environment and can import the BigQuery client.

- Initialized a standalone git repository for this project, added a `.gitignore` that excludes `.venv`, `.secrets`, and notebook checkpoints, and ensured the parent `D:\Python Projects` repo ignores this nested repo.

---------------------------------------------

SQL Table reduction queries pipeline

select id, birthdate, deathdate, race, gender from patients p

index by id

=============================================

Select id, Start, stop, Patient, organization, encounter_class, Base_Encounter_Cost, Total_Claim_Cost, description from encounters e
where 
encounter_class = 'inpatient' and
stop <= end_date - 30 days and
stop < coalesce(p.deathdate, end_date)

Index by id, start, patient, stop

=============================================

Select Start, stop, patient, encounter, description from careplans care
where care.encounter in e.id

index by encounter, patient

=============================================

Select start, stop, patient, encounter, code, description from conditions cond
where cond.encounter in e.id

index by encounter, patient

=============================================

Select Start, stop, encounter, Code, description, base_cost, dispences, totalcost from medications m
where m.encounter in e.id

index by encounter

=============================================

select date, encounter, category, code, description, value, units, type from observations obs
where obs.encounter in e.id

index by id

=============================================

select id, name, utilization from organizations org
wehre org.id in e.organization

index by id

=============================================

select start, stop, encounter, code, description, base_cost from procedures proc
where proc.encounter in e.id

index by encounter

---

### 2026-02-10 – Mock dataset loaded to BigQuery + slim tables

- Installed and initialized Google Cloud CLI on laptop and linked it to project `healthcare-test-486920`.
- Created a new BigQuery dataset `Raw_csvs_test` and loaded initial CSVs (patients, encounters, careplans) directly from local files.
- Fixed location / permission issues when querying by:
  - Setting the correct processing location for the dataset.
  - Using fully qualified table names with the correct project ID.
- Designed and built first **slim tables** in BigQuery:
  - `patients_slim`:
    - Columns: `id, birthdate, deathdate, race, gender`.
    - Partitioned by `birthdate`, clustered by `id`.
  - `encounters_slim`:
    - Joined to patients to exclude encounters after death and to truncate at a global end date.
    - Filtered to clinically relevant encounter classes (`inpatient, emergency, urgentcare, outpatient, ambulatory, virtual`).
    - Prepared for later partitioning/clustering (date + patient).
  - `careplans_slim`:
    - Joined `careplans` to `encounters_slim` on `encounter` ID.
    - Partitioned by `stop`, clustered by `encounter, patient`.
- Performed basic sanity checks on `encounters_slim`:
  - Verified encounter counts, allowed `encounterclass` values, and absence of encounters after death or after the dataset end date.
- Captured first performance/architecture learnings:
  - BigQuery uses partitioning + clustering at table level (no `CREATE INDEX`).
  - `CREATE TABLE ... PARTITION BY ... CLUSTER BY ... AS SELECT ...` is the main pattern for building optimized tables from queries.

---

### 2026-02-11 - Helper Clinical table attempt 

Created first version of the **clinical helper table** for index_stay: wired encounters to claims and conditions, selected a primary diagnosis code per stay (with disorder‑first logic), and computed key aggregates – number of diagnoses, number of procedures, and a deduplicated count of chronic conditions per patient–stay.

---

### 2026-02-12 - Helper Clinical Table done 

### Cost aggregation and comorbidity notes

- In Synthea, `total_claim_cost` is a synthetic claim amount and does **not** exactly equal `base_encounter_cost + sum(line‑item base costs)`
- For this project, I define `totalstaycost = base_encounter_cost + totalprocedurecosts + totalmedicationcosts`, and use this for `costperdaystay`, `totalreadmissioncost`, and value‑of‑reduction calculations; `total_claim_cost` is kept only as an auxiliary feature.

- The clinical helper table is built using `code_dictionary` plus simple aggregates (`COUNTIF`, flags like `hasdiabetes`, `hascancer`, `hashiv`, `has_hf`, `has_alz`, `has_ckd`, `numchronicconditions`). 
- Current condition and procedure labels (chronic vs acute, cancer, HIV, HF, CKD, Alzheimer’s/dementia, and `is_surgery`) are based on **heuristic** code + text rules and one‑off LLM classification, not full SNOMED/ICD value sets. 
- For real‑world use these label definitions should be replaced by proper SNOMED/ICD hierarchies and procedure code sets; here they are treated as a pragmatic approximation to get the end‑to‑end project working.

---

### 2026-02-13 - Index Table Done

- Built and sanity‑checked first versions of all three helper tables: **clinical**, **cost aggregation**, and **utilization**, including refined logic for chronic flags, surgery history, and 365‑day utilization. 

- Implemented and debugged BigQuery → Python integration in a Jupyter notebook using a dedicated `.venv`, service‑account auth, and a fallback pattern for loading query results into pandas without relying on `to_dataframe

- Created the **Index Stay fact table** by joining inpatient `encounters_slim` with all helper tables, wiring in diagnoses, costs, 365‑day history, and readmission flags (`readmit_30d`, `readmit_90d`). 

- Organized project SQL into a Git‑tracked structure (`sql/01_raw_to_slim`, `02_helpers`, `03_index_fact`, `04_sanity_checks`), so every BigQuery table build has a corresponding `.sql` file under version control. 

- Clarified BigQuery **sandbox/free‑tier limits** (1 TiB queries + 10 GiB storage per month, no billing account linked) and agreed on habits to stay under the limit: avoid `SELECT *`, filter early, materialize heavy steps once, and optionally monitor TB via `INFORMATION_SCHEMA`. 
- Decided to **postpone full SNOMED CT integration** (FHIR/Snowstorm or offline library) and keep using the current heuristic `code_dictionary` and `procedures_dictionary` for condition/procedure labels, with a clear note to replace them in a later phase.

Got to use snomed API in order to classify procedures and conditions well

---

### 2026-02-14 - First Model Tests

- Built baseline **logistic regression** models for both 30‑day and 90‑day readmission prediction using the engineered index_stay feature set (with cross‑validated ROC‑AUC ≈ 0.91–0.92 and strong PR performance at low prevalence).

- Interpreted 90‑day model coefficients: prior utilisation (admissions_365d, tot_length_of_stay_365d), comorbidity burden (num_chronic_conditions, CKD), and log‑transformed cost variables emerged as the main risk drivers. 

- Evaluated 30‑day model and identified plausible overall performance but less stable coefficients for some chronic disease flags and raw cost fields, suggesting collinearity and remaining scaling issues on the 30‑day target.
- Found a bug in the utilisation helper logic (missing or mis‑computed readmit30d, readmit90d, fol_admit_id, fol_stay_date) while checking consistency between helper_util and index_stay. 

- Fixed this bug in the **test dataset**: reintroduced and corrected readmit30d, readmit90d, fol_admit_id, and fol_stay_date so that helper_util and index_stay are now logically consistent there, although the corresponding BigQuery SQL wasn’t saved.

- For the **full dataset**, left helper_util and index_stay unchanged in BigQuery for now and applied the corrected readmission logic in Python, ensuring the modelling pipeline uses the fixed labels while the warehouse tables remain at their previous version. 

---

### 2026-02-15 - Models' hyperparameters tuned

- Refactored the modelling code into a reusable `train_model` + `evaluate_model` pattern, with a model registry and metric logs (ROC AUC, PR AUC, plus per‑model coefficients / feature importances).
- Tuned and compared logistic, random forest, and LightGBM hyperparameters for readmit_30d and readmit_90d using `RandomizedSearchCV` with `scoring="average_precision"`, and inspected how different configs affect 30d vs 90d performance.
- Fixed convergence and performance issues in logistic regression by adjusting `max_iter`, penalty, and class weights, and by using a consistent train/test split for both 30d and 90d targets.
- Designed a caching pattern for dataset loading: first try loading from a local CSV/Parquet file, otherwise query BigQuery and save a local cache to avoid repeated warehouse reads. 

- Scoped and partially prototyped a SNOMED‑based dictionary approach (concept IDs → clinical flags like cancer, HIV, CKD, HF, diabetes, dementia) including a plan to hit a terminology server and cache results for later use in helper tables.

- Attempted a local PySpark + Pathling setup for SNOMED work, discovered Java/Spark compatibility issues on Windows (Py4J `getSubject` error), and decided to pause Spark for now and instead pursue a pure‑Python SNOMED dictionary path in a future session. 

---

### 2026-02-15 - First Business Results Found

- Tuned hyperparameters for **light GBM** and **random forest** on the 30‑day readmission task, cleaned up the search space, and enforced sensible RF tree constraints (e.g. `min_samples_split` vs `min_samples_leaf`). 
- Implemented a **threshold‑based cost/value pipeline** using predicted probabilities, extra intervention days, capped `cost_per_day_stay`, and scenario‑based risk reduction to compute net savings vs readmission costs. 

- Found that the best current operating point is **logistic regression (30d) at threshold 0.85**, with ~66 % recall, ~45 % precision, and about **5 % maximum projected savings** of combined readmission cost on the current test cohort.

- Verified that very short, high‑cost stays were inflating intervention costs and corrected this by capping `cost_per_day_stay` for policy calculations and softening extra‑days rules, which removed large negative net‑value artifacts. 

- Not yet: validated the cost pipeline on a **larger synthetic dataset**, re‑trained models on that larger cohort, or compared model performance/cost curves in a stable “production‑like” setting (these remain to‑dos). 

- Plan for tomorrow: **regenerate a larger Synthea dataset** (e.g. 20k+20k patients), reload and rebuild BigQuery slim/helper/index tables, retrain logistic/RF (and possibly LightGBM) on the new data, and re‑run the cost/value pipeline to check if the ~5 % savings estimate holds or improves. 

- Found out that encounters older than 8 years are chronic conditions that are either still present, or are ongoing

---

### 2026-02-15-2026-02-21 - Bringing code and repo to an adequate state

ERROR in test_preprocess.ipynb when calculating gains, sth doesn't match big time
Index in df_cost and in results['pred_values'] is out of order

- add stay_type to index_stay_table
//done

- cross-validate train_test stuff to get more metrics //1st priority
//done

- what do I need 90 days data for

- filter admission_date for last 8 years
//fixed in data.filter_data with function, but can be fixed in bigquery with filters
//done in bigquery

- decide for a better intervention type, look for cost reduction there

- map different readmission probability reductions
//done as functions, *seen values

- reorganize code so that it is functions and variables
//done

- skip cross-validation when building models if I build cross-validated models
//done

- in results['pred_values'] some readmit_90d = 1 are converted into readmit_90d = 0
//fixed

- write docs to functions so that it is all readable

---

### 2026-02-15-2026-02-21 - Built and debugged full cross-validation for the entire dataset

Task - dig deeper into data for:

- stay types for index stay table;
//done, but index stay not yet recompiled

- diagnoses or some forms of diagnosis groups
//done

- mb count ambulatory procedures or any maintanence types

Tasks with the data itself and with index_stay table

- see what morphologic abnormality means
//done

- check my project question framing: I try to prevent only related readmission or all of them
---

- fixed flags in helper_utilization, and sanity checked new flags

- morphological abnormalities are only sprains and brain injuries that all fall into index stay table as their stay_type is emergency

- findings from talk with Jenya: each component should be wrapped into an object/class (Holy Shit)

- check when I scale my data and on what
//checked
- find a way to make a dataset balanced
//found

- full kfold sucks, gotta leave out some data

- gotta resolve multicollinearity in stay_type
//done

---

### 2026-02-24 - Almost built a dictionary for procedures, ready to build diagnoses, but not mapped yet

- snowstorm api actually works

- created a list of non-procedural parents, and added other flag called therapies

- now data.py file: data.load_data() gets data_path, sql, query = False, gotta fix in test_preprocedss.ipynb and in config

sql = """
    SELECT
        *
        from `hospital-readmission-4.helper_tables.index_stay`
    """
save_csv_path = 'D:\Python Projects\Hospital readmission risk\data\cleaned\index_stay.csv'

//done

- created a list for diagnoses, but not yet run dictionary builder

Tasks apart from the obvious:

- save individual procedure and diagnosis jsons into separate files for a faster lookup and less memory
//done
- I don't check whether readmission is related to the previous condition, it's only done by querying snomed, or mb there is another way
//functions are written, but not checked
- find intervention types for diagnoses, mb explore the data a bit to find insights

- mb count ambulatory procedures or any maintanence types

- an encounter can be planned if there was a scheduling procedure prior to it(in procedures_slim, code 410538000), gotta remark planned/unplanned accordingly
//flags are done but no remapping

- findings from talk with Jenya: each component should be wrapped into an object/class (Holy Shit)

-remove full Kfold, rather load new bigger dataset

---
### 2026-02-25 Built both dictionaries, only diagnoses descriptions are missing
---
### 2026-02-26 Keep modifying dictionaries

- verified that every code in conditions_slim has a correspondence in unique_diagnoses

- descriptions for diagnoses built

- diagnosis flags built and chronic flag fixed

- issue with drug abuse, there is only name for it in uk-snomed
//resolved somehow

- sth is wrong with multidisorder code
//solved

- with diagnoses dictionary also check "due to" realtionships
//later afterindex_stay full reconstruction

---
### 2026-02-26

- changed query of creation of unique procedures and diagnoses, now only code and name columns for easier mapping

- updating diagnosis dictionary for full coverage of disorders
93% covered

- updating diagnosis dictionary to map findings
no reason to do it, main are stress, pregnancy, other BS

- gotta rebuild diagnoses dictionary for all
//done

- started rebuilding helper_clinical_creation, gotta rebuild planned stuff and related surgeries
---
### 2026-02-27

- Some inpatient stays for the same patient are split into 2 or more different stays because of a transfer to a different unit, although a patient was not really discharged. This gets fixed on the stage of index_stay creation by merging adjacent records in encounters and readjusting all values and combining them together
//done
- For each planned inpatient stay there is an ambulatory visit which is admission to a hospital itself as a procedure followed by an immediate inpatient stay, 
//done

idea: group visits with similar date and group their diagnoses to find their diagnoses from there and check relations with readmissions from those groupings
//done

To build that I will have to rebuild all helper tables, not excluding any other stays, than group them and aggregate all metrics by summing/min/max/avg/main_diagnosis/los etc. But with costs one aggregated table is needed and one non-aggregated to see how much will be saved on checkups and readmissions.

Finding: if there was a (urgetcare/inpatient/emergency) encounter, then discharge, then an ambulatory during which a new inpatient was planned, that is exactly what I am trying to target and prevent. That's why when building is_planned flags from procedures, ambulatory visits can't be counted. However they must be counted when reducing cost, because those ambulatories are costly.

- is_planned flag updated and built

- helper_clinical recreated and sanity checked

- idea: to automate all steps in bigquery with dictionaries and realted stuff viausing bigquery api for tables creation

-started building groupped helper table for all encounters in helper clinical

---
### 2026-02-28

- Rebuilt main_diagnoses with respect to grouped encounters, main issues noted only with pregnancy and anemia/metabolic syndrom X

- Possibly finished diagnoses relations but not tested yet
---
### 2026-03-01

- Main diagnoses rebuilt and loaded

- helpers grouped are built

- rebuilt dependent_diagnoses with respect to new basic set and loaded it

- rebuilt index_stay

- gotta revisit my to-do list, rebuild models and decide for intervention type

---
### 2026-03-02 

- Checked whether there are many planned readmission that are planned after the discharge date

- Checked data loading and preprocessing for test-set

- Built bigger dictionaries for diagnoses and procedures, haven't loaded yet

- Gotta make an additional deep research on main_diagnoses and/or sanity check them

- Done, now it gets either the one lower down the tree or the one with less descendants

- Gotta rebuild main_diagnoses and dependent diagnoses and index_stays

- Found out that querying in the wrong snomed API version can lead to stupid results

- uploaded train_main_diagnoses to BQ

Tasks:

- Gotta rebuild main_diagnoses and dependent diagnoses and index_stays
---
### 2026-03-03
- If any table is created from drive, a copy needs to be created for BQ API to query it well

- Both index_stays constructed

- Ran full models, crossval sucks, metrics are not the best

-Gotta build costs, but models look promising

---
### 2026-03-04 

- Built mapping of desired probability reduction and daily probability reduction, nothing worked well

- vast goddamn majority of related readmissions is lung cancer. Gotta find a way to treat lung cancer effectively or to deep dive into records for extra data on interventions

- intervention type is still key, most of readmit_30d that are unrelated or unknown, are still with lung cancer in the end.

---
###2026-03-06

- loaded observations into BQ

- maybe careplans are meant to last for more than 60 days if the enddate is not given?

- it looks like there is a careplan for every cancerous encounter, which is not limited to 60 days

- why are full time employment and carcinoma of lung related??

- rebuilt related_diagnosis_builder by fixing is_or_has_ancestor_in in dictionaries

- Gotta rebuild index_stay and models with it

---
#2026-09-03

- Careplans have got reasons to be implemented, I can filter encounters by relation to careplan reasoncode

- Gotta update the logic of creating planned readmissions. If planned flag is taken from careplans, it must check whether main_code of this stay_id is related to careplan reason_code

- Never terminate a careplan, because they can be long-lasting

- built careplan_related_encounters

- after rebuilding test set only 47/106761  are related readmissions in 30 days, 110/106761 in 90 days, in index_stay for test data it is 686/66489 simple readmissions in 30 days, mb not related and only 24/66465 related readmissions in 30 days

- recreated both index_stays

- rebuilt both models, they seem bad, gotta reoptimize hyperparameters and see what I can do with costs and build them

- Obtained results and an "avoided" table , where all the mapped results are tracked

---
#2026-10-03

- Mapped all values, obtained maximum results ranging from the best unrealistic 86% reductions to -1% as the worst possible result. The best and the most realistic one is -26%($2M) in standard setting (daily reduction 10%, target 20-25%, real reduction 27%). Even in rather pessimistic scenarios like 5% daily reduction, 20% target reduction - obtained best reduction of 21% of costs($1.6M). 

- The best model is RF_d30_0.8, even 14% precision, 14% recall, 0.14 f1 is enough for such results, MB many of low-cost are targeted as false positives, which is fine

- Run first hyperparameter optimization, haven't updated it yet, mb gotta rerun in order to reoptimize

- Started resructuring the entire project to apply OOP and move to production-like deployment

---
#2026-11-03

- Built OOP pipeline until the point where the slim_tables are created

- The first dictionaries are in csv forms but not yet automatically loaded into BQ, gotta build a module load_dictionaries within the BigQueryLoader
---

#2026-12-03

- Query 12 is changed from main_diagnoses_nat to main_diagnoses to check the pipeline, but gotta rewrite that part for bigger diagnoses tables with google drive

- pipeline to index_stay done, no data sanity checks yet

- sanity checks written but gotta recheck the sanity check queries themselves

- sanity checks are done

- Got to find a way to run uploads through google drive//no need for that as API handles huge uploads

- Hyperparameter tuner is constructed, gotta test it now somehow, but need more data

- index_stay table has messed up name of csv file when loaded, underscore missing after mock

---
#2026-13-03

- hyperparameters tuned on a bigger mock dataset

- constructed model_config_manager, model_registry and evaluator, changed config to test that on real project data, created a test cell in ipynb, but haven't tested yet

- gotta fix the part where I obtain pred_values

---
#2026-16-03

- Finished entrie pipeline from generating Synthea data to obtaining metrics

---
#2026-17-03

- Working on targeted hyperparameter tuning to optimize cost reduction and not classic metrics

- Building a dataframe report for constructed models

- Need to be parsing dataset_end_date somehow

- Reports are now printed well, however the cost reductions are not as good as before

- Created and saved report, now at 11% save from RF 0.9, maybe gotta retune hyperparams or whatever

---
#2026-18-03

- Started refactoring project for continuous flow, created slim tables again