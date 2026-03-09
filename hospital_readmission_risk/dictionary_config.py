"""
Configuration for SNOMED-based dictionaries and related SQL pipelines.

Defines:
- SNOMED target concept sets for procedures and diagnoses.
- Basic ancestor sets used to stop ancestry walks.
- Output schemas for main-diagnosis tables.
- Per-dictionary IO config: state paths, local CSV paths, and SQL queries.
- Snowstorm (SNOMED) API location and caching globals.

Used by `dictionaries.py` to build and persist code dictionaries and relations.
"""

from pathlib import Path

# ---------------------------------------------------------------------
# SNOMED target concept sets
# ---------------------------------------------------------------------

# High-level SNOMED parents that define procedure-related flags.
procedure_targets = {
    "is_procedure": {
        "243796009",  # situation
        "14734007",   # admin_procedure
        "409073007",  # education_procedure
        "108217004",  # interview, history, physical examination
        "119270007",  # management procedure
        "185316007",  # indirect encounter
        "122869004",  # measurement procedure
        "363787002",  # observation
        "404684003",  # clinical finding
        "72353004",   # psychosocial
        "260787004",  # physical_object
        "11429006",   # consultation
    },
    "is_therapy": {"243120004"},
    "is_surgery": {"387713003"},
    "is_planning": {"410538000", "183976008"},
}

# High-level SNOMED parents that define diagnosis-related flags.
diagnosis_targets = {
    "is_disorder": {"64572001"},
    "is_symptom": {"404684003"},
    "is_dementia": {"52448006"},
    "is_cancer": {"363346000"},
    "is_hiv": {"86406008"},
    "is_hf": {"105981003"},
    "is_ckd": {"709044004"},
    "is_diabetes": {"44054006"},
    "is_lf": {"235856003"},
    "is_chronic": {"27624003"},
    "inflammation": {"363171009"},
    "musculoskeletal": {"928000"},
    "nervous": {"118940003"},
    "respiratory": {"50043002"},
    "cardiac": {"49601007"},
    "renal": {"90708001"},
    "trauma": {"417163006"},
    "intoxication": {"1149322001"},
}

# Concepts where ancestry search should stop (generic / very high-level).
basic_set = {
    "138875005",  # snomed concept
    "404684003",  # clinical finding
    "64572001",   # disease(disorder)
    "844005",     # behaviour finding
    "362965005",  # disorder of body system
    "417163006",  # injury
    "76712006",   # disorder of digestive organ
    "118228005",  # functional finding
    "27624003",   # chronic disease
    "105612003",  # injury of internal organ
    "302768007",  # employment finding
    "384821006",  # mental state finding
    "365526009",  # job details finding
    "34014006",   # viral disease (generic)
    "128139000",  # inflammatory disorder
    "22253000",   # pain
    "302292003",  # finding of trunk structure
    "363171009",  # inflammation of specific body systems
}

# SNOMED codes used to prioritise viral diagnoses.
viral_codes = {"34014006"}

# Columns produced by the main-diagnoses builder.
main_diags_output_cols = [
    "main_diagnosis_code",
    "main_diagnosis_name",
    "main_diagnosis_type",
    "num_of_disorders",
    "num_of_findings",
]

# ---------------------------------------------------------------------
# Per-dictionary IO configuration
# ---------------------------------------------------------------------

# Config for each dictionary type:
# - state: JSON state file with CACHE/RESULTS/ANCESTORS.
# - targets: SNOMED high-level target concepts.
# - train/test: local CSV paths, SQL to pull raw data, output CSV paths.
config_data = {
    "procedures": {
        "state": Path(
            "D:\\Python Projects\\Hospital readmission risk\\data\\intermediate\\procedures_snomed_state.json"
        ),
        "targets": procedure_targets,
        "train": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\dictionaries\train_unique_procedures.csv",
            "sql": """
select * 
from `raw_data_for_dictionaries.train_unique_procedures`
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\train_procedures_dictionary.csv",
        },
        "test": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\dictionaries\unique_procedures.csv",
            "sql": """
select *
from `raw_data_for_dictionaries.unique_procedures`
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\procedures_dictionary.csv",
        },
    },
    "diagnoses": {
        "state": Path(
            "D:\\Python Projects\\Hospital readmission risk\\data\\intermediate\\diagnosess_snomed_state.json"
        ),
        "targets": diagnosis_targets,
        "train": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\dictionaries\train_unique_diagnoses.csv",
            "sql": """
select *
from `raw_data_for_dictionaries.train_unique_diagnoses`
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\train_diagnoses_dictionary.csv",
        },
        "test": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\dictionaries\unique_diagnoses.csv",
            "sql": """
select *
from `raw_data_for_dictionaries.unique_diagnoses`
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv",
        },
    },
    "main_diagnoses": {
        "state": Path(
            "D:\\Python Projects\\Hospital readmission risk\\data\\intermediate\\diagnosess_snomed_state.json"
        ),
        "output_cols": main_diags_output_cols,
        "train": {
            "dictionary_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\train_diagnoses_dictionary.csv",
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\train_diagnoses_per_stays.csv",
            "sql": """
WITH
group_flags AS (
  SELECT
    id,
    patient,
    start,
    CASE encounterclass
      WHEN 'ambulatory' THEN 1
      WHEN 'outpatient' THEN 2
      WHEN 'virtual' THEN 3
      WHEN 'urgentcare' THEN 4
      WHEN 'emergency' THEN 5
      WHEN 'inpatient' THEN 6
      ELSE 99
    END type_flag,
    CASE
      WHEN date_diff(
        start,
        lag(stop, 1) OVER (PARTITION BY patient ORDER BY start ASC),
        hour
      ) < 12
      THEN 0
      ELSE 1
    END AS group_change
  FROM `hospital-readmission-4.data_slim.train_encounters_slim`
),
clusters AS (
  SELECT
    id,
    patient,
    start,
    type_flag,
    sum(group_change) OVER (
      PARTITION BY patient
      ORDER BY start ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) group_number
  FROM group_flags
),
best_stay_per_group AS (
  SELECT
    patient,
    group_number,
    id AS group_id,
    ROW_NUMBER() OVER (
      PARTITION BY patient, group_number
      ORDER BY
        type_flag DESC,
        start ASC,
        id ASC
    ) AS rn
  FROM clusters
),
final_groups AS (
  SELECT
    clust.id,
    best.group_id,
  FROM clusters clust
  LEFT JOIN best_stay_per_group best
    ON best.patient = clust.patient
   AND best.group_number = clust.group_number
   AND best.rn = 1
),
claims AS (
  SELECT DISTINCT
    encounter AS id,
    diagnosis1,
    diagnosis2,
    diagnosis3,
    diagnosis4,
    diagnosis5,
    diagnosis6,
    diagnosis7,
    diagnosis8
  FROM hospital-readmission-4.data_slim.train_claims_slim
)
SELECT
  final.id,
  final.group_id,
  cl.diagnosis1,
  cl.diagnosis2,
  cl.diagnosis3,
  cl.diagnosis4,
  cl.diagnosis5,
  cl.diagnosis6,
  cl.diagnosis7,
  cl.diagnosis8
FROM final_groups final
LEFT JOIN claims cl
  ON final.id = cl.id
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\train_main_diagnoses.csv",
        },
        "test": {
            "dictionary_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv",
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\diagnoses_per_stays.csv",
            "sql": """
with group_flags as (
  SELECT
    id,
    patient,
    start,
    CASE encounterclass
      WHEN 'ambulatory' THEN 1
      WHEN 'outpatient' THEN 2
      WHEN 'virtual' THEN 3
      WHEN 'urgentcare' THEN 4
      WHEN 'emergency' THEN 5
      WHEN 'inpatient' THEN 6
      ELSE 99
    END type_flag,
    CASE
      WHEN date_diff(
        start,
        lag(stop, 1) OVER (PARTITION BY patient ORDER BY start ASC),
        hour
      ) < 12
      THEN 0
      ELSE 1
    END AS group_change
  FROM `hospital-readmission-4.data_slim.encounters_slim`
),
clusters as (
  select 
    id,
    patient,
    start,
    type_flag,
    sum(group_change) over(
      partition by patient
      order by start asc
      rows between unbounded preceding and current row
    ) group_number
  from group_flags
),
best_stay_per_group AS (
  SELECT
    patient,
    group_number,
    id AS group_id,
    ROW_NUMBER() OVER (
      PARTITION BY patient, group_number
      ORDER BY
        type_flag DESC,
        start ASC,
        id ASC
    ) AS rn
  FROM clusters
),
final_groups as (
  SELECT
    clust.id,
    best.group_id,
  FROM clusters clust
  LEFT JOIN best_stay_per_group best
    ON best.patient = clust.patient
   AND best.group_number = clust.group_number
   AND best.rn = 1
),
claims as (
  select distinct
    encounter as id,
    diagnosis1,
    diagnosis2,
    diagnosis3,
    diagnosis4,
    diagnosis5,
    diagnosis6,
    diagnosis7,
    diagnosis8
  from hospital-readmission-4.data_slim.claims_slim
)
select 
  final.id,
  final.group_id,
  cl.diagnosis1,
  cl.diagnosis2,
  cl.diagnosis3,
  cl.diagnosis4,
  cl.diagnosis5,
  cl.diagnosis6,
  cl.diagnosis7,
  cl.diagnosis8
from final_groups final
left join claims cl
  on final.id = cl.id
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\main_diagnoses.csv",
        },
    },
    "related_diagnoses": {
        "state": Path(
            "D:\\Python Projects\\Hospital readmission risk\\data\\intermediate\\diagnosess_snomed_state.json"
        ),
        "output_cols": ["is_related"],
        "train": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\train_diagnoses_and_following.csv",
            "dictionary_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\train_diagnoses_dictionary.csv",
            "sql": """
with sec_codes as (
  select 
    stay_id,
    following_stay_id as fol_id,
    readmit_30d,
    readmit_90d,
    main.main_diagnosis_code sec_code
  from `hospital-readmission-4.helper_tables.train_helper_utilization` hu
  left join `hospital-readmission-4.data_slim.train_main_diagnoses_nat` main
    on hu.following_stay_id = main.id
)
SELECT
  sec.stay_id,
  main.main_diagnosis_code as code,
  sec.fol_id, 
  sec.sec_code,
  readmit_30d,
  readmit_90d
FROM sec_codes sec
left join `hospital-readmission-4.data_slim.train_main_diagnoses_nat` main
  on main.id = sec.stay_id
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\train_related_diagnoses.csv",
        },
        "test": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\diagnoses_and_following.csv",
            "dictionary_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv",
            "sql": """
with sec_codes as (
  select 
    stay_id,
    following_stay_id as fol_id,
    readmit_30d,
    readmit_90d,
    main.main_diagnosis_code sec_code
  from `hospital-readmission-4.helper_tables.helper_utilization` hu
  left join `hospital-readmission-4.data_slim.main_diagnoses` main
    on hu.following_stay_id = main.id
)
SELECT
  sec.stay_id,
  main.main_diagnosis_code as code,
  sec.fol_id, 
  sec.sec_code,
  readmit_30d,
  readmit_90d
FROM sec_codes sec
left join `hospital-readmission-4.data_slim.main_diagnoses` main
  on main.id = sec.stay_id
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\related_diagnoses.csv",
        },
    },
    "careplans_related_diagnoses": {
        "state": Path(
            "D:\\Python Projects\\Hospital readmission risk\\data\\intermediate\\diagnosess_snomed_state.json"
        ),
        "output_cols": ["is_planned"],
        "train": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\train_diagnoses_and_careplans.csv",
            "dictionary_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\train_diagnoses_dictionary.csv",
            "sql": """
WITH
end_date AS (
  SELECT MAX(stop) AS end_ts
  FROM `hospital-readmission-4.data_slim.train_encounters_slim`
)
SELECT
  DISTINCT
  main.id AS stay_id,
  main.main_diagnosis_code AS code,
  care.code AS sec_code
FROM `data_slim.train_main_diagnoses_nat` main
LEFT JOIN `data_slim.train_encounters_slim` e
  ON main.id = e.id
LEFT JOIN `data_slim.train_careplans_slim` care
  ON e.patient = care.patient
WHERE
  DATE(e.start) > care.start
  AND DATE(e.start) < COALESCE(
    care.stop,
    DATE((SELECT end_ts FROM end_date))
  )
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\train_careplans_related_encounters.csv",
        },
        "test": {
            "data_path": r"D:\Python Projects\Hospital readmission risk\data\raw\diagnoses_and_careplans.csv",
            "dictionary_path": r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv",
            "sql": """
WITH
end_date AS (
  SELECT MAX(stop) AS end_ts
  FROM `hospital-readmission-4.data_slim.encounters_slim`
)
SELECT
  DISTINCT
  main.id AS stay_id,
  main.main_diagnosis_code AS code,
  care.code AS sec_code
FROM `data_slim.main_diagnoses` main
LEFT JOIN `data_slim.encounters_slim` e
  ON main.id = e.id
LEFT JOIN `data_slim.careplans_slim` care
  ON e.patient = care.patient
WHERE
  DATE(e.start) > care.start
  AND DATE(e.start) < COALESCE(
    care.stop,
    DATE((SELECT end_ts FROM end_date))
  )
""",
            "write_path": r"D:\Python Projects\Hospital readmission risk\data\processed\careplans_related_encounters.csv",
        },
    },
}

# ---------------------------------------------------------------------
# SNOMED / Snowstorm API and cache globals
# ---------------------------------------------------------------------

# Local folder where concept JSON files are cached.
concept_path = r"D:\Python Projects\Hospital readmission risk\data\concepts"

# Snowstorm API base configuration (keep in sync with SNOMED browser).
BASE = "https://termbrowser.nhs.uk/sct-browser-api/snomed"
EDITION = "uk-edition"
RELEASE = "v20260211"  # keep in sync with what you see in the browser

# HTTP retry and backoff settings for concept fetches.
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0

# Global counters/state used by dictionaries.py.
REQUEST_COUNT = 0
CACHE: dict[str, str] = {}              # concept_id -> local JSON path
RESULTS: dict[str, dict[str, bool]] = {}  # flag -> concept_id -> bool
ANCESTORS: dict[str, set[str]] = {}       # concept_id -> set of ancestors
