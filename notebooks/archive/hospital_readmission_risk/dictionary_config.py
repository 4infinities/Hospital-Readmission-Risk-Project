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
