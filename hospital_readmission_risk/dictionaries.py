import time
import requests
import json
from pathlib import Path
import pandas as pd
from data import load_data

# ----- CONFIGURE THESE -----
procedure_data_path = r"D:\Python Projects\Hospital readmission risk\data\raw\dictionaries\unique_procedures.csv"
procedure_write_path = r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\procedures_dictionary.csv"
diagnoses_data_path = r"D:\Python Projects\Hospital readmission risk\data\raw\dictionaries\unique_diagnoses.csv"
diagnoses_write_path = r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv"
concept_path = r"D:\Python Projects\Hospital readmission risk\data\concepts"
procedure_sql = """
    select * 
    from `raw_data_for_dictionaries.unique_procedures`
"""
diagnoses_sql = """
    select *
    from `raw_data_for_dictionaries.unique_diagnoses`
"""
BASE = "https://termbrowser.nhs.uk/sct-browser-api/snomed"
EDITION = "uk-edition"
RELEASE = "v20251119"  # keep in sync with what you see in the browser
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0
REQUEST_COUNT = 0
STATE_PATH = Path("D:\Python Projects\Hospital readmission risk\data\intermediate\diagnosess_snomed_state.json")
#STATE_PATH = Path("D:\Python Projects\Hospital readmission risk\data\intermediate\procedures_snomed_state.json")
CACHE: dict[str, str] = {}
RESULTS: dict[str, dict[str, bool]] = {} 
procedure_targets = {    
    "is_procedure": {
        "243796009", #situation
        "14734007", #admin_procedure
        "409073007", #education_procedure
        "108217004", #interview, history, physical examination
        "119270007", #management procedure
        "185316007", #indirect encounter
        "122869004", #measurement procedure
        "363787002", #observation
        "404684003", #clinical finding
        "72353004", #psychosocial
        "260787004", #physical_object
        "11429006" #consultation
                    },
    "is_therapy" : {"243120004"},
    "is_surgery" : {"387713003"},
    "is_planning" : {"410538000", "183976008"}
}    
diagnosis_targets = {
    "is_disorder" : {"64572001"},
    "is_symptom" : {"404684003"},
    "is_dementia" : {"52448006"},
    "is_cancer" : {"363346000"},
    "is_hiv": {"86406008"},
    "is_hf": {"105981003"},
    "is_ckd": {"709044004"},
    "is_diabetes": {"44054006"},
    "is_lf" : {"235856003"},
    "is_chronic": {"27624003"},
    'inflammation': {'363171009'},
    'musculoskeletal': {'928000'},
    'mental': {'74732009'},
    'nervous': {'118940003'},
    'respiratory': {'50043002'},
    'cardiac': {'49601007'},
    'renal': {'90708001'}
}

session = requests.Session()

def load_state():

    global CACHE, RESULTS

    if not STATE_PATH.exists():
        return

    with STATE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    raw_cache = data.get("cache", {})
    CACHE = {code: path for code, path in raw_cache.items()}
    """
    raw_results = data.get("results", {})  # dict[str, dict]
    RESULTS = {
        code: {flag: bool(val) for flag, val in flags.items()}
        for code, flags in raw_results.items()
            }
            """

def save_state():

    data = {
        "cache": CACHE,
        "results": RESULTS,
    }
    
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f)

def save_concept(concept_id, data, save_path = concept_path):

    global REQUEST_COUNT

    path = Path(save_path)
    path.mkdir(parents=True, exist_ok=True)

    file_name = f"{concept_id}.json"
    file_path = path/file_name

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    CACHE[concept_id] = str(file_path)

    REQUEST_COUNT = REQUEST_COUNT + 1

    if REQUEST_COUNT % 100 == 0:
        save_state()

def read_concept(concept_path):

    path = Path(concept_path)

    if not path.exists():
        return {}
    
    with path.open("r", encoding="utf-8") as f:
        return json.load(f) 

def get_concept(concept_id: str, targets) -> dict:
    """Fetch a SNOMED concept JSON once, with simple 429 handling and caching."""

    if concept_id in CACHE:

        return read_concept(CACHE[concept_id])

    url = f"{BASE}/{EDITION}/{RELEASE}/concepts/{concept_id}"

    for attempt in range(1, MAX_RETRIES + 1):

        resp = session.get(url)

        if resp.status_code == 429:
            # too many requests - small backoff then retry
            time.sleep(BACKOFF_SECONDS * attempt)
            continue

        if resp.status_code in (404, 410, 500, 502, 503, 504):

            save_concept(concept_id, {})

            for flag in targets:
                RESULTS[flag][concept_id] = False
                
            return {}

        resp.raise_for_status()

        data = resp.json()

        save_concept(concept_id, data)
        
        return data

    # if we get here, all retries hit 429 or other non-OK without raise
    resp.raise_for_status()

def get_parent_ids(concept: dict) -> list[str]:

    """Extract parent conceptIds from the concept JSON (adjust to actual structure)."""

    parents = []

    for rel in concept.get("relationships", []):
        # 'is a' relationship has conceptId 116680003 in SNOMED CT
        if rel.get("type", {}).get("conceptId") == "116680003" and rel.get("active"):

            target = rel.get("target") or {}

            cid = target.get("conceptId")

            if cid:
                
                parents.append(cid)
            
    return parents

def is_or_has_ancestor_in(concept_id: str, targets, target_ids: set[str], flag: str, max_depth: int = 10) -> bool:
    # if we’ve already fully explored this concept’s ancestors in a previous run,
    # just return the previous answer if you also store it, or skip re-walk.

    if flag not in RESULTS: 
        RESULTS[flag] = {}

    if concept_id in RESULTS[flag]:
        return RESULTS[flag][concept_id]

    visited = set()
    frontier = {concept_id}
    result = False

    if concept_id in target_ids:
        RESULTS[flag][concept_id] = True
        return True

    for depth in range(max_depth):

        next_frontier = set()

        for cid in frontier:

            if cid in visited:
                continue

            if cid in RESULTS[flag] and RESULTS[flag][cid]:

                RESULTS[flag][concept_id] = True
                return True

            visited.add(cid)

            c = get_concept(cid, targets)

            if not isinstance(c, dict):
                continue

            parents = get_parent_ids(c)

            if target_ids.intersection(parents):

                RESULTS[flag][concept_id] = True
                
                if cid != concept_id:
                    RESULTS[flag][cid] = True

                return True
            
            next_frontier.update(p for p in parents if p not in visited)

        if not next_frontier:
            break
        
        frontier = next_frontier

    RESULTS[flag][concept_id] = False

    return result

def build_dictionary(data, targets):

    for id in data['code']:

        for flag in targets:

            b = is_or_has_ancestor_in(str(id), targets, targets[flag], flag)
        
def build_flags(data: pd.DataFrame):

    data.set_index('code', inplace = True)

    for flag in RESULTS:
        
        col = pd.Series(RESULTS[flag], name = flag)
        col.index = col.index.astype(int)
        data[flag] = col

        if flag == 'is_procedure':
                data[flag] = ~data[flag]

    cols = [col for col in data.columns if col.startswith("is_")]
    data[cols] = data[cols].map(lambda x: int(x))

def get_description(concept_id: str):

    if CACHE[concept_id]:

        return get_concept(concept_id, diagnosis_targets)["descriptions"][0]["term"]

    return None

def fill_descriptions(data, description_col = 'description'):

    description_na_mask = data[description_col].isna()

    for id in data.loc[description_na_mask, :].index:
         
        data.loc[id, description_col] = get_description(str(id))

def fix_disorders(data):

    disorder_mask = data['is_disorder'] == 1
    data.loc[disorder_mask, 'is_symptom'] = 0

def fix_chronic(data):

    chronic_mask = data.loc[:, 'is_dementia':'is_lf'].sum(axis = 1) > 0
    data.loc[chronic_mask, 'is_chronic'] = 1

def fix_flags(data):

    fix_disorders(data)
    fix_chronic(data)

def pack_dictionary(data, path: str):

    data.to_csv(path)

if __name__ == "__main__":
    """
    Running checklist:
    1. Comment wrong path in STATE_PATH at the beginning
    2. In load_state() comment results if rebuilding flags
    3. Here comment all that is relevant to another dictionary
    """


    load_state()
    
    #data = load_data(procedure_data_path, procedure_sql)           # load previous results

    data = load_data(diagnoses_data_path, diagnoses_sql)
    
    try:
        
        #build_dictionary(data, procedure_targets)
        build_dictionary(data, diagnosis_targets)

    finally:
        save_state()

        
    build_flags(data)

    #fill_descriptions(data, 'description')
    #pack_dictionary(data, procedure_write_path) 

    fill_descriptions(data, 'diagnosis_name')
    fix_flags(data)
    

    cols = ['inflammation', 'nervous', 'musculoskeletal', 'mental', 'renal', 'cardiac', 'respiratory']
    data[cols] = data[cols].map(lambda x: int(x))

    #pack_dictionary(data, diagnoses_write_path)
