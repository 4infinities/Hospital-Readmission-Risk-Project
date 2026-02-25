import time
import requests
import json
from pathlib import Path
import pandas as pd
from data import load_data

data_path = r"D:\Python Projects\Hospital readmission risk\data\raw\diagnoses_per_stays.csv"
dictionary_path = r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv"
STATE_PATH = Path("D:\Python Projects\Hospital readmission risk\data\intermediate\diagnosess_snomed_state.json")
concept_path = r"D:\Python Projects\Hospital readmission risk\data\concepts"
write_path = r"D:\Python Projects\Hospital readmission risk\data\processed\main_diagnoses.csv"
CACHE: dict[str, str] = {}
sql = """
select distinct
i.stay_id,
  cl.diagnosis1,
  cl.diagnosis2,
  cl.diagnosis3,
  cl.diagnosis4,
  cl.diagnosis5,
  cl.diagnosis6,
  cl.diagnosis7,
  cl.diagnosis8
from hospital-readmission-4.helper_tables.index_stay i
left join hospital-readmission-4.data_slim.claims_slim cl
on i.stay_id = cl.encounter
"""
output_cols = ['main_diagnosis_code', 'main_diagnosis_name', 'main_diagnosis_type',
'num_of_disorders', 'num_of_findings']
viral_codes = {'34014006'}

def get_dictionary(path):

    dictionary = pd.read_csv(path)

    dictionary.set_index('code', inplace = True)

    return dictionary

def disorders_and_symptoms_split(codes: set, dictionary):

    disorders = set()
    symptoms = set()

    for code in codes:

        if int(code) in dictionary.index:

            if dictionary.loc[int(code), 'is_disorder'] == 1:
                disorders.add(code)

            if dictionary.loc[int(code), 'is_symptom'] == 1:
                symptoms.add(code)

    return disorders, symptoms

def load_state():

    global CACHE

    if not STATE_PATH.exists():
        return

    with STATE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    raw_cache = data.get("cache", {})
    CACHE = {code: path for code, path in raw_cache.items()}

def save_concept(concept_id, data, save_path = concept_path):

    path = Path(save_path)
    path.mkdir(parents=True, exist_ok=True)

    file_name = f"{concept_id}.json"
    file_path = path/file_name

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    CACHE[concept_id] = str(file_path)

def read_concept(concept_path):

    path = Path(concept_path)

    if not path.exists():
        return {}
    
    with path.open("r", encoding="utf-8") as f:
        return json.load(f) 

def get_concept(concept_id: str) -> dict:

    if concept_id in CACHE:

        return read_concept(CACHE[concept_id])

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

def is_or_has_ancestor_in(concept_id: str, target_ids: set[str], max_depth: int = 10) -> bool:
    # if we’ve already fully explored this concept’s ancestors in a previous run,
    # just return the previous answer if you also store it, or skip re-walk.
    visited = set()
    frontier = {concept_id}
    result = False

    if concept_id in target_ids:
        return True

    for depth in range(max_depth):

        next_frontier = set()

        for cid in frontier:

            if cid in visited:
                continue

            visited.add(cid)

            c = get_concept(cid)

            if not isinstance(c, dict):
                continue

            parents = get_parent_ids(c)

            if target_ids.intersection(parents):
                return True
            
            next_frontier.update(p for p in parents if p not in visited)

        if not next_frontier:
            break
        
        frontier = next_frontier

    return result

def get_description(concept_id: str):

    if CACHE[concept_id]:

        return get_concept(concept_id)["descriptions"][0]["term"]

    return None

def find_main_disorder(disorders: set) -> str:

    if len(disorders) == 1:
        return disorders.pop()

    for code in disorders:

        if is_or_has_ancestor_in(code, viral_codes):
            return code

    suspects = disorders.copy()
    visited = set()
    frontier = set(disorders)

    for depth in range(20):

        next_frontier = frontier.copy()

        for cid in frontier:
            
            if cid in visited: 
                continue

            visited.add(cid)

            c = get_concept(cid)

            if not isinstance(c, dict):
                continue

            parents = get_parent_ids(c)
            
            if frontier.intersection(set(parents)) or suspects.intersection(set(parents)):

                intersect = frontier.intersection(set(parents))
                intersect.update(suspects.intersection(set(parents)))

                for disorder in list(suspects):

                    if is_or_has_ancestor_in(disorder, intersect, depth):

                        suspects.discard(disorder)

                        if len(suspects) == 1:
                            return suspects.pop()
            next_frontier.update(p for p in parents if p not in visited)

        frontier = next_frontier
            
    print("Returned minimum, initial set:" + str(disorders) + ", suspects: " + str(suspects))
    return suspects.pop()
    
def get_codes(data, id) -> set(str()):

    codes = set ()

    cols = data.loc[id, 'diagnosis1':'diagnosis8'].dropna()

    for col in cols:

        codes.add(str(int(col)))

    return codes

def build_main_diagnoses(data):

    data.set_index('stay_id', inplace = True)

    main_diagnoses = pd.DataFrame(index = data.index, columns = output_cols)

    dictionary = get_dictionary(dictionary_path)

    for id in data.index:

        codes = get_codes(data, id)
        
        disorders, symptoms = disorders_and_symptoms_split(codes, dictionary)

        main_diagnoses.loc[id, 'num_of_disorders'] = len(disorders)
        main_diagnoses.loc[id, 'num_of_findings'] = len(symptoms)

        if len(disorders) == 0:
            
            if len(symptoms) > 0:
                
                min_symptom = symptoms.pop()

                main_diagnoses.loc[id, 'main_diagnosis_code'] = min_symptom
                main_diagnoses.loc[id, 'main_diagnosis_type'] = 'finding'

        else: 

            main_diagnoses.loc[id, 'main_diagnosis_type'] = 'disorder'
            main_diagnoses.loc[id, 'main_diagnosis_code'] = find_main_disorder(disorders)

        if not pd.isna(main_diagnoses.loc[id, 'main_diagnosis_code']):
            main_diagnoses.loc[id, 'main_diagnosis_name'] = get_description(main_diagnoses.loc[id, 'main_diagnosis_code'])

    return main_diagnoses

def pack_dictionary(data, path: str):

    data.to_csv(path)

data = load_data(data_path, sql)
load_state()
main_diagnoses = build_main_diagnoses(data)
pack_dictionary(main_diagnoses, write_path)