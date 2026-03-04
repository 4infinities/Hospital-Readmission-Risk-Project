import time
import requests
import json
from pathlib import Path
import pandas as pd
from data import load_data
from dictionary_config import config_data, concept_path, viral_codes, basic_set, BASE, EDITION, RELEASE, MAX_RETRIES, BACKOFF_SECONDS, REQUEST_COUNT, CACHE, RESULTS, ANCESTORS

session = requests.Session()

def load_config(dict_type = 'procedures', source_type = 'train'):

    STATE_PATH = config_data[dict_type]['state']
    targets = config_data[dict_type]['targets']

    cfg = config_data[dict_type][source_type]

    data_path = cfg['data_path']
    sql = cfg['sql']
    write_path = cfg['write_path']

    return STATE_PATH, targets, data_path, sql, write_path

def load_main_config(dict_type = 'main_diagnoses', source_type = 'train'):

    STATE_PATH = config_data[dict_type]['state']
    output_cols = config_data[dict_type]['output_cols']

    cfg = config_data[dict_type][source_type]

    dictionary_path = cfg['dictionary_path']
    data_path = cfg['data_path']
    sql = cfg['sql']
    write_path = cfg['write_path']

    return STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path

def load_state(state_path: Path):

    global CACHE, RESULTS, ANCESTORS

    if not state_path.exists():

        return

    with state_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    raw_cache = data.get("cache", {})
    CACHE = {code: path for code, path in raw_cache.items()}
    
    raw_results = data.get("results", {})  # dict[str, dict]
    RESULTS = {
        code: {flag: bool(val) for flag, val in flags.items()}
        for code, flags in raw_results.items()
            }

    raw_ancestors = data.get("ancestors", {})
    ANCESTORS = {code: set(ancestor_list) for code, ancestor_list in raw_ancestors.items()}

def save_state(state_path: Path):

    data = {
        "cache": CACHE,
        "results": RESULTS,
        "ancestors": {
        code: list(ancestor_set)
        for code, ancestor_set in ANCESTORS.items()
                    }
    }
    
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(data, f)

def update_cache(state_path):

    NEW_CACHE: dict[str, str] = {}

    for key in CACHE.items():

        if Path(key[1]).exists():

            NEW_CACHE[key[0]] = key[1]

    CACHE = NEW_CACHE

    save_state(state_path)

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

def save_concept(concept_id, data, state_path = None, save_path = concept_path):

    global REQUEST_COUNT

    path = Path(save_path)
    path.mkdir(parents=True, exist_ok=True)

    file_name = f"{concept_id}.json"
    file_path = path/file_name

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    CACHE[concept_id] = str(file_path)

    REQUEST_COUNT += 1

    if REQUEST_COUNT % 10 == 0 and state_path is not None:
        save_state(state_path)

def read_concept(concept_path):

    path = Path(concept_path)

    if not path.exists():
        return {}
    
    with path.open("r", encoding="utf-8") as f:
        return json.load(f) 

def get_concept(concept_id: str, targets = {}, state_path = None) -> dict:
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

        save_concept(concept_id, data, state_path = state_path)
        
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

def find_least_children(concepts: set[str], state_path) -> str:

    concepts_copy = concepts.copy()

    for cid in concepts:

        if not isinstance(get_concept(cid, state_path = state_path), dict) or get_concept(cid, state_path = state_path) == {}:

            concepts_copy.discard(cid)
    
    return min(concepts_copy,key = lambda cid: get_concept(cid, state_path = state_path)["inferredDescendants"])

def is_or_has_ancestor_in(concept_id: str, targets, target_ids: set[str], flag: str, state_path = None, max_depth: int = 10) -> bool:
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

            c = get_concept(cid, targets, state_path)

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

def find_main_disorder(disorders: set, state_path) -> str:

    if len(disorders) == 1:
        return disorders.pop()

    for code in disorders:

        if is_or_has_ancestor_in(code, targets = {}, target_ids = viral_codes, flag = 'viral', state_path = state_path):
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

            c = get_concept(cid, state_path = state_path)

            if not isinstance(c, dict):
                continue

            parents = get_parent_ids(c)
            
            if frontier.intersection(set(parents)) or suspects.intersection(set(parents)):

                intersect = frontier.intersection(set(parents))
                intersect.update(suspects.intersection(set(parents)))

                for disorder in list(suspects):

                    if is_or_has_ancestor_in(disorder, 
                    targets = {}, 
                    target_ids = intersect, 
                    flag = '', 
                    state_path = state_path, 
                    max_depth = depth):

                        suspects.discard(disorder)

                        if len(suspects) == 1:
                            return suspects.pop()
            next_frontier.update(p for p in parents if p not in visited)

        frontier = next_frontier

    return find_least_children(suspects, state_path)

def find_all_ancestors(concept_id: str, basic_set: set, targets = {}, state_path = None):

    global REQUEST_COUNT

    if concept_id in ANCESTORS:
        return ANCESTORS[concept_id]

    ancestors : set[str] = set()
    frontier = {concept_id}
    visited = set()

    for depth in range(10):

        new_frontier = set()

        for cid in frontier:

            if cid in visited or cid in basic_set:
                continue
            
            c = get_concept(cid, targets, state_path)

            if c is {}:
                continue

            parents = get_parent_ids(c)

            for parent in parents:

                if parent in basic_set:
                    continue

                ancestors.add(parent)
                new_frontier.add(parent)
            
        if not new_frontier:
            break
        
        frontier = new_frontier

    ANCESTORS[concept_id] = ancestors

    REQUEST_COUNT += 1

    if REQUEST_COUNT % 50 == 0 and state_path is not None:
        save_state(state_path)
            
    return ancestors

def get_codes(data) -> set(str()):

    raw_codes = pd.Series(data.values.ravel()).dropna()

    string_codes = raw_codes.astype(int).astype(str)
    
    codes = set(string_codes.unique())

    return codes

def build_main_diagnoses(data, output_cols, dictionary_path, state_path = None):

    data.set_index(['group_id', 'id'], inplace = True)

    main_diagnoses = pd.DataFrame(columns = output_cols)

    dictionary = get_dictionary(dictionary_path)

    rows = []

    for gid, group_df in data.groupby('group_id'):

        codes = get_codes(group_df)
            
        disorders, symptoms = disorders_and_symptoms_split(codes, dictionary)

        disorder_count = len(disorders)
        symptom_count = len(symptoms)

        code = ""
        type = ""
        name = ""

        if len(disorders) == 0:
                
            if len(symptoms) > 0:

                code = find_main_disorder(symptoms, state_path)
                type = 'finding'

        else: 

            type = 'disorder'
            code = find_main_disorder(disorders, state_path)

        if not code == "":
            name = get_description(code)

        row = {
            'id': gid,
            'num_of_disorders': disorder_count,
            'num_of_findings': symptom_count,
            'main_diagnosis_code': code, 
            'main_diagnosis_type': type, 
            'main_diagnosis_name': name
        }

        rows.append(row)

    main_diagnoses = pd.DataFrame(rows).set_index('id')

    return main_diagnoses

def build_dictionary(data, targets, state_path = None):

    for id in data['code']:

        for flag in targets:

            b = is_or_has_ancestor_in(str(id), targets, targets[flag], flag, state_path)
        
def build_flags(data: pd.DataFrame, targets):

    data.set_index('code', inplace = True)

    for flag in targets:
        
        col = pd.Series(RESULTS[flag], name = flag)
        col.index = col.index.astype(int)
        data[flag] = col

        if flag == 'is_procedure':
                data[flag] = ~data[flag]

    cols = [col for col in data.columns if not col.startswith("name")]
    data[cols] = data[cols].map(lambda x: int(x))

def get_relation(concept_id1, concept_id2, basic_set: set, state_path = None):

    if pd.isna(concept_id1) or pd.isna(concept_id2):
        return 0

    ancestors1 = find_all_ancestors(str(int(concept_id1)), basic_set, state_path = state_path)

    if is_or_has_ancestor_in(str(int(concept_id2)), 
                            targets = {}, 
                            target_ids = ancestors1, 
                            flag = '', 
                            state_path = state_path):
        return 1

    return -1

def build_relations(data, state_path):

    data.set_index('stay_id', inplace = True)
    
    rows = []
    for row in data.itertuples():

        is_related = get_relation(row.code, row.sec_code, basic_set, state_path)
        
        rows.append({
            'stay_id': row.Index,
            'is_related' : is_related,
            #'relation_list' : str(relation_list)
        })

    relations = pd.DataFrame(rows).set_index('stay_id')

    flag_cols = ['readmit_30d', 'readmit_90d']

    relations[flag_cols] = data[flag_cols]

    relations['rel_readmit_30d'] = (relations['readmit_30d'] * relations['is_related']).clip(lower = 0)
    relations['rel_readmit_90d'] = (relations['readmit_90d'] * relations['is_related']).clip(lower = 0)

    return relations

def get_description(concept_id: str, targets = {}):

    if isinstance(get_concept(concept_id, targets = targets), dict) and not get_concept(concept_id, targets = targets) == {}:

        return get_concept(concept_id, targets)["descriptions"][0]["term"]

    return None

def fill_descriptions(data, targets):

    description_na_mask = data['name'].isna()

    for id in data.loc[description_na_mask, :].index:
         
        data.loc[id, 'name'] = get_description(str(id), targets)

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