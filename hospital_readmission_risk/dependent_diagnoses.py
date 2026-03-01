import json
from pathlib import Path
import pandas as pd
from data import load_data

data_path = r"D:\Python Projects\Hospital readmission risk\data\raw\diagnoses_and_following.csv"
dictionary_path = r"D:\Python Projects\Hospital readmission risk\data\processed\dictionaries\diagnoses_dictionary.csv"
STATE_PATH = Path("D:\Python Projects\Hospital readmission risk\data\intermediate\diagnosess_snomed_state.json")
concept_path = r"D:\Python Projects\Hospital readmission risk\data\concepts"
write_path = r"D:\Python Projects\Hospital readmission risk\data\processed\related_diagnoses.csv"
CACHE: dict[str, str] = {}
ANCESTORS: dict[str, set[str]] = {}
"""
any null diagnoses will be 0
"""
sql = """
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
"""
output_cols = ['is_related']
basic_set = {
  '138875005', #snomed concept
  '404684003', #clinical finding
  '64572001', #disease(disorder)
  '844005', #behaviour finding
  '362965005', #disorder of body system
  '417163006', #injury
  '76712006' #disorder of digestive organ
  '118228005', #functional finding
  '27624003', #chronic disease
  '105612003', #Injury of internal organ
  '302768007', #Employment finding
  '384821006', #mental state bs
  '365526009', #job details finding
  '34014006', #viral disease, could be different viruses
  '128139000', # Inflammatory disorder
  '22253000', #Pain
   '118228005', #Finding by function
   '302292003', #finding of trunk structure
   '363171009', #Inflammation of specific body systems
  }

def load_state():

    global CACHE

    if not STATE_PATH.exists():
        return

    with STATE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    raw_cache = data.get("cache", {})
    CACHE = {code: path for code, path in raw_cache.items()}

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

    if concept_id in target_ids:
        return 1, {concept_id}

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
                return 1, target_ids.intersection(parents)
            
            next_frontier.update(p for p in parents if p not in visited)

        if not next_frontier:
            break
        
        frontier = next_frontier

    return -1, {}

def find_all_ancestors(concept_id: str, basic_set: set):

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
            
            c = get_concept(cid)

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
            
    return ancestors

def get_relation(concept_id1, concept_id2, basic_set: set):

    if pd.isna(concept_id1) or pd.isna(concept_id2):

        return 0, {}

    ancestors1 = find_all_ancestors(str(int(concept_id1)), basic_set)
    
    return is_or_has_ancestor_in(str(int(concept_id2)), ancestors1)

def get_description(concept_id: str):

    if CACHE[concept_id]:

        return get_concept(concept_id)["descriptions"][0]["term"]

    return None

def pack_dictionary(data, path: str):

    data.to_csv(path)

def build_relations(data):

    data.set_index('stay_id', inplace = True)
    
    rows = []
    for row in data.itertuples():

        is_related, relation_list = get_relation(row.code, row.sec_code, basic_set)
        
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

if __name__ == '__main__':

    data = load_data(data_path, sql)
    load_state()
    relations = build_relations(data)
    pack_dictionary(relations, write_path)


