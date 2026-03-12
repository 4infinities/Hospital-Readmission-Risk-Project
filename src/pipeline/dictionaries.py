"""
SNOMED dictionary utilities and diagnosis–careplan relations.

Responsibilities:
- Load/save SNOMED lookup state (cache, flags, ancestors).
- Fetch and cache SNOMED concepts via the Snowstorm API.
- Compute ancestry relationships and main diagnosis codes.
- Build per-stay relation flags between diagnoses and care plans.
"""

import time
import requests
import json
from pathlib import Path
import pandas as pd

from pipeline.dictionary_config import (
    concept_path,
    viral_codes,
    basic_set,
    BASE,
    EDITION,
    RELEASE,
    MAX_RETRIES,
    BACKOFF_SECONDS,
    REQUEST_COUNT,
    CACHE,
    RESULTS,
    ANCESTORS,
)

session = requests.Session()

def load_state(state_path: Path):
    """
    Load cached concepts, results and ancestors from disk into globals.

    Returns:
        None (updates CACHE, RESULTS, ANCESTORS in place).
    """
    global CACHE, RESULTS, ANCESTORS

    if not state_path.exists():
        return

    with state_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    raw_cache = data.get("cache", {})
    CACHE = {code: path for code, path in raw_cache.items()}

    raw_results = data.get("results", {})
    RESULTS = {
        code: {flag: bool(val) for flag, val in flags.items()}
        for code, flags in raw_results.items()
    }

    raw_ancestors = data.get("ancestors", {})
    ANCESTORS = {code: set(ancestor_list) for code, ancestor_list in raw_ancestors.items()}


def save_state(state_path: Path):
    """
    Persist CACHE, RESULTS and ANCESTORS globals to disk.

    Returns:
        None.
    """
    data = {
        "cache": CACHE,
        "results": RESULTS,
        "ancestors": {
            code: list(ancestor_set) for code, ancestor_set in ANCESTORS.items()
        },
    }

    with state_path.open("w", encoding="utf-8") as f:
        json.dump(data, f)


def update_cache(state_path: Path):
    """
    Drop cache entries pointing to missing files and save updated state.

    Returns:
        None (updates CACHE in place and saves state).
    """
    global CACHE

    NEW_CACHE: dict[str, str] = {}
    for key in CACHE.items():
        if Path(key[1]).exists():
            NEW_CACHE[key[0]] = key[1]

    CACHE = NEW_CACHE
    save_state(state_path)


def get_dictionary(path: str) -> pd.DataFrame:
    """
    Load local code dictionary CSV and index by `code`.

    Returns:
        DataFrame indexed by code.
    """
    dictionary = pd.read_csv(path)
    dictionary.set_index("code", inplace=True)
    return dictionary


def disorders_and_symptoms_split(codes: set, dictionary: pd.DataFrame):
    """
    Split a set of codes into disorder vs symptom codes.

    Returns:
        (disorders, symptoms) as two sets of code strings.
    """
    disorders = set()
    symptoms = set()

    for code in codes:
        if int(code) in dictionary.index:
            if dictionary.loc[int(code), "is_disorder"] == 1:
                disorders.add(code)
            if dictionary.loc[int(code), "is_symptom"] == 1:
                symptoms.add(code)

    return disorders, symptoms


def save_concept(
    concept_id: str,
    data: dict,
    state_path: Path | None = None,
    save_path: str = concept_path,
):
    """
    Write concept JSON to disk, update CACHE, occasionally persist state.

    Returns:
        None.
    """
    global REQUEST_COUNT

    path = Path(save_path)
    path.mkdir(parents=True, exist_ok=True)

    file_name = f"{concept_id}.json"
    file_path = path / file_name

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    CACHE[concept_id] = str(file_path)
    REQUEST_COUNT += 1

    if REQUEST_COUNT % 10 == 0 and state_path is not None:
        save_state(state_path)


def read_concept(concept_path: str) -> dict:
    """
    Read a cached concept JSON file; return {} if missing.

    Returns:
        Concept JSON as dict, or {}.
    """
    path = Path(concept_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_concept(concept_id: str, targets: dict = {}, state_path: Path | None = None) -> dict:
    """
    Fetch a concept JSON with retries and caching.

    Returns:
        Concept JSON as dict, or {} for non-recoverable errors.
    """
    if concept_id in CACHE:
        return read_concept(CACHE[concept_id])

    url = f"{BASE}/{EDITION}/{RELEASE}/concepts/{concept_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url)

        if resp.status_code == 429:
            time.sleep(BACKOFF_SECONDS * attempt)
            continue

        if resp.status_code in (404, 410, 500, 502, 503, 504):
            save_concept(concept_id, {})
            for flag in targets:
                RESULTS[flag][concept_id] = False
            return {}

        resp.raise_for_status()
        data = resp.json()
        save_concept(concept_id, data, state_path=state_path)
        return data

    resp.raise_for_status()


def get_parent_ids(concept: dict) -> list[str]:
    """
    Return parent conceptIds from SNOMED 'is a' relationships.

    Returns:
        List of parent conceptIds as strings.
    """
    parents = []
    for rel in concept.get("relationships", []):
        if rel.get("type", {}).get("conceptId") == "116680003" and rel.get("active"):
            target = rel.get("target") or {}
            cid = target.get("conceptId")
            if cid:
                parents.append(cid)
    return parents

def find_least_children(concepts: set[str], state_path: Path) -> str:
    """
    Pick concept with the smallest inferredDescendants count.

    Returns:
        Concept id (string) from `concepts`.
    """
    concepts_copy = concepts.copy()

    for cid in concepts:
        c = get_concept(cid, state_path=state_path)
        if not isinstance(c, dict) or c == {}:
            concepts_copy.discard(cid)
    
    return min(
        concepts_copy,
        key=lambda cid: get_concept(cid, state_path=state_path)["inferredDescendants"],
    )


def is_or_has_ancestor_in(
    concept_id: str,
    targets: dict,
    target_ids: set[str],
    flag: str,
    state_path: Path | None = None,
    max_depth: int = 10,
) -> bool:
    """
    Check if concept is or has an ancestor in `target_ids`.

    Returns:
        True if concept_id or any ancestor is in target_ids, else False.
    """
    if len(target_ids) == 0:
        return False

    if flag not in RESULTS:
        RESULTS[flag] = {}

    if concept_id in RESULTS[flag]:
        return RESULTS[flag][concept_id]

    visited = set()
    frontier = {concept_id}

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
    return False


def find_main_disorder(disorders: set, state_path: Path) -> str:
    """
    Choose a main disorder code from a set.

    Prefers viral codes, then more specific codes in the SNOMED tree.

    Returns:
        Single disorder code as string.
    """
    if len(disorders) == 1:
        return disorders.pop()

    for code in disorders:
        if is_or_has_ancestor_in(
            code,
            targets={},
            target_ids=viral_codes,
            flag="viral",
            state_path=state_path,
        ):
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
            c = get_concept(cid, state_path=state_path)

            if not isinstance(c, dict):
                continue

            parents = get_parent_ids(c)

            if frontier.intersection(parents) or suspects.intersection(parents):
                intersect = frontier.intersection(parents)
                intersect.update(suspects.intersection(parents))

                for disorder in list(suspects):
                    if is_or_has_ancestor_in(
                        disorder,
                        targets={},
                        target_ids=intersect,
                        flag="",
                        state_path=state_path,
                        max_depth=depth,
                    ):
                        suspects.discard(disorder)

                if len(suspects) == 1:
                    return suspects.pop()

            next_frontier.update(p for p in parents if p not in visited)

        frontier = next_frontier
 
    return find_least_children(disorders, state_path)


def find_all_ancestors(
    concept_id: str,
    basic_set: set,
    targets: dict = {},
    state_path: Path | None = None,
) -> set[str]:
    """
    Compute and cache all non-basic ancestors of a concept.

    Returns:
        Set of ancestor conceptIds (strings).
    """
    global REQUEST_COUNT

    if concept_id in ANCESTORS:
        return ANCESTORS[concept_id]

    ancestors: set[str] = set()
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


def get_codes(data: pd.DataFrame) -> set[str]:
    """
    Flatten diagnosis columns into a unique set of codes.

    Returns:
        Set of code strings.
    """
    raw_codes = pd.Series(data.values.ravel()).dropna()
    string_codes = raw_codes.astype(int).astype(str)
    codes = set(string_codes.unique())
    return codes


def build_main_diagnoses(
    data: pd.DataFrame,
    output_cols: list[str],
    dictionary_path: str,
    state_path: Path | None = None,
) -> pd.DataFrame:
    """
    Build main diagnosis per group_id (stay) using SNOMED hierarchy.

    Returns:
        DataFrame indexed by stay id with:
        - num_of_disorders, num_of_findings
        - main_diagnosis_code, main_diagnosis_type, main_diagnosis_name
    """
    data.set_index(["group_id", "id"], inplace=True)

    main_diagnoses = pd.DataFrame(columns=output_cols)
    dictionary = get_dictionary(dictionary_path)

    rows = []

    for gid, group_df in data.groupby("group_id"):
        codes = get_codes(group_df)
        disorders, symptoms = disorders_and_symptoms_split(codes, dictionary)

        disorder_count = len(disorders)
        symptom_count = len(symptoms)

        code = ""
        dtype = ""
        name = ""

        if len(disorders) == 0:
            if len(symptoms) > 0:
                code = find_main_disorder(symptoms, state_path)
                dtype = "finding"
        else:
            dtype = "disorder"
            code = find_main_disorder(disorders, state_path)

        if code != "":
            name = get_description(code)

        row = {
            "id": gid,
            "num_of_disorders": disorder_count,
            "num_of_findings": symptom_count,
            "main_diagnosis_code": code,
            "main_diagnosis_type": dtype,
            "main_diagnosis_name": name,
        }
        rows.append(row)

    main_diagnoses = pd.DataFrame(rows).set_index("id")
    return main_diagnoses


def build_dictionary(data: pd.DataFrame, targets: dict, state_path: Path | None = None):
    """
    Populate RESULTS flags by walking ancestors for each code.

    Returns:
        None (RESULTS updated globally).
    """
    for id_ in data["code"]:
        for flag in targets:
            _ = is_or_has_ancestor_in(str(id_), targets, targets[flag], flag, state_path)


def build_flags(data: pd.DataFrame, targets: dict):
    """
    Attach boolean flag columns from RESULTS to the dictionary dataframe.

    Returns:
        None (data mutated in place).
    """
    data.set_index("code", inplace=True)

    for flag in targets:
        col = pd.Series(RESULTS[flag], name=flag)
        col.index = col.index.astype(int)
        data[flag] = col

        if flag == "is_procedure":
            data[flag] = ~data[flag]

    cols = [col for col in data.columns if not col.startswith("name")]
    data[cols] = data[cols].map(lambda x: int(x))


def get_relation(
    concept_id1,
    concept_id2,
    basic_set: set,
    state_path: Path | None = None,
) -> int:
    """
    Check if concept_id2 lies under concept_id1 in SNOMED.

    Returns:
        1  if concept_id2 is concept_id1 or its descendant,
        0  if any id is NaN,
       -1 otherwise.
    """
    if pd.isna(concept_id1) or pd.isna(concept_id2):
        return 0

    ancestors1 = find_all_ancestors(
        str(int(concept_id1)),
        basic_set,
        state_path=state_path,
    )

    if is_or_has_ancestor_in(
        str(int(concept_id2)),
        targets={},
        target_ids=ancestors1,
        flag="",
        state_path=state_path,
    ):
        return 1

    return -1


def build_relations(data: pd.DataFrame, state_path: Path) -> pd.DataFrame:
    """
    Compute diagnosis–careplan relation per stay_id.

    Returns:
        DataFrame indexed by stay_id with:
        - is_related: 1 if secondary code lies under primary code in SNOMED,
          0 or -1 otherwise.
    """
    data.set_index("stay_id", inplace=True)

    rows = []
    for row in data.itertuples():
        is_related = get_relation(row.code, row.sec_code, basic_set, state_path)
        rows.append({"stay_id": row.Index, "is_related": is_related})

    relations = pd.DataFrame(rows).set_index("stay_id")
    return relations


def build_diagnoses_related(data: pd.DataFrame, state_path: Path) -> pd.DataFrame:
    """
    Enrich relations with readmission flags and related-readmission labels.

    Returns:
        DataFrame indexed by stay_id with:
        - is_related, readmit_30d, readmit_90d
        - rel_readmit_30d, rel_readmit_90d
    """
    relations = build_relations(data, state_path)

    flag_cols = ["readmit_30d", "readmit_90d"]
    relations[flag_cols] = data[flag_cols]

    relations["rel_readmit_30d"] = (
        relations["readmit_30d"] * relations["is_related"]
    ).clip(lower=0)
    relations["rel_readmit_90d"] = (
        relations["readmit_90d"] * relations["is_related"]
    ).clip(lower=0)

    return relations


def build_careplan_relations(data: pd.DataFrame, state_path: Path) -> pd.DataFrame:
    """
    Aggregate careplan relation per stay.

    For each stay_id, checks whether any diagnosis in the stay belongs to the
    same SNOMED branch as the care plan (reason for the care plan). If at
    least one diagnosis is related, the stay is marked as related.

    Returns:
        DataFrame indexed by stay_id with:
        - is_related: 1 if this stay’s diagnoses are related to the care plan
          indication, else 0 or -1.
    """
    relations = build_relations(data, state_path)
    return relations.groupby(level="stay_id")["is_related"].max().to_frame("is_related")


def get_description(concept_id: str, targets: dict = {}):
    """
    Return first preferred term for a concept, or None.

    Returns:
        String term or None.
    """
    c = get_concept(concept_id, targets=targets)
    if isinstance(c, dict) and c != {}:
        return c["descriptions"][0]["term"]
    return None


def fill_descriptions(data: pd.DataFrame, targets: dict):
    """
    Back-fill missing `name` column using SNOMED descriptions.

    Returns:
        None (data mutated in place).
    """
    description_na_mask = data["name"].isna()
    for id_ in data.loc[description_na_mask, :].index:
        data.loc[id_, "name"] = get_description(str(id_), targets)


def fix_disorders(data: pd.DataFrame):
    """
    Ensure disorders are not also marked as symptoms.

    Returns:
        None (data mutated in place).
    """
    disorder_mask = data["is_disorder"] == 1
    data.loc[disorder_mask, "is_symptom"] = 0


def fix_chronic(data: pd.DataFrame):
    """
    Set is_chronic if any chronic flag (is_dementia..is_lf) is 1.

    Returns:
        None (data mutated in place).
    """
    chronic_mask = data.loc[:, "is_dementia":"is_lf"].sum(axis=1) > 0
    data.loc[chronic_mask, "is_chronic"] = 1


def fix_flags(data: pd.DataFrame):
    """
    Apply consistency fixes to disorder and chronic flags.

    Returns:
        None (data mutated in place).
    """
    fix_disorders(data)
    fix_chronic(data)


def pack_dictionary(data: pd.DataFrame, path: str):
    """
    Write dictionary dataframe to CSV.

    Returns:
        None.
    """
    data.to_csv(path)
