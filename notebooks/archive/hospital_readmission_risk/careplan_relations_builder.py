"""
Command-line entrypoint for building careplan relation tables.

Responsibilities:
- Load config for diagnosis–careplan relations (train / test).
- Load cached SNOMED state (for hierarchy lookups).
- Pull stays and associated care plans from BigQuery.
- Use SNOMED ancestry to check if each stay’s diagnosis is related
  to the care plan indication.
- Aggregate per stay and write the is_related flag to CSV.
"""

from dictionaries import (
    load_main_config,
    load_state,
    load_data,
    build_careplan_relations,
    pack_dictionary,
)

# Choose split: train / test.
STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path = load_main_config(
    dict_type="careplans_related_diagnoses",
    source_type="train",
)
# STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path = load_main_config(
#     dict_type="careplans_related_diagnoses",
#     source_type="test",
# )

# Load data and SNOMED state.
data = load_data(data_path, sql)
load_state(STATE_PATH)

# Build careplan relation per stay and write to CSV.
relations = build_careplan_relations(data, STATE_PATH)
pack_dictionary(relations, write_path)
