"""
Command-line entrypoint for building related-diagnoses tables.

Responsibilities:
- Load config for diagnosis–diagnosis relations (train / test).
- Load cached SNOMED state (for hierarchy lookups).
- Pull pairs of stays and following stays from BigQuery.
- Use SNOMED ancestry to tag whether readmissions are related
  to the index stay’s main diagnosis.
- Write the resulting relations table to CSV.
"""

from dictionaries import (
    load_main_config,
    load_state,
    load_data,
    build_related_diagnoses,
    pack_dictionary,
)

if __name__ == "__main__":
    # Choose split: train / test.
    STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path = (
        load_main_config(dict_type="related_diagnoses", source_type="train")
    )
    # STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path = (
    #     load_main_config(dict_type="related_diagnoses", source_type="test")
    # )

    # Always hit BigQuery (query=True) to refresh relations.
    data = load_data(data_path, sql, query=True)
    load_state(STATE_PATH)

    # Build diagnosis–diagnosis relations per stay and write to CSV.
    relations = build_related_diagnoses(data, STATE_PATH)
    pack_dictionary(relations, write_path)
