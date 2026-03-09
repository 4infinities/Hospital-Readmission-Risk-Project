"""
Command-line entrypoint for building main-diagnosis tables.

Responsibilities:
- Load config for main diagnoses (train / test).
- Load cached SNOMED state (for hierarchy lookups).
- Pull per-stay diagnosis groups from BigQuery (with CSV cache).
- Select a main diagnosis per stay using SNOMED hierarchy rules.
- Write the resulting main_diagnoses table to CSV.
"""

from dictionaries import (
    load_main_config,
    load_state,
    load_data,
    build_main_diagnoses,
    pack_dictionary,
)

if __name__ == "__main__":
    # Choose split: train / test.
    STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path = (
        load_main_config(dict_type="main_diagnoses", source_type="train")
    )
    # STATE_PATH, output_cols, dictionary_path, data_path, sql, write_path = (
    #     load_main_config(dict_type="main_diagnoses", source_type="test")
    # )

    # Load data and SNOMED state.
    data = load_data(data_path, sql)
    load_state(STATE_PATH)

    # Build main diagnosis per stay and write to CSV.
    main_diagnoses = build_main_diagnoses(
        data,
        output_cols,
        dictionary_path,
        STATE_PATH,
    )
    pack_dictionary(main_diagnoses, write_path)
