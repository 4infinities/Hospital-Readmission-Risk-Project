"""
Command-line entrypoint for building SNOMED-based dictionaries.

Responsibilities:
- Load config for procedures or diagnoses (train / test).
- Load cached SNOMED state (concept JSON cache, flags, ancestors).
- Pull unique codes from BigQuery (with local CSV cache).
- Walk SNOMED hierarchy to populate flag RESULTS.
- Attach flags, fill missing descriptions, fix diagnosis flags.
- Save updated state and write final dictionary CSV.

Typical usage:
    # Diagnoses dictionary, train split
    python dictionary_builder.py

To switch:
    - Change `diagnoses` to False for procedures.
    - Swap `source_type` from "train" to "test" in load_config().
"""

from dictionaries import (
    load_state,
    load_data,
    build_dictionary,
    build_flags,
    fill_descriptions,
    fix_flags,
    pack_dictionary,
    save_state,
    load_config,
)

if __name__ == "__main__":
    # Set which dictionary to build.
    diagnoses = True

    # Choose config: diagnoses/procedures, train/test.
    STATE_PATH, targets, data_path, sql, write_path = load_config(
        dict_type="diagnoses",
        source_type="train",
    )
    # STATE_PATH, targets, data_path, sql, write_path = load_config(
    #     dict_type="diagnoses",
    #     source_type="test",
    # )
    # diagnoses = False
    # STATE_PATH, targets, data_path, sql, write_path = load_config(
    #     dict_type="procedures",
    #     source_type="train",
    # )
    # STATE_PATH, targets, data_path, sql, write_path = load_config(
    #     dict_type="procedures",
    #     source_type="test",
    # )

    # Load cached SNOMED state and source data.
    load_state(STATE_PATH)
    data = load_data(data_path, sql)

    # Populate RESULTS by walking SNOMED graph; always persist state.
    try:
        build_dictionary(data, targets, STATE_PATH)
    finally:
        save_state(STATE_PATH)

    # Attach flags, backfill names, and fix diagnosis-specific flags.
    build_flags(data, targets)
    fill_descriptions(data, targets)

    if diagnoses:
        fix_flags(data)

    # Write final dictionary CSV.
    pack_dictionary(data, write_path)
