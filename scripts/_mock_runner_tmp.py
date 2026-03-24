import sys, os
os.chdir(r"D:\Python Projects\Hospital readmission risk")
# ============================================================
# Cell 0: Setup & imports
import sys
import json
import shutil
import calendar
from datetime import date
from pathlib import Path

import pandas as pd

# Notebook lives at scripts/ — project root is one level up
project_root = Path.cwd().parent if Path.cwd().name == "scripts" else Path.cwd()
# pipeline modules use both `from pipeline.x import` AND `from src.utils.x import`
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pipeline.synthea_runner import SyntheaRunner
from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from pipeline.walk_forward import WalkForwardOrchestrator

# Shared config paths (used across all cells)
synthea_config_path = str(project_root / "config" / "synthea_config.json")
config_path_bq     = str(project_root / "config" / "bigquery_config.json")
recipe_path        = str(project_root / "config" / "bigquery_recipes.json")
io_config_path     = str(project_root / "config" / "dictionary_io_config.json")
watermark_path     = str(project_root / "config" / "watermark.json")
checks_dir         = str(project_root / "sql" / "checks")

print(f"project_root: {project_root}")

# ============================================================
# Cell 1: Archive existing local files before overwriting
today = date.today().strftime("%Y%m%d")
archive_root = project_root / "data" / "archive" / today

FILES_TO_ARCHIVE = [
    # Synthea raw CSVs
    "data/raw/Synthea/mock/patients.csv",
    "data/raw/Synthea/mock/encounters.csv",
    "data/raw/Synthea/mock/careplans.csv",
    "data/raw/Synthea/mock/claims.csv",
    "data/raw/Synthea/mock/conditions.csv",
    "data/raw/Synthea/mock/medications.csv",
    "data/raw/Synthea/mock/organizations.csv",
    "data/raw/Synthea/mock/procedures.csv",
    # BQ fetch caches (data_path in dictionary_io_config.json)
    "data/raw/dictionaries/unique_diagnoses.csv",
    "data/raw/dictionaries/unique_procedures.csv",
    "data/raw/diagnoses_per_stays.csv",
    "data/raw/diagnoses_and_careplans.csv",
    "data/raw/diagnoses_and_following.csv",
    # SNOMED classification state
    "data/intermediate/diagnosess_snomed_state.json",
    "data/intermediate/procedures_snomed_state.json",
    # Processed dictionaries (write_path in dictionary_io_config.json)
    "data/processed/dictionaries/diagnoses_dictionary.csv",
    "data/processed/dictionaries/procedures_dictionary.csv",
    # Watermark
    "config/watermark.json",
]

archived, skipped = [], []
for rel in FILES_TO_ARCHIVE:
    src = project_root / rel
    if src.exists():
        dst = archive_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        archived.append(rel)
    else:
        skipped.append(rel)

print(f"Archive path: {archive_root}")
print(f"Archived ({len(archived)}):")
for f in archived:
    print(f"  {f}")
if skipped:
    print(f"\nNot found / skipped ({len(skipped)}):")
    for f in skipped:
        print(f"  {f}")

# ============================================================
# Cell 2: Run Synthea (mock: 1000 patients, 3 years, seed=100)
runner, run_params = SyntheaRunner.from_profile(synthea_config_path, "mock")
runner.run(**run_params)
print("Synthea done.")

# ============================================================
# Cell 3: Load raw Synthea CSVs into BigQuery raw_data dataset
bq_loader, profile_cfg = BigQueryLoader.from_profile(config_path_bq, "mock")
bq_loader.load_profile_tables(profile_cfg)
print("Raw CSVs loaded to BQ.")

# ============================================================
# Cell 4: Create slim tables (recipe index 0)
transformer, _ = BigQueryTransformer.from_profile(config_path_bq)
transformer.run_query_sequence(recipe_path, 0, str(project_root))
print("Slim tables created.")

# ============================================================
# Cell 5: Build dictionaries locally via DictionaryBuilder
# New interface: no config_path_bq / profile_name args
builder = DictionaryBuilder(transformer=transformer, io_config_path=io_config_path)

builder.build_diagnoses_dictionary()
builder.build_procedures_dictionary()
builder.build_main_diagnoses()
print("Dictionaries built locally.")

# ============================================================
# Cell 6: Load dictionaries to BQ with plain table names (no mock_ prefix)
# Cannot use bq_loader.load_dictionaries() — it filters by profile_name in filename
# and adds mock_ prefix. The walk-forward update_* methods append to plain names
# (e.g. helper_tables.diagnoses_dictionary), so creation must match.
import json as _json
with open(config_path_bq) as f:
    _bq_cfg = _json.load(f)
helpers_dataset = _bq_cfg["dataset_helpers"]

helpers_loader = bq_loader.with_dataset(helpers_dataset)
helpers_loader.ensure_dataset_exists()

dict_dir = project_root / "data" / "processed" / "dictionaries"
dict_tables = {
    "diagnoses_dictionary.csv": "diagnoses_dictionary",
    "procedures_dictionary.csv": "procedures_dictionary",
    "main_diagnoses.csv": "main_diagnoses",
}

for filename, table_name in dict_tables.items():
    helpers_loader.load_one_csv(dict_dir / filename, table_name)

print("Dictionaries loaded to BQ.")

# ============================================================
# Cell 7: Build careplans_related_diagnoses locally + load to BQ
builder.build_careplans_related_diagnoses()

careplans_csv = project_root / "data" / "processed" / "careplans" / "careplans_related_encounters.csv"
helpers_loader.load_one_csv(careplans_csv, "careplans_related_encounters")

print("Careplans loaded to BQ.")

# ============================================================
# Cell 8: Create helper tables (recipe index 1) + run all 3 sanity checks
transformer.run_query_sequence(recipe_path, 1, str(project_root))
print("Helper tables created.")

transformer.run_helper_clinical_sanity_checks(checks_dir)
transformer.run_helper_cost_sanity_checks(checks_dir)
transformer.run_helper_utilization_sanity_checks(checks_dir)
print("All sanity checks passed.")

# ============================================================
# Cell 9: Build related_diagnoses locally + load to BQ
builder.build_related_diagnoses()

related_csv = project_root / "data" / "processed" / "related" / "related_diagnoses.csv"
helpers_loader.load_one_csv(related_csv, "related_diagnoses")

print("related_diagnoses loaded to BQ.")

# ============================================================
# Cell 10: Create index_stay (recipe index 2)
transformer.run_query_sequence(recipe_path, 2, str(project_root))
print("index_stay created.")

# ============================================================
# Cell 11: Compute watermark from MAX(stop) in encounters_slim minus 1 year
max_stop_sql = f"""
SELECT DATE(MAX(stop)) AS max_stop
FROM `{transformer.dataset_slim_fq}.encounters_slim`
"""
result_df = transformer.fetch_to_dataframe(sql=max_stop_sql, query=True)
max_stop_raw = result_df["max_stop"].iloc[0]

# Normalise to date object regardless of whether BQ returns date or datetime
if hasattr(max_stop_raw, "date"):
    max_stop = max_stop_raw.date()
else:
    max_stop = max_stop_raw

print(f"MAX(stop) from encounters_slim: {max_stop}")

# Subtract 1 year, last day of that same month
target_year  = max_stop.year - 1
target_month = max_stop.month
last_day     = calendar.monthrange(target_year, target_month)[1]
next_end_date = date(target_year, target_month, last_day).isoformat()

print(f"Computed next_end_date: {next_end_date}")

watermark = {"last_processed_date": None, "next_end_date": next_end_date}
with open(watermark_path, "w", encoding="utf-8") as f:
    json.dump(watermark, f, indent=4)

print(f"Watermark written: {watermark}")

# ============================================================
# Cell 12: Walk-forward — run one month
orch = WalkForwardOrchestrator(
    transformer=transformer,
    dict_builder=builder,
    recipe_path=recipe_path,
    project_root=str(project_root),
    watermark_path=watermark_path,
)

processed_end_date = orch.run_next_month()
print(f"\nWalk-forward month complete: {processed_end_date}")

# ============================================================
# Cell 13: Final status check
with open(watermark_path, encoding="utf-8") as f:
    wm = json.load(f)

print("=== Final watermark ===")
print(f"  last_processed_date : {wm['last_processed_date']}")
print(f"  next_end_date       : {wm['next_end_date']}")
print()
print(f"Processed month      : {wm['last_processed_date']}")
print(f"Next run would cover : window ending {wm['next_end_date']}")
