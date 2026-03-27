"""
Full pipeline run: base load → slim/helpers/index_stay → walk-forward x2.
Segmented files must already exist in data/raw/segmented/mock/.
Run from project root: .venv\Scripts\python.exe scripts/run_pipeline.py
"""
import json
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from pipeline.synthea_segmenter import SyntheaSegmenter
from pipeline.walk_forward import WalkForwardOrchestrator

CONFIG_BQ      = str(project_root / "config" / "bigquery_config.json")
RECIPE_PATH    = str(project_root / "config" / "bigquery_recipes.json")
IO_CONFIG      = str(project_root / "config" / "dictionary_io_config.json")
WATERMARK_PATH = str(project_root / "config" / "watermark.json")
CHECKS_DIR     = str(project_root / "sql" / "checks")


def step(msg: str) -> None:
    print(f"\n{'='*60}\n{msg}\n{'='*60}")


# ---------- init ----------
step("Init: BQ clients")
bq_loader, _  = BigQueryLoader.from_profile(CONFIG_BQ, "mock")
transformer, _ = BigQueryTransformer.from_profile(CONFIG_BQ)
builder        = DictionaryBuilder(transformer=transformer, io_config_path=IO_CONFIG)

# Derive simulation window from segmenter (reads encounters.csv, no file writes)
segmenter = SyntheaSegmenter.from_profile(CONFIG_BQ, "mock")
segmenter.segment(overwrite=False)
simulation_start = segmenter.simulation_start
base_cutoff_date = segmenter.base_cutoff_date
print(f"simulation_start : {simulation_start}")
print(f"base_cutoff_date : {base_cutoff_date}")

helpers_loader = bq_loader.with_dataset("helper_tables")
helpers_loader.ensure_dataset_exists()

# ---------- Phase 1: base load ----------
step("S1: Load base segment CSVs to BQ raw_data")
bq_loader.load_base_segment()

step("S2: Create slim tables (recipe 0)")
transformer.run_query_sequence(RECIPE_PATH, 0, str(project_root))

step("S3: Build diagnoses_dictionary locally")
builder.build_diagnoses_dictionary()

step("S4: Build procedures_dictionary locally")
builder.build_procedures_dictionary()

step("S5: Build main_diagnoses locally")
builder.build_main_diagnoses()

step("S6: Load dictionaries to BQ helper_tables")
dict_dir = project_root / "data" / "processed" / "dictionaries"
helpers_loader.load_one_csv(dict_dir / "diagnoses_dictionary.csv", "diagnoses_dictionary")
helpers_loader.load_one_csv(dict_dir / "procedures_dictionary.csv", "procedures_dictionary")
helpers_loader.load_one_csv(dict_dir / "main_diagnoses.csv",        "main_diagnoses")

step("S7: Build careplans_related_diagnoses locally")
builder.build_careplans_related_diagnoses()

step("S8: Load careplans_related_encounters to BQ")
careplans_csv = project_root / "data" / "processed" / "careplans" / "careplans_related_encounters.csv"
helpers_loader.load_one_csv(careplans_csv, "careplans_related_encounters")

step("S9: Create helper tables (recipe 1)")
transformer.run_query_sequence(RECIPE_PATH, 1, str(project_root))

step("S10: Build related_diagnoses locally")
builder.build_related_diagnoses()

step("S11: Load related_diagnoses to BQ")
related_csv = project_root / "data" / "processed" / "related" / "related_diagnoses.csv"
helpers_loader.load_one_csv(related_csv, "related_diagnoses")

step("S12: Create index_stay (recipe 2)")
transformer.run_query_sequence(RECIPE_PATH, 2, str(project_root))

step("S13: Initialize watermark")
orch = WalkForwardOrchestrator(
    transformer=transformer,
    dict_builder=builder,
    loader=bq_loader,
    recipe_path=RECIPE_PATH,
    project_root=str(project_root),
    watermark_path=WATERMARK_PATH,
)
orch.initialize_watermark(simulation_start, base_cutoff_date)
with open(WATERMARK_PATH) as f:
    wm = json.load(f)
print(f"Watermark: {wm}")

# ---------- Phase 2: walk-forward ----------
next_end_date = wm["next_end_date"]
step(f"S14: Bootstrap prior-month staging for {next_end_date}")
orch.bootstrap_prior_month_staging(next_end_date)

step("S15: Walk-forward month 1")
m1 = orch.run_next_month()
print(f"Month 1 done: {m1}")

step("S16: Walk-forward month 2")
m2 = orch.run_next_month()
print(f"Month 2 done: {m2}")

step("Done")
with open(WATERMARK_PATH) as f:
    wm = json.load(f)
print(f"Final watermark: {wm}")
