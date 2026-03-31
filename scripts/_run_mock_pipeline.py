"""
Mock pipeline runner — equivalent to notebook cells 0, 3-10, 12-13.
Skips archive (cell 1) and Synthea run (cell 2).
Runs base load → slim → dicts → helpers → index_stay → base predictions → 2 months.
"""
import sys
import json
import calendar
from datetime import date
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pipeline.synthea_segmenter import SyntheaSegmenter
from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from pipeline.walk_forward import WalkForwardOrchestrator
from pipeline.preprocessing import DataPreprocessor
from pipeline.model_config_manager import ModelConfigManager
from pipeline.model_registry import ModelRegistry
from pipeline.hyperparameter_tuner import HyperparameterTuner
from pipeline.evaluator import Evaluator
from pipeline.cost_reducer import CostReducer

config_path_bq    = str(project_root / "config" / "bigquery_config.json")
recipe_path       = str(project_root / "config" / "bigquery_recipes.json")
io_config_path    = str(project_root / "config" / "dictionary_io_config.json")
watermark_path    = str(project_root / "config" / "watermark.json")
model_config_path = str(project_root / "config" / "model_config.json")
cost_config_path  = str(project_root / "config" / "cost_config.json")
checks_dir        = str(project_root / "sql" / "checks")

print(f"project_root: {project_root}")

# ── Cell 3: SyntheaSegmenter ──────────────────────────────────────────────────
print("\n=== Cell 3: SyntheaSegmenter ===")
RESEGMENT = False
segmenter = SyntheaSegmenter(config_path_bq, "mock")
if RESEGMENT:
    segmenter.segment(overwrite=True)
else:
    segmenter.derive_window_from_existing()
segmenter.write_watermark(watermark_path)
print(f"simulation_start    : {segmenter.simulation_start}")
print(f"base_cutoff_date    : {segmenter.base_cutoff_date}")
print(f"simulation_end_date : {segmenter.simulation_end_date}")

# ── Cell 4: Load base segment CSVs ───────────────────────────────────────────
print("\n=== Cell 4: Load base segment CSVs ===")
bq_loader, profile_cfg = BigQueryLoader.from_profile(config_path_bq, "mock")
bq_loader.load_base_segment()
print("Base segment CSVs loaded to BQ.")

# ── Cell 5: Slim tables + build dictionaries ──────────────────────────────────
print("\n=== Cell 5: Slim tables + build dictionaries ===")
base_cutoff_iso = segmenter.base_cutoff_date.isoformat()
print(f"base_cutoff_iso : {base_cutoff_iso}")

transformer, _ = BigQueryTransformer.from_profile(config_path_bq)
transformer.run_query_sequence(recipe_path, 0, str(project_root), end_date=base_cutoff_iso)
print("Slim tables created.")

builder = DictionaryBuilder(transformer=transformer, io_config_path=io_config_path)
builder.build_diagnoses_dictionary(end_date=base_cutoff_iso)
builder.build_procedures_dictionary(end_date=base_cutoff_iso)
builder.build_main_diagnoses(end_date=base_cutoff_iso)
print("Dictionaries built locally.")

# ── Cell 6: Load dictionaries to BQ ──────────────────────────────────────────
print("\n=== Cell 6: Load dictionaries to BQ ===")
bq_loader.load_dictionaries()
print("Dictionaries loaded to BQ.")

# ── Cell 7: Careplans ─────────────────────────────────────────────────────────
print("\n=== Cell 7: Careplans ===")
builder.build_careplans_related_diagnoses()
bq_loader.load_careplans()
print("Careplans loaded to BQ.")

# ── Cell 8: Helper tables + sanity checks ─────────────────────────────────────
print("\n=== Cell 8: Helper tables + sanity checks ===")
transformer.run_query_sequence(recipe_path, 1, str(project_root))
print("Helper tables created.")
transformer.run_helper_clinical_sanity_checks(checks_dir)
transformer.run_helper_cost_sanity_checks(checks_dir)
transformer.run_helper_utilization_sanity_checks(checks_dir)
print("All sanity checks passed.")

# ── Cell 9: Related diagnoses ─────────────────────────────────────────────────
print("\n=== Cell 9: Related diagnoses ===")
builder.build_related_diagnoses()
bq_loader.load_related_diagnoses()
print("related_diagnoses loaded to BQ.")

# ── Cell 10: Index stay ───────────────────────────────────────────────────────
print("\n=== Cell 10: Index stay ===")
transformer.run_query_sequence(recipe_path, 2, str(project_root))
print("index_stay created.")

# ── Cell 12: Walk-forward — base predictions + 2 months ──────────────────────
print("\n=== Cell 12: Walk-forward ===")
preprocessor = DataPreprocessor.from_config(model_config_path)
registry     = ModelRegistry.from_config(model_config_path)
cfg_mgr      = registry.config_mgr
tuner        = HyperparameterTuner(config_mgr=cfg_mgr, target_col="readmit_30d", cost_config_path=cost_config_path)
evaluator    = Evaluator(registry=registry, cfg_mgr=cfg_mgr)
cost_reducer = CostReducer.from_config(cost_config_path)

orch = WalkForwardOrchestrator(
    transformer=transformer,
    dict_builder=builder,
    loader=bq_loader,
    recipe_path=recipe_path,
    project_root=str(project_root),
    watermark_path=watermark_path,
    preprocessor=preprocessor,
    registry=registry,
    tuner=tuner,
    evaluator=evaluator,
    cost_reducer=cost_reducer,
    index_stay_sql_path=str(project_root / "sql" / "20_index_stay_selection.sql"),
)

base_cutoff_date = segmenter.base_cutoff_date.isoformat()
orch.fit_and_evaluate(base_cutoff_date)
print(f"Base predictions built for {base_cutoff_date}")

with open(watermark_path) as _f:
    _wm = json.load(_f)
orch.bootstrap_prior_month_staging(_wm["next_end_date"])

for i in range(2):
    processed = orch.run_next_month()
    print(f"Month {i+1} complete: {processed}")

print("2-month simulation complete.")

# ── Cell 13: Final status check ───────────────────────────────────────────────
print("\n=== Cell 13: Final status ===")
with open(watermark_path, encoding="utf-8") as f:
    wm = json.load(f)
print(f"last_processed_date : {wm['last_processed_date']}")
print(f"next_end_date       : {wm['next_end_date']}")
print(f"simulation_end_date : {wm['simulation_end_date']}")
