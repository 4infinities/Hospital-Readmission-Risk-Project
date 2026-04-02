"""
Full refactor pipeline — from data generation to per-month reports.

Phases
------
1  SyntheaRunner        : generate synthetic data (refactor profile)
2  SyntheaSegmenter     : segment into base + 60 monthly CSVs
3  BigQueryLoader       : load base CSVs to BQ
4  BigQueryTransformer  : slim tables (recipe 0)
5  DictionaryBuilder    : full dictionary build
   BigQueryLoader       : load dictionaries + careplans to BQ
   BigQueryTransformer  : helper tables (recipe 1) + sanity checks
   DictionaryBuilder    : build related_diagnoses
   BigQueryLoader       : load related_diagnoses to BQ
   BigQueryTransformer  : index_stay (recipe 2)
6  WalkForwardOrchestrator.fit_and_evaluate(base_cutoff_date)
       -> tune once, fit, predict, save PSI baseline
   bootstrap_prior_month_staging
7  orch.tuner = None    : disable all future retuning
   orch.run_until(simulation_end_date)
       -> per month: evaluate prior | refit (no retune) | predict | append to results/

Each phase is idempotent: re-running after a crash skips completed phases.
Skip logic:
  Phases 3+4  : patients_slim exists in BQ
  Phase  5a   : diagnoses_dictionary.csv + main_diagnoses.csv exist locally
  Phases 5b+5c: careplans_related_encounters table exists in BQ
  Phase  5d   : helper_utilization table exists in BQ
  Phases 5e+5f: index_stay table exists in BQ
  Phase  6    : psi_baseline.json exists (base fit completed)
"""
import sys
import json
import glob as _glob
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from google.cloud.exceptions import NotFound

from pipeline.synthea_runner import SyntheaRunner
from pipeline.synthea_segmenter import SyntheaSegmenter
from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from pipeline.walk_forward import WalkForwardOrchestrator
from pipeline.preprocessing import DataPreprocessor
from pipeline.model_registry import ModelRegistry
from pipeline.hyperparameter_tuner import HyperparameterTuner
from pipeline.evaluator import Evaluator
from pipeline.cost_reducer import CostReducer

PROFILE = "refactor"

config_path_bq      = str(project_root / "config" / "bigquery_config.json")
config_path_synthea = str(project_root / "config" / "synthea_config.json")
recipe_path         = str(project_root / "config" / "bigquery_recipes.json")
io_config_path      = str(project_root / "config" / "dictionary_io_config.json")
watermark_path      = str(project_root / "config" / "watermark.json")
model_config_path   = str(project_root / "config" / "model_config.json")
cost_config_path    = str(project_root / "config" / "cost_config.json")
checks_dir          = str(project_root / "sql" / "checks")

with open(config_path_bq) as _f:
    _bqcfg = json.load(_f)
_dictionaries_dir = Path(_bqcfg["dictionaries_dir"])
_careplans_dir    = Path(_bqcfg["careplans_dir"])
_related_dir      = Path(_bqcfg["related_dir"])

# ── Early init (needed for BQ existence checks) ───────────────────────────────
bq_loader,   _ = BigQueryLoader.from_profile(config_path_bq, PROFILE)
transformer, _ = BigQueryTransformer.from_profile(config_path_bq, PROFILE)

def _bq_exists(dataset: str, table: str) -> bool:
    try:
        transformer.client.get_table(f"{transformer.project_id}.{dataset}.{table}")
        return True
    except NotFound:
        return False

# ── Phases 1+2: Data generation ───────────────────────────────────────────────
segmenter = SyntheaSegmenter(config_path_bq, PROFILE)
_segmented_base = segmenter.segmented_path / "encounters_base.csv"

if _segmented_base.exists():
    print("\n=== Phase 1: SyntheaRunner (skipped — segmented files exist) ===")
    print("\n=== Phase 2: SyntheaSegmenter (skipped — segmented files exist) ===")
    segmenter.derive_window_from_existing()
    # Do NOT write watermark here — preserve last_processed_date/next_end_date from prior run
else:
    print("\n=== Phase 1: SyntheaRunner ===")
    runner, run_params = SyntheaRunner.from_profile(config_path_synthea, PROFILE)
    runner.run(**run_params)
    print("Synthea data generation complete.")

    print("\n=== Phase 2: SyntheaSegmenter ===")
    segmenter.segment(overwrite=True)
    segmenter.write_watermark(watermark_path)

print(f"simulation_start    : {segmenter.simulation_start}")
print(f"base_cutoff_date    : {segmenter.base_cutoff_date}")
print(f"simulation_end_date : {segmenter.simulation_end_date}")

base_cutoff_iso = segmenter.base_cutoff_date.isoformat()

# ── Phases 3+4: Base load + slim tables ───────────────────────────────────────
if _bq_exists(transformer.slim_dataset_id, "patients_slim"):
    print("\n=== Phase 3: Base load (skipped — slim tables exist) ===")
    print("\n=== Phase 4: Slim tables (skipped — slim tables exist) ===")
else:
    print("\n=== Phase 3: Base load to BQ ===")
    bq_loader.load_base_segment()
    print("Base segment CSVs loaded to BQ.")

    print("\n=== Phase 4: Slim tables ===")
    transformer.run_query_sequence(recipe_path, 0, str(project_root), end_date=base_cutoff_iso)
    print("Slim tables created.")

# ── Phase 5a: Dictionary build ────────────────────────────────────────────────
builder = DictionaryBuilder(transformer=transformer, io_config_path=io_config_path)

_dict_done = (
    (_dictionaries_dir / "diagnoses_dictionary.csv").exists() and
    (_dictionaries_dir / "main_diagnoses.csv").exists()
)
if _dict_done:
    print("\n=== Phase 5a: Dictionary build (skipped — local CSVs exist) ===")
else:
    print("\n=== Phase 5a: Dictionary build ===")
    builder.build_diagnoses_dictionary(end_date=base_cutoff_iso)
    builder.build_procedures_dictionary(end_date=base_cutoff_iso)
    builder.build_main_diagnoses(end_date=base_cutoff_iso)
    print("Dictionaries built locally.")

# ── Phases 5b+5c: Load dicts + careplans to BQ ───────────────────────────────
if _bq_exists(transformer.helpers_dataset_id, "careplans_related_encounters"):
    print("\n=== Phase 5b: Load dictionaries (skipped — BQ table exists) ===")
    print("\n=== Phase 5c: Careplans (skipped — BQ table exists) ===")
else:
    print("\n=== Phase 5b: Load dictionaries to BQ ===")
    bq_loader.load_dictionaries()
    print("Dictionaries loaded to BQ.")

    print("\n=== Phase 5c: Careplans ===")
    builder.build_careplans_related_diagnoses()
    bq_loader.load_careplans()
    print("Careplans loaded to BQ.")

# ── Phase 5d: Helper tables ───────────────────────────────────────────────────
if _bq_exists(transformer.helpers_dataset_id, "helper_utilization"):
    print("\n=== Phase 5d: Helper tables (skipped — BQ tables exist) ===")
else:
    print("\n=== Phase 5d: Helper tables + sanity checks ===")
    transformer.run_query_sequence(recipe_path, 1, str(project_root))
    print("Helper tables created.")
    transformer.run_helper_clinical_sanity_checks(checks_dir)
    transformer.run_helper_cost_sanity_checks(checks_dir)
    transformer.run_helper_utilization_sanity_checks(checks_dir)
    print("All sanity checks passed.")

# ── Phases 5e+5f: Related diagnoses + index stay ──────────────────────────────
if _bq_exists(transformer.helpers_dataset_id, "index_stay"):
    print("\n=== Phase 5e: Related diagnoses (skipped — index_stay exists) ===")
    print("\n=== Phase 5f: Index stay (skipped — index_stay exists) ===")
else:
    print("\n=== Phase 5e: Related diagnoses ===")
    builder.build_related_diagnoses()
    bq_loader.load_related_diagnoses()
    print("related_diagnoses loaded to BQ.")

    print("\n=== Phase 5f: Index stay ===")
    transformer.run_query_sequence(recipe_path, 2, str(project_root))
    print("index_stay created.")

# ── Phase 6: Base ML — tune once ──────────────────────────────────────────────
preprocessor = DataPreprocessor.from_config(model_config_path)
registry     = ModelRegistry.from_config(model_config_path)
cfg_mgr      = registry.config_mgr
tuner        = HyperparameterTuner(
    config_mgr=cfg_mgr,
    target_col="readmit_30d",
    cost_config_path=cost_config_path,
)
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
    predictions_dir=str(project_root / "predictions"),
    results_dir=str(project_root / "results"),
    index_stay_sql_path=str(project_root / "sql" / "20_index_stay_selection.sql"),
)

_psi_baseline = project_root / "predictions" / "psi_baseline.json"
if _psi_baseline.exists():
    print("\n=== Phase 6: Base ML (skipped — psi_baseline.json exists) ===")
else:
    print("\n=== Phase 6: Base ML (tune once) ===")
    # Clear any stale partial predictions/results before first fit
    for _p in _glob.glob(str(project_root / "predictions" / "*.csv")):
        Path(_p).unlink()
    for _p in _glob.glob(str(project_root / "results" / "*.csv")):
        Path(_p).unlink()

    orch.fit_and_evaluate(base_cutoff_iso)
    print(f"Base predictions built for {base_cutoff_iso}")

with open(watermark_path) as f:
    wm = json.load(f)
orch.bootstrap_prior_month_staging(wm["next_end_date"])
print(f"Prior-month staging bootstrapped for {wm['next_end_date']}")

# ── Phase 7: Walk-forward simulation — no retuning ───────────────────────────
print("\n=== Phase 7: Walk-forward (60 months, no retuning) ===")

orch.tuner = None
orch.run_until(wm["simulation_end_date"])

with open(watermark_path) as f:
    wm_final = json.load(f)
print(f"\nSimulation complete. last_processed_date={wm_final['last_processed_date']}")
print("Per-month reports written to results/{model}_results.csv")
