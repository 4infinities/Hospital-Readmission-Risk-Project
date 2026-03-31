"""Walk-forward only — assumes base tables already in BQ."""
import sys, json
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from pipeline.bq_loader import BigQueryLoader
from pipeline.walk_forward import WalkForwardOrchestrator
from pipeline.preprocessing import DataPreprocessor
from pipeline.model_registry import ModelRegistry
from pipeline.hyperparameter_tuner import HyperparameterTuner
from pipeline.evaluator import Evaluator
from pipeline.cost_reducer import CostReducer
from pipeline.synthea_segmenter import SyntheaSegmenter

config_path_bq    = str(project_root / "config" / "bigquery_config.json")
recipe_path       = str(project_root / "config" / "bigquery_recipes.json")
io_config_path    = str(project_root / "config" / "dictionary_io_config.json")
watermark_path    = str(project_root / "config" / "watermark.json")
model_config_path = str(project_root / "config" / "model_config.json")
cost_config_path  = str(project_root / "config" / "cost_config.json")

segmenter = SyntheaSegmenter(config_path_bq, "mock")
segmenter.derive_window_from_existing()

transformer, _ = BigQueryTransformer.from_profile(config_path_bq)
builder = DictionaryBuilder(transformer=transformer, io_config_path=io_config_path)
bq_loader, _ = BigQueryLoader.from_profile(config_path_bq, "mock")

preprocessor = DataPreprocessor.from_config(model_config_path)
registry     = ModelRegistry.from_config(model_config_path)
cfg_mgr      = registry.config_mgr
tuner        = None  # best_params already in model_config.json from prior tuning run
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
print(f"Running fit_and_evaluate for {base_cutoff_date}...")
orch.fit_and_evaluate(base_cutoff_date)
print("fit_and_evaluate done.")

with open(watermark_path) as f:
    wm = json.load(f)
print(f"Bootstrapping prior-month staging for {wm['next_end_date']}...")
orch.bootstrap_prior_month_staging(wm["next_end_date"])
print("Bootstrap done.")

for i in range(2):
    print(f"Running month {i+1}...")
    processed = orch.run_next_month()
    print(f"Month {i+1} complete: {processed}")

print("Done.")
