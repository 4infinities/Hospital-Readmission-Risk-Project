"""
Predict-only walk-forward runner.

Assumes:
  - Base tables already loaded to BQ
  - Bootstrap (prior-month staging) already done
  - Models already fitted and saved in models_dir

Per month: loads CSV segment → updates BQ tables (slim, helpers, index_stay) → predicts.
No evaluation, no retuning, no retraining.
"""
import sys
import json
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

config_path_bq    = str(project_root / "config" / "bigquery_config.json")
recipe_path       = str(project_root / "config" / "bigquery_recipes.json")
io_config_path    = str(project_root / "config" / "dictionary_io_config.json")
watermark_path    = str(project_root / "config" / "watermark.json")
model_config_path = str(project_root / "config" / "model_config.json")

transformer, _ = BigQueryTransformer.from_profile(config_path_bq)
builder        = DictionaryBuilder(transformer=transformer, io_config_path=io_config_path)
bq_loader, _   = BigQueryLoader.from_profile(config_path_bq, "mock")
preprocessor   = DataPreprocessor.from_config(model_config_path)
registry       = ModelRegistry.from_config(model_config_path)

orch = WalkForwardOrchestrator(
    transformer=transformer,
    dict_builder=builder,
    loader=bq_loader,
    recipe_path=recipe_path,
    project_root=str(project_root),
    watermark_path=watermark_path,
    preprocessor=preprocessor,
    registry=registry,
    predictions_dir=str(project_root / "predictions"),
    # evaluator / tuner / cost_reducer intentionally omitted — predict only
)


def _predict_only(end_date: str) -> None:
    """Preprocess then save predictions — no evaluate, no retrain."""
    orch.logger.info("[predict_only] Preprocessing for end_date=%s", end_date)
    _, _, X_test, stay_ids, _ = orch.preprocessor.preprocess(
        end_date=end_date,
        transformer=orch.transformer,
    )
    orch._save_predictions(end_date, X_test, stay_ids)
    orch.logger.info("[predict_only] Predictions saved for %s", end_date)


# Replace ML step with predict-only
orch.fit_and_evaluate = _predict_only

with open(watermark_path) as f:
    wm = json.load(f)

next_end     = wm["next_end_date"]
final_end    = wm["simulation_end_date"]

print(f"Predict-only walk-forward: {next_end} to {final_end}")
print("No evaluation, no retuning, no retraining.\n")

orch.run_until(final_end)

with open(watermark_path) as f:
    wm_final = json.load(f)
print(f"\nDone. last_processed_date={wm_final['last_processed_date']}")
