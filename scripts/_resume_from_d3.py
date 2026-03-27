"""
Recovery script: resume S15 from D3 (main_diagnoses) onward.
S0, S0.5, D1, D2 already completed successfully for 2025-04-30.
Then runs full S16 (2025-05-31).
"""
import sys, os, json, calendar
from datetime import date
from pathlib import Path

os.chdir(r"D:\Python Projects\Hospital readmission risk")
project_root = Path.cwd()
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from pipeline.walk_forward import WalkForwardOrchestrator

config_path_bq = str(project_root / "config" / "bigquery_config.json")
recipe_path    = str(project_root / "config" / "bigquery_recipes.json")
io_config_path = str(project_root / "config" / "dictionary_io_config.json")
watermark_path = str(project_root / "config" / "watermark.json")

transformer, _ = BigQueryTransformer.from_profile(config_path_bq)
builder = DictionaryBuilder(transformer=transformer, io_config_path=io_config_path)
bq_loader, _ = BigQueryLoader.from_profile(config_path_bq, "mock")

END_DATE_APR = "2025-04-30"

print(f"\n=== S15 resume: D3–I1 for {END_DATE_APR} ===")

print("[D3] Updating main_diagnoses")
builder.update_main_diagnoses(END_DATE_APR)

print("[D4] Updating careplans_related_encounters")
builder.update_careplans_related_encounters(END_DATE_APR)

print("[H1-H5] Running helper table updates")
transformer.run_query_sequence(recipe_path, 3, str(project_root), END_DATE_APR)

print("[D5] Updating related_diagnoses")
builder.update_related_diagnoses(END_DATE_APR)

print("[I1] Running index_stay update")
transformer.run_query_sequence(recipe_path, 4, str(project_root), END_DATE_APR)

# Advance watermark to 2025-05-31
END_DATE_MAY = "2025-05-31"
watermark = {"last_processed_date": END_DATE_APR, "next_end_date": END_DATE_MAY}
with open(watermark_path, "w", encoding="utf-8") as f:
    json.dump(watermark, f, indent=4)
print(f"Watermark advanced: last_processed={END_DATE_APR}, next={END_DATE_MAY}")

print(f"\n=== S16: full run for {END_DATE_MAY} ===")
orch = WalkForwardOrchestrator(
    transformer=transformer,
    dict_builder=builder,
    loader=bq_loader,
    recipe_path=recipe_path,
    project_root=str(project_root),
    watermark_path=watermark_path,
)
processed = orch.run_next_month()
print(f"S16 complete: {processed}")

with open(watermark_path, encoding="utf-8") as f:
    wm = json.load(f)
print(f"\n=== Final watermark ===")
print(f"  last_processed_date : {wm['last_processed_date']}")
print(f"  next_end_date       : {wm['next_end_date']}")
