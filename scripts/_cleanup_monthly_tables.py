"""
One-shot BQ cleanup: drop all dated monthly tables in raw_data except the last two.

Targets both raw staging tables  (encounters_2023_11_30)
and monthly slim tables          (encounters_slim_2023_11_30)
in the raw_data dataset of the refactor project.

The last two months kept are derived from watermark.json:
  last_processed_date  → keep
  month before that    → keep
  everything older     → DELETE

Safe to run multiple times.
"""
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pipeline.bq_transformer import BigQueryTransformer

PROFILE = "refactor"
config_path_bq  = str(project_root / "config" / "bigquery_config.json")
watermark_path  = project_root / "config" / "watermark.json"

# ── Determine which two months to keep ───────────────────────────────────────
with open(watermark_path) as f:
    wm = json.load(f)

last_processed = date.fromisoformat(wm["last_processed_date"])   # e.g. 2023-11-30
prior          = (last_processed.replace(day=1) - timedelta(days=1))  # last day of prev month

keep = {last_processed.strftime("%Y_%m_%d"), prior.strftime("%Y_%m_%d")}
print(f"Keeping tables for: {sorted(keep)}")

# ── Connect to BQ ─────────────────────────────────────────────────────────────
transformer, _ = BigQueryTransformer.from_profile(config_path_bq, PROFILE)
raw_dataset_fq = f"{transformer.project_id}.{transformer.raw_dataset_id}"

print(f"Scanning dataset: {raw_dataset_fq}")

# ── List + delete ─────────────────────────────────────────────────────────────
_DATE_RE = re.compile(r"_(\d{4}_\d{2}_\d{2})$")

try:
    tables = list(transformer.client.list_tables(raw_dataset_fq))
except Exception as e:
    print(f"ERROR listing tables: {e}")
    sys.exit(1)

to_delete = []
for t in tables:
    m = _DATE_RE.search(t.table_id)
    if m and m.group(1) not in keep:
        to_delete.append(t.table_id)

if not to_delete:
    print("Nothing to delete — dataset is already clean.")
    sys.exit(0)

print(f"\nTables to delete ({len(to_delete)}):")
for name in sorted(to_delete):
    print(f"  {name}")

confirm = input(f"\nDelete {len(to_delete)} tables? [y/N] ").strip().lower()
if confirm != "y":
    print("Aborted.")
    sys.exit(0)

deleted = 0
failed  = 0
for name in to_delete:
    fq = f"{raw_dataset_fq}.{name}"
    try:
        transformer.client.delete_table(fq)
        deleted += 1
        print(f"  Deleted: {name}")
    except Exception as e:
        failed += 1
        print(f"  FAILED:  {name}  ({e})")

print(f"\nDone. Deleted {deleted}, failed {failed}.")
