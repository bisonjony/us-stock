from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

MODEL_PANEL_GLOB = ROOT / "data/clean_parquet/model_panel/**/*.parquet"
OUT_DIR = ROOT / "data/clean_parquet/model_panel_sample"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "model_panel_quick_sample_100mb.csv"
OUT_PARQUET = OUT_DIR / "model_panel_quick_sample_100mb.parquet"

TARGET_SIZE_MB = 100
RANDOM_SEED = 9527

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=2")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")


# ------------------------------------------------------------
# Step 1. Estimate average CSV row size from a small random sample.
# ------------------------------------------------------------

ESTIMATE_N = 10_000

estimate_df = con.execute(f"""
    SELECT *
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    USING SAMPLE {ESTIMATE_N} ROWS
""").df()

csv_bytes = len(estimate_df.to_csv(index=False).encode("utf-8"))
avg_bytes_per_row = csv_bytes / max(len(estimate_df), 1)

target_bytes = TARGET_SIZE_MB * 1024 * 1024
target_n_rows = int(target_bytes / avg_bytes_per_row)

print(f"Estimated average CSV bytes per row: {avg_bytes_per_row:,.1f}")
print(f"Target CSV size: {TARGET_SIZE_MB} MB")
print(f"Estimated target rows: {target_n_rows:,}")


# ------------------------------------------------------------
# Step 2. Create a deterministic random sample.
#
# ORDER BY RANDOM() is okay here because model_panel is only a few million
# rows. The seed makes the sample reproducible.
# ------------------------------------------------------------

con.execute(f"SELECT SETSEED({RANDOM_SEED / 10000})")

sample_df = con.execute(f"""
    SELECT *
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    ORDER BY RANDOM()
    LIMIT {target_n_rows}
""").df()

sample_df.to_csv(OUT_CSV, index=False)
sample_df.to_parquet(OUT_PARQUET, index=False)

actual_size_mb = OUT_CSV.stat().st_size / 1024 / 1024

print(f"\nSaved CSV sample to: {OUT_CSV}")
print(f"Saved Parquet sample to: {OUT_PARQUET}")
print(f"Actual CSV size: {actual_size_mb:.2f} MB")
print(f"Number of rows: {len(sample_df):,}")


# ------------------------------------------------------------
# Step 3. Save simple diagnostics.
# ------------------------------------------------------------

summary = pd.DataFrame({
    "target_size_mb": [TARGET_SIZE_MB],
    "actual_size_mb": [actual_size_mb],
    "n_rows": [len(sample_df)],
    "avg_bytes_per_row_estimate": [avg_bytes_per_row],
    "min_date": [sample_df["dlycaldt"].min()],
    "max_date": [sample_df["dlycaldt"].max()],
    "n_unique_dates": [sample_df["dlycaldt"].nunique()],
    "n_unique_permnos": [sample_df["permno"].nunique()],
})

summary_path = OUT_DIR / "model_panel_quick_sample_summary.csv"
summary.to_csv(summary_path, index=False)

print(f"Saved summary to: {summary_path}")
print(summary.to_string(index=False))