from pathlib import Path
import gc

import duckdb
import numpy as np
import pandas as pd


# ============================================================
# Paths
# ============================================================

ROOT = Path("/home/xul9527/us-stock")

MODEL_PANEL_GLOB = ROOT / "data/clean_parquet/model_panel/**/*.parquet"
FEATURE_COLS_CSV = ROOT / "data/clean_parquet/model_panel_analysis/model_panel_feature_columns.csv"

OUT_DIR = ROOT / "model_outputs/single_feature_ic_analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_DAILY_IC = OUT_DIR / "single_feature_daily_rank_ic.csv"
OUT_SUMMARY = OUT_DIR / "single_feature_rank_ic_summary.csv"
OUT_YEARLY_SUMMARY = OUT_DIR / "single_feature_rank_ic_summary_by_year.csv"
OUT_USED_FEATURES = OUT_DIR / "used_feature_columns.csv"


# ============================================================
# Settings
# ============================================================

TARGET_COL = "target_5d_cs_zscore"
DATE_COL = "dlycaldt"
YEAR_COL = "year"

MIN_OBS_PER_DATE = 50

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# DuckDB setup
# ============================================================

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=2")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")
con.execute("SET max_temp_directory_size='150GB'")


# ============================================================
# Helper functions
# ============================================================

def load_feature_columns() -> list[str]:
    feature_df = pd.read_csv(FEATURE_COLS_CSV)

    if "feature" not in feature_df.columns:
        raise ValueError(f"{FEATURE_COLS_CSV} must contain a column named 'feature'.")

    features = feature_df["feature"].dropna().astype(str).tolist()

    # Preserve order and remove duplicates.
    seen = set()
    unique_features = []
    for f in features:
        if f not in seen:
            unique_features.append(f)
            seen.add(f)

    return unique_features


def get_schema_columns() -> set[str]:
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    """).df()

    return set(schema["column_name"].tolist())


def get_years() -> list[int]:
    years = con.execute(f"""
        SELECT DISTINCT year
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        ORDER BY year
    """).df()["year"].tolist()

    return [int(y) for y in years]


def load_year_data(year: int, feature_cols: list[str]) -> pd.DataFrame:
    selected_cols = [DATE_COL, YEAR_COL, TARGET_COL] + feature_cols

    # Remove duplicates while preserving order.
    selected_cols_unique = []
    seen = set()
    for c in selected_cols:
        if c not in seen:
            selected_cols_unique.append(c)
            seen.add(c)

    select_sql = ",\n            ".join(selected_cols_unique)

    df = con.execute(f"""
        SELECT
            {select_sql}
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        WHERE year = {year}
          AND {TARGET_COL} IS NOT NULL
        ORDER BY {DATE_COL}
    """).df()

    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df[TARGET_COL] = df[TARGET_COL].astype("float32")

    for col in feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    return df


def compute_daily_rank_ic_for_feature(
    df: pd.DataFrame,
    feature: str,
) -> pd.DataFrame:
    sub = df[[DATE_COL, YEAR_COL, TARGET_COL, feature]].dropna()

    if sub.empty:
        return pd.DataFrame(
            columns=["feature", "dlycaldt", "year", "n_obs", "rank_ic"]
        )

    records = []

    for date, g in sub.groupby(DATE_COL, sort=True):
        n = len(g)

        if n < MIN_OBS_PER_DATE:
            rank_ic = np.nan
        elif g[feature].nunique(dropna=True) <= 1:
            rank_ic = np.nan
        elif g[TARGET_COL].nunique(dropna=True) <= 1:
            rank_ic = np.nan
        else:
            # Pandas Spearman uses average ranks for ties, which is important
            # for binary/event-style features.
            rank_ic = g[feature].corr(g[TARGET_COL], method="spearman")

        records.append({
            "feature": feature,
            "dlycaldt": date,
            "year": int(g[YEAR_COL].iloc[0]),
            "n_obs": n,
            "rank_ic": rank_ic,
        })

    return pd.DataFrame(records)


def summarize_ic(daily_ic: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for feature, g in daily_ic.groupby("feature", sort=False):
        x = g["rank_ic"].dropna()

        if len(x) == 0:
            rows.append({
                "feature": feature,
                "n_days": 0,
                "mean_rank_ic": np.nan,
                "std_rank_ic": np.nan,
                "icir": np.nan,
                "tstat": np.nan,
                "positive_frac": np.nan,
                "median_rank_ic": np.nan,
                "p25_rank_ic": np.nan,
                "p75_rank_ic": np.nan,
                "mean_abs_rank_ic": np.nan,
                "avg_daily_n_obs": g["n_obs"].mean(),
            })
            continue

        mean_ic = x.mean()
        std_ic = x.std(ddof=1)

        rows.append({
            "feature": feature,
            "n_days": int(len(x)),
            "mean_rank_ic": mean_ic,
            "std_rank_ic": std_ic,
            "icir": mean_ic / std_ic if std_ic and std_ic > 0 else np.nan,
            "tstat": mean_ic / (std_ic / np.sqrt(len(x))) if std_ic and std_ic > 0 else np.nan,
            "positive_frac": (x > 0).mean(),
            "median_rank_ic": x.median(),
            "p25_rank_ic": x.quantile(0.25),
            "p75_rank_ic": x.quantile(0.75),
            "mean_abs_rank_ic": x.abs().mean(),
            "avg_daily_n_obs": g["n_obs"].mean(),
        })

    summary = pd.DataFrame(rows)

    summary = summary.sort_values(
        ["tstat", "mean_rank_ic"],
        ascending=[False, False],
    )

    return summary


def summarize_ic_by_year(daily_ic: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (feature, year), g in daily_ic.groupby(["feature", "year"], sort=False):
        x = g["rank_ic"].dropna()

        if len(x) == 0:
            rows.append({
                "feature": feature,
                "year": year,
                "n_days": 0,
                "mean_rank_ic": np.nan,
                "std_rank_ic": np.nan,
                "icir": np.nan,
                "tstat": np.nan,
                "positive_frac": np.nan,
                "median_rank_ic": np.nan,
                "avg_daily_n_obs": g["n_obs"].mean(),
            })
            continue

        mean_ic = x.mean()
        std_ic = x.std(ddof=1)

        rows.append({
            "feature": feature,
            "year": int(year),
            "n_days": int(len(x)),
            "mean_rank_ic": mean_ic,
            "std_rank_ic": std_ic,
            "icir": mean_ic / std_ic if std_ic and std_ic > 0 else np.nan,
            "tstat": mean_ic / (std_ic / np.sqrt(len(x))) if std_ic and std_ic > 0 else np.nan,
            "positive_frac": (x > 0).mean(),
            "median_rank_ic": x.median(),
            "avg_daily_n_obs": g["n_obs"].mean(),
        })

    yearly_summary = pd.DataFrame(rows)

    yearly_summary = yearly_summary.sort_values(
        ["feature", "year"],
        ascending=[True, True],
    )

    return yearly_summary


# ============================================================
# Main
# ============================================================

print("\nLoading feature columns...")
feature_cols = load_feature_columns()

schema_cols = get_schema_columns()

missing_features = [f for f in feature_cols if f not in schema_cols]
if missing_features:
    missing_path = OUT_DIR / "missing_feature_columns.csv"
    pd.DataFrame({"missing_feature": missing_features}).to_csv(missing_path, index=False)
    print(f"Warning: {len(missing_features)} features not found in model_panel.")
    print(f"Saved missing feature list to: {missing_path}")

feature_cols = [f for f in feature_cols if f in schema_cols]

if len(feature_cols) == 0:
    raise ValueError("No usable feature columns found.")

pd.DataFrame({"feature": feature_cols}).to_csv(OUT_USED_FEATURES, index=False)

print(f"Number of features: {len(feature_cols)}")
print(f"Saved used feature columns to: {OUT_USED_FEATURES}")

years = get_years()
print(f"Years found in model_panel: {years}")

all_daily_ic_parts = []

for year in years:
    print(f"\nProcessing year {year}...")

    df_year = load_year_data(year, feature_cols)
    print(f"Loaded rows: {len(df_year):,}")

    year_ic_parts = []

    for i, feature in enumerate(feature_cols, start=1):
        if i % 10 == 0 or i == 1:
            print(f"  Feature {i}/{len(feature_cols)}: {feature}")

        feature_ic = compute_daily_rank_ic_for_feature(df_year, feature)
        year_ic_parts.append(feature_ic)

    year_daily_ic = pd.concat(year_ic_parts, ignore_index=True)
    all_daily_ic_parts.append(year_daily_ic)

    year_out_path = OUT_DIR / f"single_feature_daily_rank_ic_{year}.csv"
    year_daily_ic.to_csv(year_out_path, index=False)

    print(f"Saved yearly daily IC to: {year_out_path}")

    del df_year, year_ic_parts, year_daily_ic
    gc.collect()


daily_ic = pd.concat(all_daily_ic_parts, ignore_index=True)
daily_ic.to_csv(OUT_DAILY_IC, index=False)

print(f"\nSaved all daily feature IC to: {OUT_DAILY_IC}")


summary = summarize_ic(daily_ic)
summary.to_csv(OUT_SUMMARY, index=False)

print(f"Saved feature IC summary to: {OUT_SUMMARY}")


yearly_summary = summarize_ic_by_year(daily_ic)
yearly_summary.to_csv(OUT_YEARLY_SUMMARY, index=False)

print(f"Saved yearly feature IC summary to: {OUT_YEARLY_SUMMARY}")


print("\nTop 20 features by t-stat:")
print(
    summary[
        [
            "feature",
            "n_days",
            "mean_rank_ic",
            "std_rank_ic",
            "icir",
            "tstat",
            "positive_frac",
        ]
    ]
    .head(20)
    .to_string(index=False)
)

print("\nBottom 20 features by t-stat:")
print(
    summary[
        [
            "feature",
            "n_days",
            "mean_rank_ic",
            "std_rank_ic",
            "icir",
            "tstat",
            "positive_frac",
        ]
    ]
    .tail(20)
    .to_string(index=False)
)

print("\nFinished single-feature IC analysis.")
print(f"All outputs saved under: {OUT_DIR}")