from pathlib import Path
import json
import gc

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd


# ============================================================
# Paths
# ============================================================

ROOT = Path("/home/xul9527/us-stock")

MODEL_PANEL_GLOB = ROOT / "data/clean_parquet/model_panel/**/*.parquet"

# This file was produced by the controlled top-K validation experiment.
# It freezes the top-20 feature set selected before choosing the final model.
SELECTED_TOP20_FEATURES_CSV = (
    ROOT
    / "model_outputs/lgbm_topk_stable_features_2019_2022_valid_2023"
    / "selected_top20_features.csv"
)

OUT_DIR = ROOT / "model_outputs/lgbm_final_top20_train_2019_2023_for_backtest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Final training setting
# ============================================================

TRAIN_YEARS = [2019, 2020, 2021, 2022, 2023]

TARGET_COL = "target_5d_cs_zscore"
ID_COLS = ["permno", "dlycaldt", "year"]

# The selected validation winner was:
# top20_medium_l2_20_leaf15, best_iteration = 28.
FINAL_NUM_BOOST_ROUND = 28

# Exclude last 5 trading dates of 2023 because target_5d uses t+1,...,t+5.
# Otherwise labels for the final 2023 dates would use 2024 returns.
FORWARD_HORIZON_DAYS = 5
DROP_LAST_N_DATES_AT_BACKTEST_BOUNDARY = True


# ============================================================
# Frozen LightGBM setup from validation winner
# ============================================================

LGBM_PARAMS = {
    "objective": "regression",
    "metric": "rmse",

    "learning_rate": 0.02,
    "num_leaves": 15,
    "max_depth": 4,
    "min_data_in_leaf": 2000,

    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,

    "lambda_l1": 0.0,
    "lambda_l2": 20.0,

    "max_bin": 63,
    "histogram_pool_size": 512,

    "force_col_wise": True,
    "deterministic": True,

    "num_threads": 4,
    "verbosity": 1,
    "seed": 9527,
    "feature_fraction_seed": 9527,
    "bagging_seed": 9527,
    "data_random_seed": 9527,
}


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

def get_model_panel_schema_cols() -> set[str]:
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    """).df()

    return set(schema["column_name"].tolist())


def load_selected_features() -> list[str]:
    if not SELECTED_TOP20_FEATURES_CSV.exists():
        raise FileNotFoundError(
            f"Cannot find selected feature file:\n{SELECTED_TOP20_FEATURES_CSV}\n\n"
            "Run src/train_lgbm_baseline.py first to produce the controlled "
            "top-K validation outputs."
        )

    selected = pd.read_csv(SELECTED_TOP20_FEATURES_CSV)

    if "feature" not in selected.columns:
        raise ValueError(
            f"{SELECTED_TOP20_FEATURES_CSV} must contain a column named 'feature'."
        )

    features = selected["feature"].dropna().astype(str).tolist()

    # Preserve order and remove duplicates.
    out = []
    seen = set()
    for f in features:
        if f not in seen:
            out.append(f)
            seen.add(f)

    if len(out) != 20:
        print(f"Warning: expected 20 features, but found {len(out)} features.")

    return out


def get_last_trading_dates(year: int, n: int) -> list[str]:
    df = con.execute(f"""
        SELECT DISTINCT dlycaldt
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        WHERE year = {year}
        ORDER BY dlycaldt DESC
        LIMIT {n}
    """).df()

    return pd.to_datetime(df["dlycaldt"]).dt.strftime("%Y-%m-%d").tolist()


def format_year_filter(years: list[int]) -> str:
    return "(" + ", ".join(str(y) for y in years) + ")"


def format_date_exclusion(dates: list[str]) -> str:
    if not dates:
        return ""

    date_literals = ", ".join(f"DATE '{d}'" for d in dates)
    return f"AND dlycaldt NOT IN ({date_literals})"


def load_training_panel(
    years: list[int],
    feature_cols: list[str],
    exclude_dates: list[str],
) -> pd.DataFrame:
    selected_cols = ID_COLS + [TARGET_COL] + feature_cols

    selected_cols_unique = []
    seen = set()
    for col in selected_cols:
        if col not in seen:
            selected_cols_unique.append(col)
            seen.add(col)

    select_sql = ",\n            ".join(selected_cols_unique)
    year_filter = format_year_filter(years)
    date_exclusion = format_date_exclusion(exclude_dates)

    query = f"""
        SELECT
            {select_sql}
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        WHERE year IN {year_filter}
          AND {TARGET_COL} IS NOT NULL
          {date_exclusion}
        ORDER BY dlycaldt, permno
    """

    df = con.execute(query).df()

    df["dlycaldt"] = pd.to_datetime(df["dlycaldt"])
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce").astype("float32")

    for col in feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    return df


def save_eval_history(evals_result: dict, out_path: Path) -> None:
    rows = []

    for dataset_name, metric_dict in evals_result.items():
        for metric_name, values in metric_dict.items():
            for i, value in enumerate(values, start=1):
                rows.append({
                    "iteration": i,
                    "dataset": dataset_name,
                    "metric": metric_name,
                    "value": value,
                })

    pd.DataFrame(rows).to_csv(out_path, index=False)


# ============================================================
# Main
# ============================================================

print("\nLoading selected top-20 validation-chosen features...")
feature_cols = load_selected_features()

schema_cols = get_model_panel_schema_cols()

missing_features = [c for c in feature_cols if c not in schema_cols]
if missing_features:
    missing_path = OUT_DIR / "missing_features_from_model_panel.csv"
    pd.DataFrame({"missing_feature": missing_features}).to_csv(missing_path, index=False)
    raise ValueError(
        f"{len(missing_features)} selected features are missing from model_panel. "
        f"Saved list to {missing_path}"
    )

if TARGET_COL not in schema_cols:
    raise ValueError(f"{TARGET_COL} is not in model_panel.")

used_features_path = OUT_DIR / "used_top20_feature_columns.csv"
pd.DataFrame({"feature": feature_cols}).to_csv(used_features_path, index=False)

print(f"Number of selected features: {len(feature_cols)}")
print(f"Saved used feature list to: {used_features_path}")


# ------------------------------------------------------------
# Boundary leakage control
# ------------------------------------------------------------

excluded_dates = []

if DROP_LAST_N_DATES_AT_BACKTEST_BOUNDARY:
    excluded_dates = get_last_trading_dates(
        year=max(TRAIN_YEARS),
        n=FORWARD_HORIZON_DAYS,
    )

split_info = {
    "train_years": TRAIN_YEARS,
    "target_col": TARGET_COL,
    "forward_horizon_days": FORWARD_HORIZON_DAYS,
    "excluded_dates": excluded_dates,
    "reason_for_exclusion": (
        "target_5d uses t+1 through t+5. Excluding the last 5 trading dates "
        "of 2023 prevents training labels from using 2024 returns."
    ),
    "final_model_setup": "top20_medium_l2_20_leaf15",
    "final_num_boost_round": FINAL_NUM_BOOST_ROUND,
}

with open(OUT_DIR / "final_training_config.json", "w") as f:
    json.dump(split_info, f, indent=2)

print("\nExcluded dates for 2024 boundary leakage control:")
print(excluded_dates)


# ------------------------------------------------------------
# Load training data
# ------------------------------------------------------------

print("\nLoading final training data...")
train_df = load_training_panel(
    years=TRAIN_YEARS,
    feature_cols=feature_cols,
    exclude_dates=excluded_dates,
)

print(f"Training rows: {len(train_df):,}")
print(f"Training date range: {train_df['dlycaldt'].min()} to {train_df['dlycaldt'].max()}")

size_summary = pd.DataFrame([{
    "n_train_rows": len(train_df),
    "n_features": len(feature_cols),
    "min_train_date": train_df["dlycaldt"].min(),
    "max_train_date": train_df["dlycaldt"].max(),
    "target_mean": float(train_df[TARGET_COL].mean()),
    "target_std": float(train_df[TARGET_COL].std(ddof=1)),
}])

size_summary_path = OUT_DIR / "final_training_size_summary.csv"
size_summary.to_csv(size_summary_path, index=False)

print(f"Saved training size summary to: {size_summary_path}")


# ------------------------------------------------------------
# Convert to LightGBM arrays
# ------------------------------------------------------------

print("\nConverting training data to float32 arrays...")

X_train = train_df[feature_cols].to_numpy(dtype=np.float32, copy=True)
y_train = train_df[TARGET_COL].to_numpy(dtype=np.float32, copy=True)

del train_df
gc.collect()

print(f"X_train shape: {X_train.shape}")


# ------------------------------------------------------------
# Train final LightGBM model
# ------------------------------------------------------------

params_path = OUT_DIR / "final_lgbm_params.json"
with open(params_path, "w") as f:
    json.dump(
        {
            "params": LGBM_PARAMS,
            "num_boost_round": FINAL_NUM_BOOST_ROUND,
            "features": feature_cols,
        },
        f,
        indent=2,
    )

print("\nCreating LightGBM Dataset...")

train_set = lgb.Dataset(
    X_train,
    label=y_train,
    feature_name=feature_cols,
    free_raw_data=True,
)

evals_result = {}

print("\nTraining final LightGBM model for backtesting...")

model = lgb.train(
    params=LGBM_PARAMS,
    train_set=train_set,
    num_boost_round=FINAL_NUM_BOOST_ROUND,
    valid_sets=[train_set],
    valid_names=["train"],
    callbacks=[
        lgb.log_evaluation(period=5),
        lgb.record_evaluation(evals_result),
    ],
)

model_path = OUT_DIR / "lgbm_final_top20_train_2019_2023_model.txt"
model.save_model(str(model_path))

print(f"\nSaved final model to: {model_path}")


# ------------------------------------------------------------
# Save training evaluation history and feature importance
# ------------------------------------------------------------

eval_history_path = OUT_DIR / "final_train_eval_history.csv"
save_eval_history(evals_result, eval_history_path)

importance_df = pd.DataFrame({
    "feature": feature_cols,
    "importance_gain": model.feature_importance(importance_type="gain"),
    "importance_split": model.feature_importance(importance_type="split"),
})

importance_df = importance_df.sort_values(
    ["importance_gain", "importance_split"],
    ascending=[False, False],
)

importance_path = OUT_DIR / "final_model_feature_importance.csv"
importance_df.to_csv(importance_path, index=False)

print(f"Saved training evaluation history to: {eval_history_path}")
print(f"Saved feature importance to: {importance_path}")


# ------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------

del X_train, y_train, train_set
gc.collect()

print("\nFinished training final LightGBM model for backtesting.")
print(f"All outputs saved under: {OUT_DIR}")