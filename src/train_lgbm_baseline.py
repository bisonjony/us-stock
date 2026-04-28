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
FEATURE_COLS_CSV = ROOT / "data/clean_parquet/model_panel_analysis/model_panel_feature_columns.csv"

OUT_DIR = ROOT / "data/model_outputs/lgbm_baseline_2019_2022_valid_2023"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Split setting
# ============================================================

TRAIN_YEARS = [2019, 2020, 2021, 2022]
VALID_YEARS = [2023]

TARGET_COL = "target_5d_cs_zscore"
ID_COLS = ["permno", "dlycaldt", "year"]

# Since the label is t+1 to t+5, remove the last 5 trading dates
# before the train/validation boundary to avoid label leakage across periods.
FORWARD_HORIZON_DAYS = 5
DROP_LAST_N_DATES_AT_SPLIT_BOUNDARY = True


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
# Helpers
# ============================================================

def load_feature_columns() -> list[str]:
    feature_df = pd.read_csv(FEATURE_COLS_CSV)

    if "feature" not in feature_df.columns:
        raise ValueError(f"{FEATURE_COLS_CSV} must contain a column named 'feature'.")

    feature_cols = feature_df["feature"].dropna().astype(str).tolist()

    # Remove duplicate feature names while preserving order.
    seen = set()
    unique_features = []
    for col in feature_cols:
        if col not in seen:
            unique_features.append(col)
            seen.add(col)

    return unique_features


def get_model_panel_schema_cols() -> set[str]:
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    """).df()

    return set(schema["column_name"].tolist())


def get_last_trading_dates(year: int, n: int) -> list[str]:
    dates = con.execute(f"""
        SELECT DISTINCT dlycaldt
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        WHERE year = {year}
        ORDER BY dlycaldt DESC
        LIMIT {n}
    """).df()["dlycaldt"]

    dates = pd.to_datetime(dates).dt.strftime("%Y-%m-%d").tolist()
    return dates


def format_year_filter(years: list[int]) -> str:
    return "(" + ", ".join(str(y) for y in years) + ")"


def format_date_exclusion(dates: list[str]) -> str:
    if len(dates) == 0:
        return ""

    date_literals = ", ".join(f"DATE '{d}'" for d in dates)
    return f"AND dlycaldt NOT IN ({date_literals})"


def load_panel(
    years: list[int],
    feature_cols: list[str],
    exclude_dates: list[str] | None = None,
) -> pd.DataFrame:
    exclude_dates = exclude_dates or []

    selected_cols = ID_COLS + [TARGET_COL] + feature_cols

    # Remove duplicate selected columns while preserving order.
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
    return df


def make_lgb_arrays(
    df: pd.DataFrame,
    feature_cols: list[str],
    keep_meta: bool,
):
    y = df[TARGET_COL].to_numpy(dtype=np.float32, copy=True)
    X = df[feature_cols].to_numpy(dtype=np.float32, copy=True)

    meta = None
    if keep_meta:
        meta = df[ID_COLS + [TARGET_COL]].copy()
        meta["dlycaldt"] = pd.to_datetime(meta["dlycaldt"])

    return X, y, meta


def compute_daily_ic(pred_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    records = []

    for date, g in pred_df.groupby("dlycaldt", sort=True):
        g = g[[TARGET_COL, "pred_score"]].dropna()

        n = len(g)
        if n < 10:
            records.append({
                "dlycaldt": date,
                "n": n,
                "rank_ic": np.nan,
                "pearson_ic": np.nan,
            })
            continue

        if g[TARGET_COL].nunique() <= 1 or g["pred_score"].nunique() <= 1:
            rank_ic = np.nan
            pearson_ic = np.nan
        else:
            rank_ic = g["pred_score"].corr(g[TARGET_COL], method="spearman")
            pearson_ic = g["pred_score"].corr(g[TARGET_COL], method="pearson")

        records.append({
            "dlycaldt": date,
            "n": n,
            "rank_ic": rank_ic,
            "pearson_ic": pearson_ic,
        })

    daily_ic = pd.DataFrame(records)

    valid_rank_ic = daily_ic["rank_ic"].dropna()
    valid_pearson_ic = daily_ic["pearson_ic"].dropna()

    def summarize_ic(x: pd.Series, prefix: str) -> dict:
        if len(x) == 0:
            return {
                f"{prefix}_n_days": 0,
                f"{prefix}_mean": np.nan,
                f"{prefix}_std": np.nan,
                f"{prefix}_icir": np.nan,
                f"{prefix}_tstat": np.nan,
                f"{prefix}_positive_frac": np.nan,
            }

        mean = x.mean()
        std = x.std(ddof=1)

        return {
            f"{prefix}_n_days": int(len(x)),
            f"{prefix}_mean": mean,
            f"{prefix}_std": std,
            f"{prefix}_icir": mean / std if std and std > 0 else np.nan,
            f"{prefix}_tstat": mean / (std / np.sqrt(len(x))) if std and std > 0 else np.nan,
            f"{prefix}_positive_frac": (x > 0).mean(),
        }

    summary = {
        "n_prediction_rows": len(pred_df),
        "n_unique_dates": pred_df["dlycaldt"].nunique(),
        "overall_spearman": pred_df["pred_score"].corr(
            pred_df[TARGET_COL], method="spearman"
        ),
        "overall_pearson": pred_df["pred_score"].corr(
            pred_df[TARGET_COL], method="pearson"
        ),
    }

    summary.update(summarize_ic(valid_rank_ic, "daily_rank_ic"))
    summary.update(summarize_ic(valid_pearson_ic, "daily_pearson_ic"))

    summary_df = pd.DataFrame([summary])
    return daily_ic, summary_df


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

print("\nReading feature list...")
feature_cols = load_feature_columns()

schema_cols = get_model_panel_schema_cols()

missing_features = [c for c in feature_cols if c not in schema_cols]
if missing_features:
    missing_path = OUT_DIR / "missing_features_from_model_panel.csv"
    pd.DataFrame({"missing_feature": missing_features}).to_csv(missing_path, index=False)

    print(f"\nWarning: {len(missing_features)} feature columns are not in model_panel.")
    print(f"Saved missing feature list to: {missing_path}")

feature_cols = [c for c in feature_cols if c in schema_cols]

if TARGET_COL not in schema_cols:
    raise ValueError(f"{TARGET_COL} is not in model_panel.")

if len(feature_cols) == 0:
    raise ValueError("No usable feature columns found.")

used_features_path = OUT_DIR / "used_feature_columns.csv"
pd.DataFrame({"feature": feature_cols}).to_csv(used_features_path, index=False)

print(f"Number of model features: {len(feature_cols)}")
print(f"Saved used feature list to: {used_features_path}")


# ------------------------------------------------------------
# Remove last 5 trading dates near split boundaries.
# ------------------------------------------------------------

train_exclude_dates = []
valid_exclude_dates = []

if DROP_LAST_N_DATES_AT_SPLIT_BOUNDARY:
    train_boundary_year = max(TRAIN_YEARS)
    valid_boundary_year = max(VALID_YEARS)

    train_exclude_dates = get_last_trading_dates(
        train_boundary_year,
        FORWARD_HORIZON_DAYS,
    )

    valid_exclude_dates = get_last_trading_dates(
        valid_boundary_year,
        FORWARD_HORIZON_DAYS,
    )

    split_gap_info = {
        "train_years": TRAIN_YEARS,
        "valid_years": VALID_YEARS,
        "forward_horizon_days": FORWARD_HORIZON_DAYS,
        "train_excluded_dates": train_exclude_dates,
        "valid_excluded_dates": valid_exclude_dates,
    }

    with open(OUT_DIR / "split_gap_info.json", "w") as f:
        json.dump(split_gap_info, f, indent=2)

    print("\nExcluded dates for label-boundary leakage control:")
    print(f"Train excluded dates: {train_exclude_dates}")
    print(f"Valid excluded dates: {valid_exclude_dates}")


# ------------------------------------------------------------
# Load train and validation data.
# ------------------------------------------------------------

print("\nLoading training data...")
train_df = load_panel(
    years=TRAIN_YEARS,
    feature_cols=feature_cols,
    exclude_dates=train_exclude_dates,
)

print(f"Training rows: {len(train_df):,}")

print("\nLoading validation data...")
valid_df = load_panel(
    years=VALID_YEARS,
    feature_cols=feature_cols,
    exclude_dates=valid_exclude_dates,
)

print(f"Validation rows: {len(valid_df):,}")


# ------------------------------------------------------------
# Convert to compact arrays.
# ------------------------------------------------------------

print("\nConverting pandas dataframes to float32 arrays...")

X_train, y_train, _ = make_lgb_arrays(
    train_df,
    feature_cols,
    keep_meta=False,
)

del train_df
gc.collect()

X_valid, y_valid, valid_meta = make_lgb_arrays(
    valid_df,
    feature_cols,
    keep_meta=True,
)

del valid_df
gc.collect()

print(f"X_train shape: {X_train.shape}")
print(f"X_valid shape: {X_valid.shape}")


# ------------------------------------------------------------
# Train LightGBM.
# ------------------------------------------------------------

params = {
    "objective": "regression",
    "metric": "rmse",

    "learning_rate": 0.03,
    "num_leaves": 31,
    "max_depth": 6,
    "min_data_in_leaf": 500,

    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,

    "lambda_l1": 0.0,
    "lambda_l2": 5.0,

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

with open(OUT_DIR / "lgbm_params.json", "w") as f:
    json.dump(params, f, indent=2)

train_set = lgb.Dataset(
    X_train,
    label=y_train,
    feature_name=feature_cols,
    free_raw_data=True,
)

valid_set = lgb.Dataset(
    X_valid,
    label=y_valid,
    reference=train_set,
    feature_name=feature_cols,
    free_raw_data=True,
)

evals_result = {}

print("\nTraining LightGBM baseline...")

model = lgb.train(
    params=params,
    train_set=train_set,
    num_boost_round=2000,
    valid_sets=[train_set, valid_set],
    valid_names=["train", "valid"],
    callbacks=[
        lgb.early_stopping(stopping_rounds=100),
        lgb.log_evaluation(period=50),
        lgb.record_evaluation(evals_result),
    ],
)

model_path = OUT_DIR / "lgbm_baseline_model.txt"
model.save_model(str(model_path))

print(f"\nSaved model to: {model_path}")
print(f"Best iteration: {model.best_iteration}")


# Free training arrays after model fitting.
del X_train, y_train, train_set
gc.collect()


# ------------------------------------------------------------
# Validation prediction.
# ------------------------------------------------------------

print("\nPredicting validation set...")

pred_valid = model.predict(
    X_valid,
    num_iteration=model.best_iteration,
)

valid_pred_df = valid_meta.copy()
valid_pred_df["pred_score"] = pred_valid.astype(np.float32)

valid_pred_path = OUT_DIR / "validation_predictions.parquet"
valid_pred_csv_gz_path = OUT_DIR / "validation_predictions.csv.gz"

valid_pred_df.to_parquet(valid_pred_path, index=False)
valid_pred_df.to_csv(valid_pred_csv_gz_path, index=False, compression="gzip")

print(f"Saved validation predictions to: {valid_pred_path}")
print(f"Saved compressed validation predictions to: {valid_pred_csv_gz_path}")


# ------------------------------------------------------------
# Feature importance.
# ------------------------------------------------------------

importance_df = pd.DataFrame({
    "feature": feature_cols,
    "importance_gain": model.feature_importance(importance_type="gain"),
    "importance_split": model.feature_importance(importance_type="split"),
})

importance_df = importance_df.sort_values(
    ["importance_gain", "importance_split"],
    ascending=[False, False],
)

importance_path = OUT_DIR / "feature_importance.csv"
importance_df.to_csv(importance_path, index=False)

print(f"Saved feature importance to: {importance_path}")


# ------------------------------------------------------------
# Evaluation history.
# ------------------------------------------------------------

eval_history_path = OUT_DIR / "eval_history.csv"
save_eval_history(evals_result, eval_history_path)
print(f"Saved evaluation history to: {eval_history_path}")


# ------------------------------------------------------------
# Daily IC summary.
# ------------------------------------------------------------

print("\nComputing validation daily IC...")

daily_ic, ic_summary = compute_daily_ic(valid_pred_df)

daily_ic_path = OUT_DIR / "validation_daily_ic.csv"
ic_summary_path = OUT_DIR / "validation_ic_summary.csv"

daily_ic.to_csv(daily_ic_path, index=False)
ic_summary.to_csv(ic_summary_path, index=False)

print(f"Saved daily IC to: {daily_ic_path}")
print(f"Saved IC summary to: {ic_summary_path}")

print("\nValidation IC summary:")
print(ic_summary.to_string(index=False))


# ------------------------------------------------------------
# Final cleanup.
# ------------------------------------------------------------

del X_valid, y_valid, valid_set, valid_pred_df
gc.collect()

print("\nFinished training LightGBM baseline.")
print(f"All outputs saved under: {OUT_DIR}")