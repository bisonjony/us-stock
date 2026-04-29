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

SINGLE_FEATURE_YEARLY_IC = (
    ROOT / "data/single_feature_ic_analysis/single_feature_rank_ic_summary_by_year.csv"
)

OUT_DIR = ROOT / "model_outputs/lgbm_topk_stable_features_2019_2022_valid_2023"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Split and model settings
# ============================================================

TRAIN_YEARS = [2019, 2020, 2021, 2022]
VALID_YEARS = [2023]

TARGET_COL = "target_5d_cs_zscore"
ID_COLS = ["permno", "dlycaldt", "year"]

TOP_K_LIST = [1, 3, 5, 10, 20]

# Use only continuous transformed features for the first controlled LGBM.
# This matches the feature-score baseline and avoids raw binary/event flags
# in the first controlled tuning round.
FEATURE_SUFFIX = "_cs_winsor_zscore"

# Stable feature selection uses training years only.
MIN_DAYS_PER_YEAR = 100
MIN_TRAIN_YEARS = 3
MIN_SIGN_CONSISTENCY = 0.75

# Since target is t+1 to t+5, remove the last 5 trading dates
# before train/validation boundaries.
FORWARD_HORIZON_DAYS = 5
DROP_LAST_N_DATES_AT_SPLIT_BOUNDARY = True


# ============================================================
# LightGBM controlled configs
# ============================================================

# These are intentionally shallow / regularized.
# The goal is not maximum in-sample fit. The goal is to preserve stable
# monotone cross-sectional signal and avoid noisy overfitting.
LGBM_CONFIGS = [
    {
        "config_name": "shallow_l2_10_leaf7",
        "params": {
            "objective": "regression",
            "metric": "rmse",
            "learning_rate": 0.03,
            "num_leaves": 7,
            "max_depth": 3,
            "min_data_in_leaf": 2000,
            "feature_fraction": 1.0,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "lambda_l1": 0.0,
            "lambda_l2": 10.0,
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
        },
        "num_boost_round": 1000,
        "early_stopping_rounds": 100,
    },
    {
        "config_name": "shallow_l2_30_leaf7",
        "params": {
            "objective": "regression",
            "metric": "rmse",
            "learning_rate": 0.03,
            "num_leaves": 7,
            "max_depth": 3,
            "min_data_in_leaf": 3000,
            "feature_fraction": 1.0,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "lambda_l1": 0.0,
            "lambda_l2": 30.0,
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
        },
        "num_boost_round": 1000,
        "early_stopping_rounds": 100,
    },
    {
        "config_name": "medium_l2_20_leaf15",
        "params": {
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
        },
        "num_boost_round": 1500,
        "early_stopping_rounds": 100,
    },
]


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


def get_last_trading_dates(year: int, n: int) -> list[str]:
    dates = con.execute(f"""
        SELECT DISTINCT dlycaldt
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        WHERE year = {year}
        ORDER BY dlycaldt DESC
        LIMIT {n}
    """).df()["dlycaldt"]

    return pd.to_datetime(dates).dt.strftime("%Y-%m-%d").tolist()


def format_year_filter(years: list[int]) -> str:
    return "(" + ", ".join(str(y) for y in years) + ")"


def format_date_exclusion(dates: list[str]) -> str:
    if not dates:
        return ""
    date_literals = ", ".join(f"DATE '{d}'" for d in dates)
    return f"AND dlycaldt NOT IN ({date_literals})"


def select_stable_features(schema_cols: set[str]) -> pd.DataFrame:
    if not SINGLE_FEATURE_YEARLY_IC.exists():
        raise FileNotFoundError(
            f"Cannot find {SINGLE_FEATURE_YEARLY_IC}. "
            "Run src/compute_single_feature_ic.py first."
        )

    ic = pd.read_csv(SINGLE_FEATURE_YEARLY_IC)

    required_cols = {"feature", "year", "n_days", "mean_rank_ic", "tstat"}
    missing_cols = required_cols - set(ic.columns)
    if missing_cols:
        raise ValueError(
            f"{SINGLE_FEATURE_YEARLY_IC} is missing columns: {missing_cols}"
        )

    ic = ic.copy()
    ic["year"] = ic["year"].astype(int)

    train_ic = ic[
        (ic["year"].isin(TRAIN_YEARS))
        & (ic["feature"].isin(schema_cols))
        & (ic["feature"].str.endswith(FEATURE_SUFFIX))
        & (ic["n_days"] >= MIN_DAYS_PER_YEAR)
        & (ic["mean_rank_ic"].notna())
    ].copy()

    if train_ic.empty:
        raise ValueError("No eligible feature IC rows found for training years.")

    rows = []

    for feature, g in train_ic.groupby("feature"):
        x = g["mean_rank_ic"].dropna()
        t = g["tstat"].dropna()

        if len(x) == 0:
            continue

        n_train_years = int(g["year"].nunique())

        n_pos = int((x > 0).sum())
        n_neg = int((x < 0).sum())
        sign_consistency = max(n_pos, n_neg) / len(x)

        train_mean_ic = float(x.mean())
        train_abs_mean_ic = abs(train_mean_ic)
        train_mean_abs_year_ic = float(x.abs().mean())
        train_min_abs_year_ic = float(x.abs().min())

        avg_abs_tstat = float(t.abs().mean()) if len(t) else np.nan
        min_abs_tstat = float(t.abs().min()) if len(t) else np.nan

        if np.isnan(avg_abs_tstat):
            stability_score = train_abs_mean_ic * sign_consistency
        else:
            stability_score = (
                train_abs_mean_ic
                * sign_consistency
                * np.log1p(avg_abs_tstat)
            )

        rows.append({
            "feature": feature,
            "n_train_years": n_train_years,
            "train_years_present": ",".join(map(str, sorted(g["year"].unique()))),
            "train_mean_ic": train_mean_ic,
            "train_abs_mean_ic": train_abs_mean_ic,
            "train_mean_abs_year_ic": train_mean_abs_year_ic,
            "train_min_abs_year_ic": train_min_abs_year_ic,
            "sign_consistency": sign_consistency,
            "avg_abs_tstat": avg_abs_tstat,
            "min_abs_tstat": min_abs_tstat,
            "stability_score": stability_score,
        })

    summary = pd.DataFrame(rows)

    if summary.empty:
        raise ValueError("No stable feature summary could be computed.")

    filtered = summary[
        (summary["n_train_years"] >= MIN_TRAIN_YEARS)
        & (summary["sign_consistency"] >= MIN_SIGN_CONSISTENCY)
    ].copy()

    max_k = max(TOP_K_LIST)

    if len(filtered) < max_k:
        print(
            f"Warning: only {len(filtered)} features passed strict stability filters. "
            "Falling back to all eligible features sorted by stability score."
        )
        filtered = summary.copy()

    ranking = filtered.sort_values(
        [
            "stability_score",
            "train_abs_mean_ic",
            "train_mean_abs_year_ic",
            "avg_abs_tstat",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    ranking["rank"] = np.arange(1, len(ranking) + 1)

    return ranking


def load_panel(
    years: list[int],
    feature_cols: list[str],
    exclude_dates: list[str] | None = None,
) -> pd.DataFrame:
    exclude_dates = exclude_dates or []

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


def make_arrays(df: pd.DataFrame, feature_cols: list[str], keep_meta: bool):
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
        gg = g[[TARGET_COL, "pred_score"]].dropna()
        n = len(gg)

        if n < 10:
            rank_ic = np.nan
            pearson_ic = np.nan
        elif gg[TARGET_COL].nunique() <= 1 or gg["pred_score"].nunique() <= 1:
            rank_ic = np.nan
            pearson_ic = np.nan
        else:
            rank_ic = gg["pred_score"].corr(gg[TARGET_COL], method="spearman")
            pearson_ic = gg["pred_score"].corr(gg[TARGET_COL], method="pearson")

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
            f"{prefix}_positive_frac": float((x > 0).mean()),
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

    return daily_ic, pd.DataFrame([summary])


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


def train_one_lgbm(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    config: dict,
    experiment_dir: Path,
) -> dict:
    experiment_dir.mkdir(parents=True, exist_ok=True)

    X_train, y_train, _ = make_arrays(train_df, feature_cols, keep_meta=False)
    X_valid, y_valid, valid_meta = make_arrays(valid_df, feature_cols, keep_meta=True)

    params = config["params"]
    num_boost_round = config["num_boost_round"]
    early_stopping_rounds = config["early_stopping_rounds"]

    with open(experiment_dir / "lgbm_params.json", "w") as f:
        json.dump(
            {
                "config_name": config["config_name"],
                "params": params,
                "num_boost_round": num_boost_round,
                "early_stopping_rounds": early_stopping_rounds,
                "feature_cols": feature_cols,
            },
            f,
            indent=2,
        )

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

    print(f"\nTraining {experiment_dir.name}...")
    print(f"X_train shape: {X_train.shape}, X_valid shape: {X_valid.shape}")

    model = lgb.train(
        params=params,
        train_set=train_set,
        num_boost_round=num_boost_round,
        valid_sets=[train_set, valid_set],
        valid_names=["train", "valid"],
        callbacks=[
            lgb.early_stopping(stopping_rounds=early_stopping_rounds),
            lgb.log_evaluation(period=50),
            lgb.record_evaluation(evals_result),
        ],
    )

    model_path = experiment_dir / "model.txt"
    model.save_model(str(model_path))

    pred_valid = model.predict(
        X_valid,
        num_iteration=model.best_iteration,
    ).astype(np.float32)

    valid_pred_df = valid_meta.copy()
    valid_pred_df["pred_score"] = pred_valid

    pred_path = experiment_dir / "validation_predictions.parquet"
    valid_pred_df.to_parquet(pred_path, index=False)

    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance_gain": model.feature_importance(importance_type="gain"),
        "importance_split": model.feature_importance(importance_type="split"),
    }).sort_values(
        ["importance_gain", "importance_split"],
        ascending=[False, False],
    )

    importance_path = experiment_dir / "feature_importance.csv"
    importance_df.to_csv(importance_path, index=False)

    eval_history_path = experiment_dir / "eval_history.csv"
    save_eval_history(evals_result, eval_history_path)

    daily_ic, ic_summary = compute_daily_ic(valid_pred_df)

    daily_ic_path = experiment_dir / "validation_daily_ic.csv"
    ic_summary_path = experiment_dir / "validation_ic_summary.csv"

    daily_ic.to_csv(daily_ic_path, index=False)
    ic_summary.to_csv(ic_summary_path, index=False)

    row = ic_summary.iloc[0].to_dict()
    row.update({
        "experiment": experiment_dir.name,
        "config_name": config["config_name"],
        "n_features": len(feature_cols),
        "features": "|".join(feature_cols),
        "best_iteration": int(model.best_iteration),
        "model_path": str(model_path),
        "prediction_path": str(pred_path),
    })

    del X_train, y_train, X_valid, y_valid, train_set, valid_set, valid_pred_df
    gc.collect()

    return row


# ============================================================
# Main
# ============================================================

print("\nChecking model panel schema...")
schema_cols = get_model_panel_schema_cols()

if TARGET_COL not in schema_cols:
    raise ValueError(f"{TARGET_COL} is not in model_panel.")

print("\nSelecting stable features using training years only...")
feature_ranking = select_stable_features(schema_cols)

feature_ranking_path = OUT_DIR / "feature_stability_ranking_train_years.csv"
feature_ranking.to_csv(feature_ranking_path, index=False)

max_k = max(TOP_K_LIST)
selected_max_features = feature_ranking.head(max_k)["feature"].tolist()

selected_path = OUT_DIR / f"selected_top{max_k}_features.csv"
feature_ranking.head(max_k).to_csv(selected_path, index=False)

print(f"Saved feature stability ranking to: {feature_ranking_path}")
print(f"Saved selected top-{max_k} features to: {selected_path}")

print("\nSelected top features:")
print(feature_ranking.head(max_k).to_string(index=False))


# ------------------------------------------------------------
# Boundary leakage control
# ------------------------------------------------------------

train_exclude_dates = []
valid_exclude_dates = []

if DROP_LAST_N_DATES_AT_SPLIT_BOUNDARY:
    train_exclude_dates = get_last_trading_dates(
        max(TRAIN_YEARS),
        FORWARD_HORIZON_DAYS,
    )

    valid_exclude_dates = get_last_trading_dates(
        max(VALID_YEARS),
        FORWARD_HORIZON_DAYS,
    )

split_gap_info = {
    "train_years": TRAIN_YEARS,
    "valid_years": VALID_YEARS,
    "top_k_list": TOP_K_LIST,
    "feature_suffix": FEATURE_SUFFIX,
    "forward_horizon_days": FORWARD_HORIZON_DAYS,
    "train_excluded_dates": train_exclude_dates,
    "valid_excluded_dates": valid_exclude_dates,
    "configs": [c["config_name"] for c in LGBM_CONFIGS],
}

with open(OUT_DIR / "experiment_config.json", "w") as f:
    json.dump(split_gap_info, f, indent=2)

print("\nExcluded dates for label-boundary leakage control:")
print(f"Train excluded dates: {train_exclude_dates}")
print(f"Valid excluded dates: {valid_exclude_dates}")


# ------------------------------------------------------------
# Load max-K train/validation data once.
# ------------------------------------------------------------

print("\nLoading training data with max-K stable features...")
train_df = load_panel(
    years=TRAIN_YEARS,
    feature_cols=selected_max_features,
    exclude_dates=train_exclude_dates,
)

print(f"Training rows: {len(train_df):,}")

print("\nLoading validation data with max-K stable features...")
valid_df = load_panel(
    years=VALID_YEARS,
    feature_cols=selected_max_features,
    exclude_dates=valid_exclude_dates,
)

print(f"Validation rows: {len(valid_df):,}")


# ------------------------------------------------------------
# Train controlled top-K experiments.
# ------------------------------------------------------------

all_summary_rows = []

for k in TOP_K_LIST:
    feature_cols_k = feature_ranking.head(k)["feature"].tolist()

    used_features_k_path = OUT_DIR / f"used_features_top{k}.csv"
    pd.DataFrame({"feature": feature_cols_k}).to_csv(used_features_k_path, index=False)

    for config in LGBM_CONFIGS:
        experiment_name = f"top{k}_{config['config_name']}"
        experiment_dir = OUT_DIR / experiment_name

        summary_row = train_one_lgbm(
            train_df=train_df,
            valid_df=valid_df,
            feature_cols=feature_cols_k,
            config=config,
            experiment_dir=experiment_dir,
        )

        all_summary_rows.append(summary_row)

        print("\nFinished experiment:")
        print(pd.DataFrame([summary_row])[[
            "experiment",
            "n_features",
            "best_iteration",
            "daily_rank_ic_mean",
            "daily_rank_ic_tstat",
            "daily_rank_ic_positive_frac",
            "daily_pearson_ic_mean",
            "daily_pearson_ic_tstat",
        ]].to_string(index=False))


# ------------------------------------------------------------
# Save combined summary.
# ------------------------------------------------------------

summary_df = pd.DataFrame(all_summary_rows)

summary_df = summary_df.sort_values(
    ["daily_rank_ic_mean", "daily_rank_ic_tstat"],
    ascending=[False, False],
)

summary_path = OUT_DIR / "lgbm_topk_experiment_summary.csv"
summary_df.to_csv(summary_path, index=False)

print(f"\nSaved combined experiment summary to: {summary_path}")

print("\nTop experiments by validation daily Rank IC:")
print(
    summary_df[
        [
            "experiment",
            "n_features",
            "best_iteration",
            "overall_spearman",
            "daily_rank_ic_mean",
            "daily_rank_ic_std",
            "daily_rank_ic_tstat",
            "daily_rank_ic_positive_frac",
            "daily_pearson_ic_mean",
            "daily_pearson_ic_tstat",
        ]
    ]
    .head(20)
    .to_string(index=False)
)


# ------------------------------------------------------------
# Cleanup.
# ------------------------------------------------------------

del train_df, valid_df
gc.collect()

print("\nFinished controlled LightGBM top-K stable-feature experiments.")
print(f"All outputs saved under: {OUT_DIR}")