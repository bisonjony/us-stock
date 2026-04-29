from pathlib import Path
import json
import gc

import duckdb
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

OUT_DIR = ROOT / "model_outputs/feature_score_baselines_2019_2022_valid_2023"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Settings
# ============================================================

TRAIN_YEARS = [2019, 2020, 2021, 2022]
VALID_YEARS = [2023]

TARGET_COL = "target_5d_cs_zscore"
ID_COLS = ["permno", "dlycaldt", "year"]

# Feature-score baselines to evaluate.
TOP_K_LIST = [1, 3, 5, 10]

# Stability-selection filters.
MIN_DAYS_PER_YEAR = 100
MIN_TRAIN_YEARS = 3
MIN_SIGN_CONSISTENCY = 0.75

# Since target is t+1 to t+5, exclude last 5 trading dates
# before train/validation boundaries.
FORWARD_HORIZON_DAYS = 5
DROP_LAST_N_DATES_AT_SPLIT_BOUNDARY = True

# Use only continuous transformed features for signed-average scores.
# This avoids mixing raw binary flags with standardized continuous features.
FEATURE_SUFFIX = "_cs_winsor_zscore"


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

def get_schema_columns() -> set[str]:
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    """).df()
    return set(schema["column_name"].tolist())


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
    date_sql = ", ".join(f"DATE '{d}'" for d in dates)
    return f"AND dlycaldt NOT IN ({date_sql})"


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

        direction = 1.0 if train_mean_ic >= 0 else -1.0

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
            "direction": direction,
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
            "Falling back to all eligible features sorted by stability_score."
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

    # Deduplicate columns while preserving order.
    selected_unique = []
    seen = set()
    for col in selected_cols:
        if col not in seen:
            selected_unique.append(col)
            seen.add(col)

    select_sql = ",\n            ".join(selected_unique)
    year_sql = format_year_filter(years)
    date_exclusion_sql = format_date_exclusion(exclude_dates)

    df = con.execute(f"""
        SELECT
            {select_sql}
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        WHERE year IN {year_sql}
          AND {TARGET_COL} IS NOT NULL
          {date_exclusion_sql}
        ORDER BY dlycaldt, permno
    """).df()

    df["dlycaldt"] = pd.to_datetime(df["dlycaldt"])
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce").astype("float32")

    for col in feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    return df


def add_signed_feature_scores(
    df: pd.DataFrame,
    feature_ranking: pd.DataFrame,
    top_k_list: list[int],
) -> tuple[pd.DataFrame, list[str]]:
    out = df[ID_COLS + [TARGET_COL]].copy()

    pred_cols = []

    for k in top_k_list:
        selected = feature_ranking.head(k)

        features = selected["feature"].tolist()
        directions = selected["direction"].to_numpy(dtype=np.float32)

        X = df[features].to_numpy(dtype=np.float32, copy=True)

        # Missing standardized feature value is treated as neutral.
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

        signed_score = (X * directions.reshape(1, -1)).mean(axis=1)

        col_name = f"score_top{k}_signed_avg"
        out[col_name] = signed_score.astype("float32")
        pred_cols.append(col_name)

    return out, pred_cols


def compute_daily_ic_for_scores(
    pred_df: pd.DataFrame,
    pred_cols: list[str],
    dataset_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_records = []

    for pred_col in pred_cols:
        for date, g in pred_df.groupby("dlycaldt", sort=True):
            gg = g[[TARGET_COL, pred_col]].dropna()
            n = len(gg)

            if n < 10:
                rank_ic = np.nan
                pearson_ic = np.nan
            elif gg[TARGET_COL].nunique() <= 1 or gg[pred_col].nunique() <= 1:
                rank_ic = np.nan
                pearson_ic = np.nan
            else:
                rank_ic = gg[pred_col].corr(gg[TARGET_COL], method="spearman")
                pearson_ic = gg[pred_col].corr(gg[TARGET_COL], method="pearson")

            daily_records.append({
                "dataset": dataset_name,
                "baseline": pred_col,
                "dlycaldt": date,
                "year": int(g["year"].iloc[0]),
                "n": n,
                "rank_ic": rank_ic,
                "pearson_ic": pearson_ic,
            })

    daily_ic = pd.DataFrame(daily_records)

    summary_records = []

    for baseline, g in daily_ic.groupby("baseline", sort=False):
        rank_x = g["rank_ic"].dropna()
        pearson_x = g["pearson_ic"].dropna()

        def summarize(x: pd.Series, prefix: str) -> dict:
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

        pred_col = baseline
        base_df = pred_df[[TARGET_COL, pred_col]].dropna()

        row = {
            "dataset": dataset_name,
            "baseline": baseline,
            "n_prediction_rows": len(base_df),
            "n_unique_dates": pred_df["dlycaldt"].nunique(),
            "overall_spearman": base_df[pred_col].corr(
                base_df[TARGET_COL], method="spearman"
            ),
            "overall_pearson": base_df[pred_col].corr(
                base_df[TARGET_COL], method="pearson"
            ),
        }

        row.update(summarize(rank_x, "daily_rank_ic"))
        row.update(summarize(pearson_x, "daily_pearson_ic"))

        summary_records.append(row)

    summary = pd.DataFrame(summary_records)

    return daily_ic, summary


# ============================================================
# Main
# ============================================================

print("\nChecking model panel schema...")
schema_cols = get_schema_columns()

if TARGET_COL not in schema_cols:
    raise ValueError(f"{TARGET_COL} not found in model_panel.")

print("\nSelecting stable features using training years only...")
feature_ranking = select_stable_features(schema_cols)

ranking_path = OUT_DIR / "feature_stability_ranking_train_years.csv"
feature_ranking.to_csv(ranking_path, index=False)

print(f"Saved feature stability ranking to: {ranking_path}")

max_k = max(TOP_K_LIST)
selected_features = feature_ranking.head(max_k)["feature"].tolist()

selected_path = OUT_DIR / f"selected_top{max_k}_features.csv"
feature_ranking.head(max_k).to_csv(selected_path, index=False)

print("\nSelected top features:")
print(feature_ranking.head(max_k).to_string(index=False))


# ------------------------------------------------------------
# Split gap control
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

split_info = {
    "train_years": TRAIN_YEARS,
    "valid_years": VALID_YEARS,
    "top_k_list": TOP_K_LIST,
    "min_days_per_year": MIN_DAYS_PER_YEAR,
    "min_train_years": MIN_TRAIN_YEARS,
    "min_sign_consistency": MIN_SIGN_CONSISTENCY,
    "forward_horizon_days": FORWARD_HORIZON_DAYS,
    "train_excluded_dates": train_exclude_dates,
    "valid_excluded_dates": valid_exclude_dates,
    "feature_suffix": FEATURE_SUFFIX,
}

with open(OUT_DIR / "feature_score_baseline_config.json", "w") as f:
    json.dump(split_info, f, indent=2)

print("\nExcluded dates:")
print(f"Train: {train_exclude_dates}")
print(f"Valid: {valid_exclude_dates}")


# ------------------------------------------------------------
# Load train and validation panels
# ------------------------------------------------------------

print("\nLoading training panel for in-sample score diagnostics...")
train_df = load_panel(
    TRAIN_YEARS,
    selected_features,
    exclude_dates=train_exclude_dates,
)

print(f"Training rows: {len(train_df):,}")

print("\nLoading validation panel...")
valid_df = load_panel(
    VALID_YEARS,
    selected_features,
    exclude_dates=valid_exclude_dates,
)

print(f"Validation rows: {len(valid_df):,}")


# ------------------------------------------------------------
# Build signed feature-score baselines
# ------------------------------------------------------------

print("\nComputing signed average feature scores...")

train_pred, pred_cols = add_signed_feature_scores(
    train_df,
    feature_ranking,
    TOP_K_LIST,
)

valid_pred, _ = add_signed_feature_scores(
    valid_df,
    feature_ranking,
    TOP_K_LIST,
)

del train_df, valid_df
gc.collect()


# ------------------------------------------------------------
# Save predictions
# ------------------------------------------------------------

train_pred_path = OUT_DIR / "train_feature_score_predictions.parquet"
valid_pred_path = OUT_DIR / "validation_feature_score_predictions.parquet"

train_pred.to_parquet(train_pred_path, index=False)
valid_pred.to_parquet(valid_pred_path, index=False)

print(f"Saved train predictions to: {train_pred_path}")
print(f"Saved validation predictions to: {valid_pred_path}")


# ------------------------------------------------------------
# Evaluate IC
# ------------------------------------------------------------

print("\nComputing train IC...")
train_daily_ic, train_summary = compute_daily_ic_for_scores(
    train_pred,
    pred_cols,
    dataset_name="train_2019_2022",
)

print("\nComputing validation IC...")
valid_daily_ic, valid_summary = compute_daily_ic_for_scores(
    valid_pred,
    pred_cols,
    dataset_name="valid_2023",
)

daily_ic = pd.concat([train_daily_ic, valid_daily_ic], ignore_index=True)
summary = pd.concat([train_summary, valid_summary], ignore_index=True)

daily_ic_path = OUT_DIR / "feature_score_daily_ic.csv"
summary_path = OUT_DIR / "feature_score_ic_summary.csv"

daily_ic.to_csv(daily_ic_path, index=False)
summary.to_csv(summary_path, index=False)

print(f"Saved daily IC to: {daily_ic_path}")
print(f"Saved IC summary to: {summary_path}")


# ------------------------------------------------------------
# Print validation summary
# ------------------------------------------------------------

print("\nValidation IC summary:")
valid_cols = [
    "baseline",
    "n_prediction_rows",
    "n_unique_dates",
    "overall_spearman",
    "daily_rank_ic_mean",
    "daily_rank_ic_std",
    "daily_rank_ic_tstat",
    "daily_rank_ic_positive_frac",
    "daily_pearson_ic_mean",
    "daily_pearson_ic_tstat",
]

print(
    valid_summary[valid_cols]
    .sort_values("daily_rank_ic_mean", ascending=False)
    .to_string(index=False)
)


print("\nFinished feature-score baseline analysis.")
print(f"All outputs saved under: {OUT_DIR}")