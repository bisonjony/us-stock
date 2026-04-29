from pathlib import Path
import json
import gc

import duckdb
import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV


# ============================================================
# Paths
# ============================================================

ROOT = Path("/home/xul9527/us-stock")

MODEL_PANEL_GLOB = ROOT / "data/clean_parquet/model_panel/**/*.parquet"

SINGLE_FEATURE_YEARLY_IC = (
    ROOT / "data/single_feature_ic_analysis/single_feature_rank_ic_summary_by_year.csv"
)

OUT_DIR = ROOT / "model_outputs/ridge_top10_baseline_2019_2022_valid_2023"
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

TOP_K_FEATURES = 10

# Feature-selection filters.
MIN_DAYS_PER_YEAR = 100
MIN_TRAIN_YEARS = 3
MIN_SIGN_CONSISTENCY = 0.75

# Since target is t+1 to t+5, exclude the last 5 trading dates
# of the training and validation period to avoid boundary leakage.
FORWARD_HORIZON_DAYS = 5
DROP_LAST_N_DATES_AT_SPLIT_BOUNDARY = True

ALPHAS = np.logspace(-4, 4, 25)


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


def select_stable_features(schema_cols: set[str]) -> tuple[list[str], pd.DataFrame]:
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

        n_years = int(g["year"].nunique())

        n_pos = int((x > 0).sum())
        n_neg = int((x < 0).sum())
        sign_consistency_frac = max(n_pos, n_neg) / len(x)

        train_mean_ic = float(x.mean())
        train_abs_mean_ic = abs(train_mean_ic)
        train_mean_abs_year_ic = float(x.abs().mean())
        train_min_abs_year_ic = float(x.abs().min())

        avg_abs_tstat = float(t.abs().mean()) if len(t) > 0 else np.nan
        min_abs_tstat = float(t.abs().min()) if len(t) > 0 else np.nan

        # Stability score favors:
        #   - strong average IC over train years,
        #   - consistent sign,
        #   - statistically stronger annual IC.
        if np.isnan(avg_abs_tstat):
            stability_score = train_abs_mean_ic * sign_consistency_frac
        else:
            stability_score = (
                train_abs_mean_ic
                * sign_consistency_frac
                * np.log1p(avg_abs_tstat)
            )

        rows.append({
            "feature": feature,
            "n_train_years": n_years,
            "train_years_present": ",".join(map(str, sorted(g["year"].unique()))),
            "train_mean_ic": train_mean_ic,
            "train_abs_mean_ic": train_abs_mean_ic,
            "train_mean_abs_year_ic": train_mean_abs_year_ic,
            "train_min_abs_year_ic": train_min_abs_year_ic,
            "sign_consistency_frac": sign_consistency_frac,
            "avg_abs_tstat": avg_abs_tstat,
            "min_abs_tstat": min_abs_tstat,
            "stability_score": stability_score,
        })

    summary = pd.DataFrame(rows)

    if summary.empty:
        raise ValueError("No stable-feature summary could be computed.")

    # First apply stability filters.
    filtered = summary[
        (summary["n_train_years"] >= MIN_TRAIN_YEARS)
        & (summary["sign_consistency_frac"] >= MIN_SIGN_CONSISTENCY)
    ].copy()

    # If too few features pass strict filters, fall back to available features.
    if len(filtered) < TOP_K_FEATURES:
        print(
            f"Warning: only {len(filtered)} features passed strict stability filters. "
            "Falling back to top features by stability_score."
        )
        filtered = summary.copy()

    filtered = filtered.sort_values(
        [
            "stability_score",
            "train_abs_mean_ic",
            "train_mean_abs_year_ic",
            "avg_abs_tstat",
        ],
        ascending=[False, False, False, False],
    )

    selected = filtered.head(TOP_K_FEATURES).copy()
    selected_features = selected["feature"].tolist()

    # Save full ranking for inspection.
    full_rank = summary.sort_values(
        [
            "stability_score",
            "train_abs_mean_ic",
            "train_mean_abs_year_ic",
            "avg_abs_tstat",
        ],
        ascending=[False, False, False, False],
    )

    full_rank.to_csv(OUT_DIR / "feature_stability_full_ranking.csv", index=False)
    selected.to_csv(OUT_DIR / "selected_top10_features.csv", index=False)

    return selected_features, selected


def load_panel(
    years: list[int],
    feature_cols: list[str],
    exclude_dates: list[str] | None = None,
) -> pd.DataFrame:
    exclude_dates = exclude_dates or []

    selected_cols = ID_COLS + [TARGET_COL] + feature_cols

    # Deduplicate selected columns while preserving order.
    selected_unique = []
    seen = set()
    for c in selected_cols:
        if c not in seen:
            selected_unique.append(c)
            seen.add(c)

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

    for col in feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce").astype("float32")

    return df


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

    rank_x = daily_ic["rank_ic"].dropna()
    pearson_x = daily_ic["pearson_ic"].dropna()

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

    summary.update(summarize(rank_x, "daily_rank_ic"))
    summary.update(summarize(pearson_x, "daily_pearson_ic"))

    summary_df = pd.DataFrame([summary])

    return daily_ic, summary_df


# ============================================================
# Main
# ============================================================

print("\nChecking model panel schema...")
schema_cols = get_schema_columns()

if TARGET_COL not in schema_cols:
    raise ValueError(f"{TARGET_COL} not found in model_panel.")

print("\nSelecting top stable features using training years only...")
selected_features, selected_summary = select_stable_features(schema_cols)

print("\nSelected features:")
print(selected_summary.to_string(index=False))

with open(OUT_DIR / "split_config.json", "w") as f:
    json.dump(
        {
            "train_years": TRAIN_YEARS,
            "valid_years": VALID_YEARS,
            "top_k_features": TOP_K_FEATURES,
            "min_days_per_year": MIN_DAYS_PER_YEAR,
            "min_train_years": MIN_TRAIN_YEARS,
            "min_sign_consistency": MIN_SIGN_CONSISTENCY,
            "forward_horizon_days": FORWARD_HORIZON_DAYS,
        },
        f,
        indent=2,
    )


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

with open(OUT_DIR / "split_gap_info.json", "w") as f:
    json.dump(
        {
            "train_excluded_dates": train_exclude_dates,
            "valid_excluded_dates": valid_exclude_dates,
        },
        f,
        indent=2,
    )

print("\nExcluded dates:")
print(f"Train: {train_exclude_dates}")
print(f"Valid: {valid_exclude_dates}")


# ------------------------------------------------------------
# Load train and validation data
# ------------------------------------------------------------

print("\nLoading training data...")
train_df = load_panel(
    TRAIN_YEARS,
    selected_features,
    exclude_dates=train_exclude_dates,
)

print(f"Training rows: {len(train_df):,}")

print("\nLoading validation data...")
valid_df = load_panel(
    VALID_YEARS,
    selected_features,
    exclude_dates=valid_exclude_dates,
)

print(f"Validation rows: {len(valid_df):,}")


# ------------------------------------------------------------
# Prepare arrays
# ------------------------------------------------------------

X_train = train_df[selected_features].to_numpy(dtype=np.float32, copy=True)
y_train = train_df[TARGET_COL].to_numpy(dtype=np.float32, copy=True)

valid_meta = valid_df[ID_COLS + [TARGET_COL]].copy()
X_valid = valid_df[selected_features].to_numpy(dtype=np.float32, copy=True)
y_valid = valid_df[TARGET_COL].to_numpy(dtype=np.float32, copy=True)

del train_df, valid_df
gc.collect()


# ------------------------------------------------------------
# Train RidgeCV
# ------------------------------------------------------------

print("\nTraining RidgeCV baseline...")

model = Pipeline(
    steps=[
        # For cross-sectionally standardized features, 0 is neutral.
        # This also handles small residual missingness safely.
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        # Ridge benefits from comparable feature scale, especially if
        # binary flags are selected among the top features.
        ("scaler", StandardScaler()),
        ("ridge", RidgeCV(alphas=ALPHAS, scoring="neg_mean_squared_error")),
    ]
)

model.fit(X_train, y_train)

ridge = model.named_steps["ridge"]

model_info = {
    "model": "RidgeCV",
    "selected_alpha": float(ridge.alpha_),
    "alphas": [float(a) for a in ALPHAS],
    "n_features": len(selected_features),
    "features": selected_features,
}

with open(OUT_DIR / "ridge_model_info.json", "w") as f:
    json.dump(model_info, f, indent=2)

print(f"Selected alpha: {ridge.alpha_}")


# ------------------------------------------------------------
# Save coefficients
# ------------------------------------------------------------

coef_df = pd.DataFrame({
    "feature": selected_features,
    "coef_standardized": ridge.coef_,
})

coef_df["abs_coef"] = coef_df["coef_standardized"].abs()
coef_df = coef_df.sort_values("abs_coef", ascending=False)

coef_path = OUT_DIR / "ridge_coefficients.csv"
coef_df.to_csv(coef_path, index=False)

print(f"Saved coefficients to: {coef_path}")


# ------------------------------------------------------------
# Predict validation
# ------------------------------------------------------------

print("\nPredicting validation set...")

pred_valid = model.predict(X_valid).astype(np.float32)

valid_pred_df = valid_meta.copy()
valid_pred_df["pred_score"] = pred_valid

pred_path = OUT_DIR / "validation_predictions.parquet"
pred_csv_gz_path = OUT_DIR / "validation_predictions.csv.gz"

valid_pred_df.to_parquet(pred_path, index=False)
valid_pred_df.to_csv(pred_csv_gz_path, index=False, compression="gzip")

print(f"Saved validation predictions to: {pred_path}")
print(f"Saved compressed validation predictions to: {pred_csv_gz_path}")


# ------------------------------------------------------------
# IC evaluation
# ------------------------------------------------------------

print("\nComputing validation IC...")

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
# Save train/valid size summary
# ------------------------------------------------------------

size_summary = pd.DataFrame([{
    "n_train_rows": len(X_train),
    "n_valid_rows": len(X_valid),
    "n_features": len(selected_features),
    "selected_alpha": float(ridge.alpha_),
}])

size_summary_path = OUT_DIR / "train_valid_size_summary.csv"
size_summary.to_csv(size_summary_path, index=False)

print(f"Saved size summary to: {size_summary_path}")


# ------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------

del X_train, y_train, X_valid, y_valid, valid_pred_df
gc.collect()

print("\nFinished Ridge top-feature baseline.")
print(f"All outputs saved under: {OUT_DIR}")