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

BACKTEST_PANEL_GLOB = ROOT / "data/clean_parquet/backtesting_model_panel/**/*.parquet"

FINAL_MODEL_DIR = ROOT / "model_outputs/lgbm_final_top20_train_2019_2023_for_backtest"
MODEL_PATH = FINAL_MODEL_DIR / "lgbm_final_top20_train_2019_2023_model.txt"
FEATURE_COLS_CSV = FINAL_MODEL_DIR / "used_top20_feature_columns.csv"

OUT_DIR = ROOT / "backtest_result/lgbm_final_top20_train_2019_2023_test_2024_2025"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Backtest settings
# ============================================================

TEST_YEARS = [2024, 2025]

TARGET_COL = "target_5d_cs_zscore"

RETURN_COLS = [
    "bt_5d_return_zero_after_missing",
    "bt_5d_return_delist_stress_30",
    "bt_5d_return_delist_stress_100",
]

DIAGNOSTIC_COLS = [
    "target_has_missing_return",
    "target_first_missing_return_pos",
    "target_first_missing_return_flag",
    "target_missing_return_flag_set",
    "target_has_delisting_missing_flag",
    "target_n_valid_forward_returns",
]

ID_COLS = [
    "permno",
    "dlycaldt",
    "year",
    "ticker",
    "primaryexch",
    "siccd",
    "naics",
    "icbindustry",
]

STRATEGIES = ["long_short", "long_only"]
ONE_WAY_COST_BPS_LIST = [0, 5, 10, 20]

TOP_QUANTILE = 0.10
HOLDING_DAYS = 5

# Portfolio exposure convention:
# long-short: total gross = 1.0, long = +0.5, short = -0.5
# long-only:  total gross = 1.0, long = +1.0
TOTAL_GROSS_EXPOSURE = 1.0
SLEEVE_GROSS_EXPOSURE = TOTAL_GROSS_EXPOSURE / HOLDING_DAYS

INITIAL_NAV = 1.0
INITIAL_CAPITAL_USD = 1_000_000

# Exclude the final 5 signal dates of 2025 so every opened sleeve exits
# inside the 2024--2025 backtest window.
EXCLUDE_LAST_N_SIGNAL_DATES = True

# Do not form trades on rows where the future return window is missing only
# because there is no future row in the dataset.
EXCLUDE_NO_FUTURE_ROW = True

ANNUALIZATION_DAYS = 252


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

def format_year_filter(years: list[int]) -> str:
    return "(" + ", ".join(str(y) for y in years) + ")"


def load_feature_columns() -> list[str]:
    if not FEATURE_COLS_CSV.exists():
        raise FileNotFoundError(f"Cannot find feature file: {FEATURE_COLS_CSV}")

    df = pd.read_csv(FEATURE_COLS_CSV)

    if "feature" not in df.columns:
        raise ValueError(f"{FEATURE_COLS_CSV} must contain column 'feature'.")

    features = df["feature"].dropna().astype(str).tolist()

    # Preserve order and remove duplicates.
    out = []
    seen = set()
    for f in features:
        if f not in seen:
            out.append(f)
            seen.add(f)

    return out


def get_schema_columns() -> set[str]:
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{BACKTEST_PANEL_GLOB}', hive_partitioning=true)
    """).df()

    return set(schema["column_name"].tolist())


def load_backtest_panel(feature_cols: list[str]) -> pd.DataFrame:
    selected_cols = (
        ID_COLS
        + [TARGET_COL]
        + RETURN_COLS
        + DIAGNOSTIC_COLS
        + feature_cols
    )

    # Preserve order and remove duplicates.
    selected_unique = []
    seen = set()
    for c in selected_cols:
        if c not in seen:
            selected_unique.append(c)
            seen.add(c)

    select_sql = ",\n            ".join(selected_unique)
    year_sql = format_year_filter(TEST_YEARS)

    df = con.execute(f"""
        SELECT
            {select_sql}
        FROM read_parquet('{BACKTEST_PANEL_GLOB}', hive_partitioning=true)
        WHERE year IN {year_sql}
        ORDER BY dlycaldt, permno
    """).df()

    df["dlycaldt"] = pd.to_datetime(df["dlycaldt"])

    for col in [TARGET_COL] + RETURN_COLS + feature_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    for col in DIAGNOSTIC_COLS:
        if col not in df.columns:
            continue
        if col in {
            "target_has_missing_return",
            "target_has_delisting_missing_flag",
            "target_n_valid_forward_returns",
            "target_first_missing_return_pos",
        }:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def add_predictions(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Cannot find model: {MODEL_PATH}")

    model = lgb.Booster(model_file=str(MODEL_PATH))

    X = df[feature_cols].to_numpy(dtype=np.float32, copy=True)

    pred = model.predict(X).astype(np.float32)

    out = df[
        ID_COLS
        + [TARGET_COL]
        + RETURN_COLS
        + DIAGNOSTIC_COLS
    ].copy()

    out["pred_score"] = pred

    del X
    gc.collect()

    return out


def compute_test_ic(pred_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
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
            "year": int(g["year"].iloc[0]),
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

    valid_overall = pred_df[[TARGET_COL, "pred_score"]].dropna()

    summary = {
        "n_prediction_rows": len(pred_df),
        "n_ic_rows": len(valid_overall),
        "n_unique_dates": pred_df["dlycaldt"].nunique(),
        "overall_spearman": valid_overall["pred_score"].corr(
            valid_overall[TARGET_COL],
            method="spearman",
        ),
        "overall_pearson": valid_overall["pred_score"].corr(
            valid_overall[TARGET_COL],
            method="pearson",
        ),
    }

    summary.update(summarize(rank_x, "daily_rank_ic"))
    summary.update(summarize(pearson_x, "daily_pearson_ic"))

    return daily_ic, pd.DataFrame([summary])


def compute_drawdown(nav: pd.Series) -> pd.Series:
    running_max = nav.cummax()
    return nav / running_max - 1.0


def summarize_daily_returns(
    daily_df: pd.DataFrame,
    strategy: str,
    return_col: str,
    cost_bps: float,
    ic_summary: pd.DataFrame,
    sleeve_df: pd.DataFrame,
) -> dict:
    x = daily_df["net_return"].astype(float)
    gross_x = daily_df["gross_return"].astype(float)

    n_days = len(daily_df)

    final_nav = float(daily_df["nav"].iloc[-1]) if n_days > 0 else np.nan
    final_gross_nav = float(daily_df["gross_nav"].iloc[-1]) if n_days > 0 else np.nan

    cumulative_return = final_nav - 1.0 if n_days > 0 else np.nan
    gross_cumulative_return = final_gross_nav - 1.0 if n_days > 0 else np.nan

    daily_mean = float(x.mean()) if n_days > 0 else np.nan
    daily_std = float(x.std(ddof=1)) if n_days > 1 else np.nan

    ann_vol = daily_std * np.sqrt(ANNUALIZATION_DAYS) if daily_std and daily_std > 0 else np.nan
    sharpe = (
        daily_mean / daily_std * np.sqrt(ANNUALIZATION_DAYS)
        if daily_std and daily_std > 0
        else np.nan
    )

    ann_return = (
        final_nav ** (ANNUALIZATION_DAYS / n_days) - 1.0
        if n_days > 0 and final_nav > 0
        else np.nan
    )

    max_drawdown = float(daily_df["drawdown"].min()) if n_days > 0 else np.nan
    max_drawdown_abs = abs(max_drawdown) if pd.notna(max_drawdown) else np.nan

    calmar = (
        ann_return / max_drawdown_abs
        if pd.notna(ann_return) and max_drawdown_abs and max_drawdown_abs > 0
        else np.nan
    )

    hit_rate = float((x > 0).mean()) if n_days > 0 else np.nan

    ic_row = ic_summary.iloc[0].to_dict()

    out = {
        "strategy": strategy,
        "return_col": return_col,
        "one_way_cost_bps": cost_bps,

        "n_days": n_days,
        "n_signal_days": int(sleeve_df["signal_date"].nunique()) if len(sleeve_df) else 0,
        "n_sleeves": int(len(sleeve_df)),
        "avg_n_long": float(sleeve_df["n_long"].mean()) if len(sleeve_df) else np.nan,
        "avg_n_short": float(sleeve_df["n_short"].mean()) if len(sleeve_df) else np.nan,

        "initial_nav": INITIAL_NAV,
        "final_nav": final_nav,
        "cumulative_return": cumulative_return,
        "gross_cumulative_return_before_cost": gross_cumulative_return,

        "pnl_per_1_nav": cumulative_return,
        "pnl_usd_initial_capital": cumulative_return * INITIAL_CAPITAL_USD,

        "annualized_return": ann_return,
        "annualized_volatility": ann_vol,
        "sharpe": sharpe,
        "calmar": calmar,

        "max_drawdown": max_drawdown,
        "max_drawdown_abs": max_drawdown_abs,

        "daily_mean_return": daily_mean,
        "daily_std_return": daily_std,
        "daily_hit_rate": hit_rate,

        "avg_daily_gross_return_before_cost": float(gross_x.mean()) if n_days else np.nan,
        "avg_daily_turnover": float(daily_df["turnover"].mean()) if n_days else np.nan,
        "total_turnover": float(daily_df["turnover"].sum()) if n_days else np.nan,
        "avg_daily_cost": float(daily_df["cost_return"].mean()) if n_days else np.nan,
        "total_cost": float(daily_df["cost_return"].sum()) if n_days else np.nan,

        "initial_capital_usd": INITIAL_CAPITAL_USD,
        "top_quantile": TOP_QUANTILE,
        "holding_days": HOLDING_DAYS,
        "total_gross_exposure": TOTAL_GROSS_EXPOSURE,
        "sleeve_gross_exposure": SLEEVE_GROSS_EXPOSURE,
    }

    # Add test IC metrics.
    out.update({
        "test_overall_spearman_target_cs": ic_row.get("overall_spearman"),
        "test_overall_pearson_target_cs": ic_row.get("overall_pearson"),
        "test_daily_rank_ic_mean": ic_row.get("daily_rank_ic_mean"),
        "test_daily_rank_ic_std": ic_row.get("daily_rank_ic_std"),
        "test_daily_rank_ic_tstat": ic_row.get("daily_rank_ic_tstat"),
        "test_daily_rank_ic_positive_frac": ic_row.get("daily_rank_ic_positive_frac"),
        "test_daily_pearson_ic_mean": ic_row.get("daily_pearson_ic_mean"),
        "test_daily_pearson_ic_tstat": ic_row.get("daily_pearson_ic_tstat"),
    })

    return out


def build_strategy_sleeves(
    pred_df: pd.DataFrame,
    strategy: str,
    return_col: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if strategy not in {"long_short", "long_only"}:
        raise ValueError(f"Unknown strategy: {strategy}")

    df = pred_df.copy()

    if EXCLUDE_NO_FUTURE_ROW:
        flag = df["target_missing_return_flag_set"].fillna("").astype(str)
        df = df[~flag.str.contains("NO_FUTURE_ROW", regex=False)].copy()

    df = df[
        df["pred_score"].notna()
        & df[return_col].notna()
    ].copy()

    all_dates = sorted(df["dlycaldt"].dropna().unique())

    if len(all_dates) <= HOLDING_DAYS:
        raise ValueError("Not enough dates for holding-period backtest.")

    if EXCLUDE_LAST_N_SIGNAL_DATES:
        signal_dates = all_dates[:-HOLDING_DAYS]
    else:
        signal_dates = all_dates

    date_to_exit = {
        all_dates[i]: all_dates[i + HOLDING_DAYS]
        for i in range(len(all_dates) - HOLDING_DAYS)
    }

    signal_date_set = set(signal_dates)

    sleeve_records = []

    for date, g in df.groupby("dlycaldt", sort=True):
        if date not in signal_date_set:
            continue
        if date not in date_to_exit:
            continue

        g = g[["permno", "pred_score", return_col]].dropna()
        n = len(g)

        if n < 20:
            continue

        n_select = int(np.floor(n * TOP_QUANTILE))
        n_select = max(n_select, 1)

        g_sorted = g.sort_values("pred_score", ascending=True)

        short_leg = g_sorted.head(n_select)
        long_leg = g_sorted.tail(n_select)

        exit_date = date_to_exit[date]

        if strategy == "long_short":
            side_exposure = SLEEVE_GROSS_EXPOSURE / 2.0

            long_mean_ret = float(long_leg[return_col].mean())
            short_mean_ret = float(short_leg[return_col].mean())

            sleeve_gross_return = (
                side_exposure * long_mean_ret
                - side_exposure * short_mean_ret
            )

            n_long = len(long_leg)
            n_short = len(short_leg)

        else:
            long_mean_ret = float(long_leg[return_col].mean())
            short_mean_ret = np.nan

            sleeve_gross_return = SLEEVE_GROSS_EXPOSURE * long_mean_ret

            n_long = len(long_leg)
            n_short = 0

        sleeve_records.append({
            "strategy": strategy,
            "return_col": return_col,
            "signal_date": pd.Timestamp(date),
            "exit_date": pd.Timestamp(exit_date),
            "n_universe": n,
            "n_long": n_long,
            "n_short": n_short,
            "long_mean_5d_return": long_mean_ret,
            "short_mean_5d_return": short_mean_ret,
            "sleeve_gross_exposure": SLEEVE_GROSS_EXPOSURE,
            "sleeve_gross_return_before_cost": sleeve_gross_return,
        })

    sleeve_df = pd.DataFrame(sleeve_records)

    # Build daily gross returns and turnover before costs.
    daily = pd.DataFrame({"dlycaldt": pd.to_datetime(all_dates)})
    daily["gross_return"] = 0.0
    daily["open_turnover"] = 0.0
    daily["close_turnover"] = 0.0

    if not sleeve_df.empty:
        exit_pnl = (
            sleeve_df.groupby("exit_date")["sleeve_gross_return_before_cost"]
            .sum()
            .rename("gross_return_from_exiting_sleeves")
            .reset_index()
            .rename(columns={"exit_date": "dlycaldt"})
        )

        open_turnover = (
            sleeve_df.groupby("signal_date")["sleeve_gross_exposure"]
            .sum()
            .rename("open_turnover")
            .reset_index()
            .rename(columns={"signal_date": "dlycaldt"})
        )

        close_turnover = (
            sleeve_df.groupby("exit_date")["sleeve_gross_exposure"]
            .sum()
            .rename("close_turnover")
            .reset_index()
            .rename(columns={"exit_date": "dlycaldt"})
        )

        daily = daily.merge(exit_pnl, on="dlycaldt", how="left")
        daily = daily.merge(open_turnover, on="dlycaldt", how="left", suffixes=("", "_new"))
        daily = daily.merge(close_turnover, on="dlycaldt", how="left", suffixes=("", "_new"))

        daily["gross_return"] = daily["gross_return_from_exiting_sleeves"].fillna(0.0)

        if "open_turnover_new" in daily.columns:
            daily["open_turnover"] = daily["open_turnover_new"].fillna(0.0)
            daily = daily.drop(columns=["open_turnover_new"])

        if "close_turnover_new" in daily.columns:
            daily["close_turnover"] = daily["close_turnover_new"].fillna(0.0)
            daily = daily.drop(columns=["close_turnover_new"])

        daily = daily.drop(columns=["gross_return_from_exiting_sleeves"])

    daily["open_turnover"] = daily["open_turnover"].fillna(0.0)
    daily["close_turnover"] = daily["close_turnover"].fillna(0.0)
    daily["turnover"] = daily["open_turnover"] + daily["close_turnover"]

    return sleeve_df, daily


def apply_transaction_cost_and_nav(
    daily_base: pd.DataFrame,
    cost_bps: float,
) -> pd.DataFrame:
    out = daily_base.copy()
    cost_rate = cost_bps / 10000.0

    out["one_way_cost_bps"] = cost_bps
    out["cost_return"] = out["turnover"] * cost_rate
    out["net_return"] = out["gross_return"] - out["cost_return"]

    out["gross_nav"] = INITIAL_NAV * (1.0 + out["gross_return"]).cumprod()
    out["nav"] = INITIAL_NAV * (1.0 + out["net_return"]).cumprod()
    out["drawdown"] = compute_drawdown(out["nav"])

    return out


# ============================================================
# Main
# ============================================================

print("\nLoading final model feature columns...")
feature_cols = load_feature_columns()

schema_cols = get_schema_columns()
missing_cols = [c for c in feature_cols if c not in schema_cols]
if missing_cols:
    raise ValueError(f"Missing model features in backtesting panel: {missing_cols}")

required_cols = set(ID_COLS + [TARGET_COL] + RETURN_COLS + DIAGNOSTIC_COLS)
missing_required = [c for c in required_cols if c not in schema_cols]
if missing_required:
    raise ValueError(f"Missing required columns in backtesting panel: {missing_required}")

print(f"Number of model features: {len(feature_cols)}")

with open(OUT_DIR / "backtest_config.json", "w") as f:
    json.dump(
        {
            "test_years": TEST_YEARS,
            "target_col_for_ic": TARGET_COL,
            "return_cols": RETURN_COLS,
            "strategies": STRATEGIES,
            "one_way_cost_bps_list": ONE_WAY_COST_BPS_LIST,
            "top_quantile": TOP_QUANTILE,
            "holding_days": HOLDING_DAYS,
            "total_gross_exposure": TOTAL_GROSS_EXPOSURE,
            "sleeve_gross_exposure": SLEEVE_GROSS_EXPOSURE,
            "initial_nav": INITIAL_NAV,
            "initial_capital_usd": INITIAL_CAPITAL_USD,
            "exclude_last_n_signal_dates": EXCLUDE_LAST_N_SIGNAL_DATES,
            "exclude_no_future_row": EXCLUDE_NO_FUTURE_ROW,
            "model_path": str(MODEL_PATH),
            "feature_cols_csv": str(FEATURE_COLS_CSV),
        },
        f,
        indent=2,
    )


print("\nLoading backtesting panel...")
panel = load_backtest_panel(feature_cols)

print(f"Loaded rows: {len(panel):,}")
print(f"Date range: {panel['dlycaldt'].min()} to {panel['dlycaldt'].max()}")


print("\nGenerating model predictions...")
pred_df = add_predictions(panel, feature_cols)

del panel
gc.collect()

prediction_path = OUT_DIR / "predictions_2024_2025.parquet"
prediction_csv_gz_path = OUT_DIR / "predictions_2024_2025.csv.gz"

pred_df.to_parquet(prediction_path, index=False)
pred_df.to_csv(prediction_csv_gz_path, index=False, compression="gzip")

print(f"Saved predictions to: {prediction_path}")
print(f"Saved compressed predictions to: {prediction_csv_gz_path}")


# ------------------------------------------------------------
# Test IC
# ------------------------------------------------------------

print("\nComputing test IC using target_5d_cs_zscore...")
daily_ic, ic_summary = compute_test_ic(pred_df)

daily_ic_path = OUT_DIR / "test_daily_ic.csv"
ic_summary_path = OUT_DIR / "test_ic_summary.csv"

daily_ic.to_csv(daily_ic_path, index=False)
ic_summary.to_csv(ic_summary_path, index=False)

print("\nTest IC summary:")
print(ic_summary.to_string(index=False))
print(f"Saved daily IC to: {daily_ic_path}")
print(f"Saved IC summary to: {ic_summary_path}")


# ------------------------------------------------------------
# Backtest cases
# ------------------------------------------------------------

all_daily_returns = []
all_sleeves = []
summary_rows = []

print("\nRunning portfolio backtests...")

for strategy in STRATEGIES:
    for return_col in RETURN_COLS:
        print(f"\nBuilding base sleeves: strategy={strategy}, return_col={return_col}")

        sleeve_df, daily_base = build_strategy_sleeves(
            pred_df=pred_df,
            strategy=strategy,
            return_col=return_col,
        )

        sleeve_df["case_strategy"] = strategy
        sleeve_df["case_return_col"] = return_col
        all_sleeves.append(sleeve_df)

        for cost_bps in ONE_WAY_COST_BPS_LIST:
            daily_case = apply_transaction_cost_and_nav(
                daily_base=daily_base,
                cost_bps=cost_bps,
            )

            daily_case["strategy"] = strategy
            daily_case["return_col"] = return_col
            daily_case["one_way_cost_bps"] = cost_bps

            all_daily_returns.append(daily_case)

            summary = summarize_daily_returns(
                daily_df=daily_case,
                strategy=strategy,
                return_col=return_col,
                cost_bps=cost_bps,
                ic_summary=ic_summary,
                sleeve_df=sleeve_df,
            )

            summary_rows.append(summary)

            print(
                f"  cost={cost_bps:>2} bps | "
                f"cumret={summary['cumulative_return']:.4f} | "
                f"sharpe={summary['sharpe']:.3f} | "
                f"maxDD={summary['max_drawdown']:.4f}"
            )


# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------

daily_returns = pd.concat(all_daily_returns, ignore_index=True)
sleeves = pd.concat(all_sleeves, ignore_index=True)
summary_df = pd.DataFrame(summary_rows)

summary_df = summary_df.sort_values(
    ["strategy", "return_col", "one_way_cost_bps"],
    ascending=[True, True, True],
)

daily_returns_path = OUT_DIR / "backtest_daily_returns.csv"
sleeves_path = OUT_DIR / "backtest_sleeves.csv"
summary_path = OUT_DIR / "backtest_summary.csv"

daily_returns.to_csv(daily_returns_path, index=False)
sleeves.to_csv(sleeves_path, index=False)
summary_df.to_csv(summary_path, index=False)

print(f"\nSaved daily returns to: {daily_returns_path}")
print(f"Saved sleeve-level records to: {sleeves_path}")
print(f"Saved summary to: {summary_path}")


print("\nBacktest summary:")
display_cols = [
    "strategy",
    "return_col",
    "one_way_cost_bps",
    "final_nav",
    "cumulative_return",
    "pnl_usd_initial_capital",
    "annualized_return",
    "annualized_volatility",
    "sharpe",
    "max_drawdown",
    "avg_daily_turnover",
    "total_cost",
    "test_daily_rank_ic_mean",
    "test_daily_rank_ic_tstat",
]

print(summary_df[display_cols].to_string(index=False))

print("\nFinished backtest.")
print(f"All outputs saved under: {OUT_DIR}")