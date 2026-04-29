from pathlib import Path
import shutil
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

UNIVERSE_GLOB = ROOT / "data/clean_parquet/daily_stock_universe/**/*.parquet"
BACKTEST_GLOB = ROOT / "data/clean_parquet/backtesting_data/**/*.parquet"
FEATURE_GLOB = ROOT / "data/clean_parquet/daily_features/**/*.parquet"

OUT_PANEL = ROOT / "data/clean_parquet/backtesting_model_panel"
OUT_ANALYSIS = ROOT / "data/clean_parquet/backtesting_model_panel_analysis"

if OUT_PANEL.exists():
    shutil.rmtree(OUT_PANEL)

OUT_PANEL.mkdir(parents=True, exist_ok=True)
OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=1")
con.execute("SET memory_limit='3GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")
con.execute("SET max_temp_directory_size='150GB'")


# ============================================================
# Columns
# ============================================================

ID_COLS_IN_FILE = [
    "permno",
    "dlycaldt",
    "ticker",
    "primaryexch",
    "siccd",
    "naics",
    "icbindustry",
]

# Complete-only label for IC evaluation.
# target_5d_cs_zscore is created inside this script using date-wise
# p01/p99 winsorization and median-IQR standardization.
IC_LABEL_COLS = [
    "target_5d_raw_complete_only",
    "target_5d_cs_zscore",
]

BACKTEST_RETURN_COLS = [
    "bt_5d_return_zero_after_missing",
    "bt_5d_return_delist_stress_30",
    "bt_5d_return_delist_stress_100",
]

# Keep these diagnostics for auditing and filtering during backtest.
# They are not model features.
BACKTEST_DIAGNOSTIC_COLS = [
    "target_has_missing_return",
    "target_first_missing_return_pos",
    "target_first_missing_return_flag",
    "target_missing_return_flag_set",
    "target_has_delisting_missing_flag",
    "target_n_valid_forward_returns",
]

BINARY_FEATURES = [
    "active_non_delisting_flag",
    "price_from_bidask_flag",
    "ohlc_missing_flag",
    "valid_ohlc_flag",
    "ohlc_inconsistent_flag",
    "bidask_missing_flag",
    "valid_bidask_flag",
    "crossed_quote_flag",
    "distribution_event_flag",
    "cash_distribution_event_flag",
    "split_distribution_event_flag",
    "ordinary_distribution_event_flag",
    "share_factor_event_flag",
    "strong_up_high_volume_flag",
    "strong_down_high_volume_flag",
]

CONTINUOUS_FEATURES = [
    # Universe-rank variables
    "market_cap_rank",
    "adv20_rank",

    # Basic engineered features
    "log_prc",
    "log_dlycap",
    "dollar_volume",
    "log_dollar_volume",

    # Excluded from first baseline due to high structural missingness:
    # "log_num_trades",
    # "log_market_maker_count",

    "bid_ask_spread",
    "hl_range",
    "open_close_ret",
    "turnover",
    "amihud_illiq",

    # Return history
    "ret_1d",
    "ret_2d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "ret_60d",
    "ret_120d",
    "ret_20_5",

    # Volatility and downside risk
    "vol_5d",
    "vol_10d",
    "vol_20d",
    "vol_60d",
    "skew_5d",
    "skew_10d",
    "skew_20d",
    "skew_60d",
    "max_ret_5d",
    "max_ret_10d",
    "max_ret_20d",
    "max_ret_60d",
    "min_ret_5d",
    "min_ret_10d",
    "min_ret_20d",
    "min_ret_60d",
    "downside_vol_5d",
    "downside_vol_10d",
    "downside_vol_20d",
    "downside_vol_60d",
    "hl_range_avg_5d",
    "hl_range_avg_10d",
    "hl_range_avg_20d",
    "hl_range_avg_60d",

    # Liquidity and volume
    "adv5",
    "adv20",
    "adv60",
    "avg_volume_5d",
    "avg_volume_20d",
    "avg_volume_60d",
    "volume_shock",
    "dollar_volume_shock",
    "turnover_avg_5d",
    "turnover_avg_20d",
    "turnover_avg_60d",
    "amihud_illiq_avg_5d",
    "amihud_illiq_avg_20d",
    "amihud_illiq_avg_60d",

    # Price pressure
    "ret_1d_x_volume_shock",
    "ret_5d_x_volume_shock",
    "signed_abnormal_volume",
    "signed_abnormal_volume_ratio",
    "signed_dollar_volume_shock",
    "up_high_volume_pressure",
    "down_high_volume_pressure",
]

ZERO_FILL_TRANSFORMED_FEATURES = {
    "up_high_volume_pressure_cs_winsor_zscore",
    "down_high_volume_pressure_cs_winsor_zscore",
}


# ============================================================
# SQL helpers
# ============================================================

FINITE_BOUND = "1e100"

id_sql = ",\n        ".join(ID_COLS_IN_FILE)

backtest_return_select_sql = ",\n        ".join(
    f"TRY_CAST({col} AS DOUBLE) AS {col}"
    for col in BACKTEST_RETURN_COLS
)

backtest_diagnostic_select_sql = ",\n        ".join(BACKTEST_DIAGNOSTIC_COLS)

binary_select_sql = ",\n            ".join(
    f"COALESCE(CAST(f.{col} AS INTEGER), 0) AS {col}"
    for col in BINARY_FEATURES
)

continuous_select_parts = [
    "CAST(u.market_cap_rank AS DOUBLE) AS market_cap_rank",
    "CAST(u.adv20_rank AS DOUBLE) AS adv20_rank",
]

for col in CONTINUOUS_FEATURES:
    if col not in {"market_cap_rank", "adv20_rank"}:
        continuous_select_parts.append(f"TRY_CAST(f.{col} AS DOUBLE) AS {col}")

continuous_select_sql = ",\n            ".join(continuous_select_parts)

clean_continuous_exprs = []
for col in CONTINUOUS_FEATURES:
    clean_continuous_exprs.append(f"""
        CASE
            WHEN {col} IS NOT NULL
             AND {col} BETWEEN -{FINITE_BOUND} AND {FINITE_BOUND}
            THEN {col}
            ELSE NULL
        END AS {col}
    """)

clean_continuous_sql = ",\n            ".join(clean_continuous_exprs)

feature_quantile_exprs = []
for col in CONTINUOUS_FEATURES:
    feature_quantile_exprs.extend([
        f"APPROX_QUANTILE({col}, 0.01) AS {col}_p01",
        f"APPROX_QUANTILE({col}, 0.25) AS {col}_p25",
        f"APPROX_QUANTILE({col}, 0.50) AS {col}_p50",
        f"APPROX_QUANTILE({col}, 0.75) AS {col}_p75",
        f"APPROX_QUANTILE({col}, 0.99) AS {col}_p99",
    ])

feature_quantile_sql = ",\n            ".join(feature_quantile_exprs)

feature_transform_exprs = []

for col in CONTINUOUS_FEATURES:
    clipped_expr = f"""
        CASE
            WHEN p.{col} IS NULL THEN NULL
            WHEN p.{col} < q.{col}_p01 THEN q.{col}_p01
            WHEN p.{col} > q.{col}_p99 THEN q.{col}_p99
            ELSE p.{col}
        END
    """

    robust_scale_expr = f"""
        NULLIF((q.{col}_p75 - q.{col}_p25) / 1.349, 0)
    """

    feature_transform_exprs.append(f"""
        CASE
            WHEN p.{col} IS NOT NULL
             AND q.{col}_p25 IS NOT NULL
             AND q.{col}_p75 IS NOT NULL
             AND q.{col}_p75 > q.{col}_p25
            THEN ({clipped_expr} - q.{col}_p50) / {robust_scale_expr}
            ELSE NULL
        END AS {col}_cs_winsor_zscore
    """)

feature_transform_sql = ",\n            ".join(feature_transform_exprs)

final_feature_cols = []
final_feature_cols.extend(BINARY_FEATURES)

for col in CONTINUOUS_FEATURES:
    final_feature_cols.append(f"{col}_cs_winsor_zscore")

final_feature_select_exprs = []

for col in final_feature_cols:
    if col in ZERO_FILL_TRANSFORMED_FEATURES:
        final_feature_select_exprs.append(f"COALESCE({col}, 0.0) AS {col}")
    else:
        final_feature_select_exprs.append(col)

final_feature_sql = ",\n        ".join(final_feature_select_exprs)


# ============================================================
# Year-by-year processing
# ============================================================

years = [
    row[0]
    for row in con.execute(f"""
        SELECT DISTINCT year
        FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
        ORDER BY year
    """).fetchall()
]

print(f"\nCreating backtesting model panel year by year: {years}")

for year in years:
    print(f"\nProcessing year {year}...")

    year_dir = OUT_PANEL / f"year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    out_file = year_dir / "part.parquet"

    if out_file.exists():
        out_file.unlink()

    con.execute(f"""
    COPY (
        WITH joined_panel AS (
            SELECT
                u.permno,
                u.dlycaldt,
                u.ticker,
                u.primaryexch,
                u.siccd,
                u.naics,
                u.icbindustry,

                TRY_CAST(l.target_5d_raw_complete_only AS DOUBLE)
                    AS target_5d_raw_complete_only,

                TRY_CAST(l.bt_5d_return_zero_after_missing AS DOUBLE)
                    AS bt_5d_return_zero_after_missing,
                TRY_CAST(l.bt_5d_return_delist_stress_30 AS DOUBLE)
                    AS bt_5d_return_delist_stress_30,
                TRY_CAST(l.bt_5d_return_delist_stress_100 AS DOUBLE)
                    AS bt_5d_return_delist_stress_100,

                l.target_has_missing_return,
                l.target_first_missing_return_pos,
                l.target_first_missing_return_flag,
                l.target_missing_return_flag_set,
                l.target_has_delisting_missing_flag,
                l.target_n_valid_forward_returns,

                {binary_select_sql},

                {continuous_select_sql}

            FROM (
                SELECT *
                FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
                WHERE year = {year}
            ) u

            LEFT JOIN (
                SELECT
                    permno,
                    dlycaldt,
                    target_5d_raw_complete_only,
                    bt_5d_return_zero_after_missing,
                    bt_5d_return_delist_stress_30,
                    bt_5d_return_delist_stress_100,
                    target_has_missing_return,
                    target_first_missing_return_pos,
                    target_first_missing_return_flag,
                    target_missing_return_flag_set,
                    target_has_delisting_missing_flag,
                    target_n_valid_forward_returns
                FROM read_parquet('{BACKTEST_GLOB}', hive_partitioning=true)
                WHERE year = {year}
            ) l
              ON u.permno = l.permno
             AND u.dlycaldt = l.dlycaldt

            LEFT JOIN (
                SELECT
                    permno,
                    dlycaldt,
                    {", ".join(BINARY_FEATURES)},
                    {", ".join([c for c in CONTINUOUS_FEATURES if c not in {"market_cap_rank", "adv20_rank"}])}
                FROM read_parquet('{FEATURE_GLOB}', hive_partitioning=true)
                WHERE year = {year}
            ) f
              ON u.permno = f.permno
             AND u.dlycaldt = f.dlycaldt
        ),

        clean_panel AS (
            SELECT
                {id_sql},

                CASE
                    WHEN target_5d_raw_complete_only IS NOT NULL
                     AND target_5d_raw_complete_only BETWEEN -{FINITE_BOUND} AND {FINITE_BOUND}
                    THEN target_5d_raw_complete_only
                    ELSE NULL
                END AS target_5d_raw_complete_only,

                {backtest_return_select_sql},

                {backtest_diagnostic_select_sql},

                {", ".join(BINARY_FEATURES)},

                {clean_continuous_sql}

            FROM joined_panel

            WHERE bt_5d_return_zero_after_missing IS NOT NULL
              AND bt_5d_return_delist_stress_30 IS NOT NULL
              AND bt_5d_return_delist_stress_100 IS NOT NULL
        ),

        target_quantiles AS (
            SELECT
                dlycaldt,
                APPROX_QUANTILE(target_5d_raw_complete_only, 0.01) AS target_5d_p01,
                APPROX_QUANTILE(target_5d_raw_complete_only, 0.25) AS target_5d_p25,
                APPROX_QUANTILE(target_5d_raw_complete_only, 0.50) AS target_5d_p50,
                APPROX_QUANTILE(target_5d_raw_complete_only, 0.75) AS target_5d_p75,
                APPROX_QUANTILE(target_5d_raw_complete_only, 0.99) AS target_5d_p99
            FROM clean_panel
            WHERE target_5d_raw_complete_only IS NOT NULL
            GROUP BY dlycaldt
        ),

        with_target AS (
            SELECT
                p.*,

                CASE
                    WHEN p.target_5d_raw_complete_only IS NOT NULL
                     AND q.target_5d_p25 IS NOT NULL
                     AND q.target_5d_p75 IS NOT NULL
                     AND q.target_5d_p75 > q.target_5d_p25
                    THEN
                        (
                            CASE
                                WHEN p.target_5d_raw_complete_only < q.target_5d_p01
                                    THEN q.target_5d_p01
                                WHEN p.target_5d_raw_complete_only > q.target_5d_p99
                                    THEN q.target_5d_p99
                                ELSE p.target_5d_raw_complete_only
                            END
                            - q.target_5d_p50
                        )
                        / NULLIF((q.target_5d_p75 - q.target_5d_p25) / 1.349, 0)
                    ELSE NULL
                END AS target_5d_cs_zscore

            FROM clean_panel p
            LEFT JOIN target_quantiles q
              ON p.dlycaldt = q.dlycaldt
        ),

        feature_quantiles AS (
            SELECT
                dlycaldt,
                {feature_quantile_sql}
            FROM with_target
            GROUP BY dlycaldt
        ),

        transformed AS (
            SELECT
                p.*,

                {feature_transform_sql}

            FROM with_target p
            LEFT JOIN feature_quantiles q
              ON p.dlycaldt = q.dlycaldt
        )

        SELECT
            {id_sql},

            target_5d_raw_complete_only,
            target_5d_cs_zscore,

            {", ".join(BACKTEST_RETURN_COLS)},

            {", ".join(BACKTEST_DIAGNOSTIC_COLS)},

            {final_feature_sql}

        FROM transformed
    )
    TO '{out_file}'
    (FORMAT PARQUET, COMPRESSION SNAPPY);
    """)

    print(f"Saved year {year} to: {out_file}")


# ============================================================
# Metadata
# ============================================================

feature_metadata = pd.DataFrame({
    "feature": final_feature_cols,
    "role": ["feature"] * len(final_feature_cols),
})

feature_metadata_path = OUT_ANALYSIS / "backtesting_model_panel_feature_columns.csv"
feature_metadata.to_csv(feature_metadata_path, index=False)

label_metadata = pd.DataFrame({
    "label_column": IC_LABEL_COLS,
    "description": [
        "Complete-only future 5-day raw return; non-missing only when all five future daily returns exist.",
        "Date-wise p01/p99 winsorized and median-IQR standardized complete-only future 5-day return. Used for test Rank IC evaluation only.",
    ],
})

label_metadata_path = OUT_ANALYSIS / "backtesting_model_panel_label_columns.csv"
label_metadata.to_csv(label_metadata_path, index=False)

return_metadata = pd.DataFrame({
    "return_column": BACKTEST_RETURN_COLS,
    "description": [
        "Main backtest 5-day return: compound valid returns; after first missing future return, assume zero return for the remaining horizon.",
        "Delisting stress return: if first missing return is delisting-related, apply -30% terminal return.",
        "Delisting stress return: if first missing return is delisting-related, apply -100% terminal return.",
    ],
})

return_metadata_path = OUT_ANALYSIS / "backtesting_model_panel_return_columns.csv"
return_metadata.to_csv(return_metadata_path, index=False)

diagnostic_metadata = pd.DataFrame({
    "diagnostic_column": BACKTEST_DIAGNOSTIC_COLS,
    "description": [
        "Indicator for whether any future return in the 5-day window is missing.",
        "Position of the first missing return in the 5-day forward window.",
        "CRSP missing-return flag for the first missing future return.",
        "Set of missing-return flags appearing in the 5-day forward window.",
        "Indicator for whether any missing future return is delisting-related.",
        "Number of non-missing future returns among the 5 forward observations.",
    ],
})

diagnostic_metadata_path = OUT_ANALYSIS / "backtesting_model_panel_diagnostic_columns.csv"
diagnostic_metadata.to_csv(diagnostic_metadata_path, index=False)

print(f"\nSaved feature metadata to: {feature_metadata_path}")
print(f"Saved label metadata to: {label_metadata_path}")
print(f"Saved return metadata to: {return_metadata_path}")
print(f"Saved diagnostic metadata to: {diagnostic_metadata_path}")
print("\nFinished creating backtesting model panel.")