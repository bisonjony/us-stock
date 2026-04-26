from pathlib import Path
import shutil
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

UNIVERSE_GLOB = ROOT / "data/clean_parquet/daily_stock_universe/**/*.parquet"
BACKTEST_GLOB = ROOT / "data/clean_parquet/backtesting_data/**/*.parquet"

OUT_PANEL = ROOT / "data/clean_parquet/model_panel"
OUT_ANALYSIS = ROOT / "data/clean_parquet/model_panel_analysis"

if OUT_PANEL.exists():
    shutil.rmtree(OUT_PANEL)

OUT_PANEL.parent.mkdir(parents=True, exist_ok=True)
OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=2")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")


# ---------------------------------------------------------------------
# Step 1. Merge daily universe with return labels.
#
# Universe source:
#   data/clean_parquet/daily_stock_universe
#
# Label source:
#   data/clean_parquet/backtesting_data
#
# We merge by (permno, dlycaldt). The label was created from the broader
# daily_core_dup_removed data, not from future universe membership.
# ---------------------------------------------------------------------

print("\nCreating model panel...")

con.execute(f"""
CREATE OR REPLACE TEMP VIEW joined_panel AS
SELECT
    u.*,

    l.target_1d_raw,
    l.target_5d_raw_complete_only,

    l.bt_5d_return_zero_after_missing,
    l.bt_5d_return_delist_stress_30,
    l.bt_5d_return_delist_stress_100,

    l.target_has_missing_return,
    l.target_first_missing_return_pos,
    l.target_first_missing_return_flag,
    l.target_missing_return_flag_set,
    l.target_has_delisting_missing_flag,
    l.target_n_valid_forward_returns,

    l.fwd_date_1,
    l.fwd_date_2,
    l.fwd_date_3,
    l.fwd_date_4,
    l.fwd_date_5,

    l.ret_1,
    l.ret_2,
    l.ret_3,
    l.ret_4,
    l.ret_5

FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true) u
LEFT JOIN read_parquet('{BACKTEST_GLOB}', hive_partitioning=true) l
  ON u.permno = l.permno
 AND u.dlycaldt = l.dlycaldt
;
""")


# ---------------------------------------------------------------------
# Step 2. Keep only rows with complete 5-day future return label.
#
# This automatically removes:
#   - final 5 observations for each stock without enough future returns;
#   - rows whose future 5-day window contains missing dlyret;
#   - rows that cannot be used for clean supervised training.
#
# For backtesting later, do NOT use only this complete-label table.
# Use backtesting_data and the backtest-specific return columns.
# ---------------------------------------------------------------------

con.execute("""
CREATE OR REPLACE TEMP VIEW model_panel_complete AS
SELECT *
FROM joined_panel
WHERE target_5d_raw_complete_only IS NOT NULL
;
""")


# ---------------------------------------------------------------------
# Step 3. Add cross-sectional target transformations.
#
# target_5d_raw_complete_only:
#   raw compounded future 5-day total return; useful for IC/backtest checks.
#
# target_5d_winsor:
#   date-wise 1%/99% winsorized return.
#
# target_5d_cs_zscore:
#   date-wise z-scored winsorized return; useful as LGBM regression target.
# ---------------------------------------------------------------------

con.execute("""
CREATE OR REPLACE TEMP VIEW model_panel_final AS
WITH date_quantiles AS (
    SELECT
        dlycaldt,
        APPROX_QUANTILE(target_5d_raw_complete_only, 0.01) AS target_5d_p01,
        APPROX_QUANTILE(target_5d_raw_complete_only, 0.99) AS target_5d_p99
    FROM model_panel_complete
    GROUP BY dlycaldt
),

winsorized AS (
    SELECT
        m.*,
        q.target_5d_p01,
        q.target_5d_p99,

        CASE
            WHEN m.target_5d_raw_complete_only < q.target_5d_p01 THEN q.target_5d_p01
            WHEN m.target_5d_raw_complete_only > q.target_5d_p99 THEN q.target_5d_p99
            ELSE m.target_5d_raw_complete_only
        END AS target_5d_winsor

    FROM model_panel_complete m
    LEFT JOIN date_quantiles q
      ON m.dlycaldt = q.dlycaldt
),

date_stats AS (
    SELECT
        dlycaldt,
        AVG(target_5d_winsor) AS target_5d_winsor_mean,
        STDDEV_SAMP(target_5d_winsor) AS target_5d_winsor_sd,
        COUNT(*) AS n_cross_section
    FROM winsorized
    GROUP BY dlycaldt
),

with_zscore AS (
    SELECT
        w.*,
        s.target_5d_winsor_mean,
        s.target_5d_winsor_sd,
        s.n_cross_section,

        CASE
            WHEN s.target_5d_winsor_sd IS NULL OR s.target_5d_winsor_sd = 0 THEN 0.0
            ELSE (w.target_5d_winsor - s.target_5d_winsor_mean) / s.target_5d_winsor_sd
        END AS target_5d_cs_zscore

    FROM winsorized w
    LEFT JOIN date_stats s
      ON w.dlycaldt = s.dlycaldt
)

SELECT *
FROM with_zscore
;
""")


# ---------------------------------------------------------------------
# Step 4. Save model panel.
# ---------------------------------------------------------------------

con.execute(f"""
COPY (
    SELECT *
    FROM model_panel_final
)
TO '{OUT_PANEL}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")

print(f"Saved model panel to: {OUT_PANEL}")


# ---------------------------------------------------------------------
# Step 5. Diagnostics.
# ---------------------------------------------------------------------

PANEL_GLOB = OUT_PANEL / "**/*.parquet"

summary = con.execute("""
    SELECT
        (SELECT COUNT(*) FROM joined_panel) AS n_universe_rows,
        (SELECT COUNT(*) FROM model_panel_complete) AS n_complete_label_rows,
        (SELECT COUNT(*) FROM joined_panel WHERE target_5d_raw_complete_only IS NULL)
            AS n_rows_dropped_missing_complete_5d_label,
        (SELECT AVG(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1.0 ELSE 0.0 END)
         FROM joined_panel) AS missing_complete_5d_label_rate
""").df()

summary_path = OUT_ANALYSIS / "model_panel_summary.csv"
summary.to_csv(summary_path, index=False)

print("\nModel panel summary:")
print(summary.to_string(index=False))
print(f"Saved to: {summary_path}")


daily_summary = con.execute(f"""
    SELECT
        dlycaldt,
        year,
        COUNT(*) AS n_model_rows,
        APPROX_QUANTILE(target_5d_raw_complete_only, 0.01) AS target_5d_p01,
        APPROX_QUANTILE(target_5d_raw_complete_only, 0.50) AS target_5d_p50,
        APPROX_QUANTILE(target_5d_raw_complete_only, 0.99) AS target_5d_p99,
        AVG(target_5d_raw_complete_only) AS mean_target_5d_raw,
        AVG(target_5d_cs_zscore) AS mean_target_5d_cs_zscore,
        STDDEV_SAMP(target_5d_cs_zscore) AS sd_target_5d_cs_zscore
    FROM read_parquet('{PANEL_GLOB}', hive_partitioning=true)
    GROUP BY dlycaldt, year
    ORDER BY dlycaldt
""").df()

daily_summary_path = OUT_ANALYSIS / "model_panel_daily_summary.csv"
daily_summary.to_csv(daily_summary_path, index=False)

print(f"Saved daily summary to: {daily_summary_path}")


yearly_summary = con.execute(f"""
    SELECT
        year,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT dlycaldt) AS n_days,
        COUNT(DISTINCT permno) AS n_unique_permnos,
        AVG(target_5d_raw_complete_only) AS mean_target_5d_raw,
        APPROX_QUANTILE(target_5d_raw_complete_only, 0.50) AS median_target_5d_raw,
        AVG(target_5d_cs_zscore) AS mean_target_5d_cs_zscore,
        STDDEV_SAMP(target_5d_cs_zscore) AS sd_target_5d_cs_zscore
    FROM read_parquet('{PANEL_GLOB}', hive_partitioning=true)
    GROUP BY year
    ORDER BY year
""").df()

yearly_summary_path = OUT_ANALYSIS / "model_panel_yearly_summary.csv"
yearly_summary.to_csv(yearly_summary_path, index=False)

print("\nYearly summary:")
print(yearly_summary.to_string(index=False))
print(f"Saved to: {yearly_summary_path}")


# Rows dropped because complete 5-day target is missing.
# We save only examples, not all dropped rows.
dropped_examples = con.execute("""
    SELECT *
    FROM joined_panel
    WHERE target_5d_raw_complete_only IS NULL
    ORDER BY dlycaldt DESC, permno
    LIMIT 500
""").df()

dropped_examples_path = OUT_ANALYSIS / "model_panel_dropped_missing_target_examples.csv"
dropped_examples.to_csv(dropped_examples_path, index=False)

print(f"\nSaved dropped-label examples to: {dropped_examples_path}")


recent_dates = con.execute("""
    SELECT
        dlycaldt,
        COUNT(*) AS n_universe_rows,
        SUM(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1 ELSE 0 END)
            AS n_missing_complete_5d_label,
        AVG(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1.0 ELSE 0.0 END)
            AS missing_complete_5d_label_rate
    FROM joined_panel
    GROUP BY dlycaldt
    ORDER BY dlycaldt DESC
    LIMIT 15
""").df()

recent_dates_path = OUT_ANALYSIS / "recent_dates_label_availability.csv"
recent_dates.to_csv(recent_dates_path, index=False)

print("\nRecent dates label availability:")
print(recent_dates.to_string(index=False))
print(f"Saved to: {recent_dates_path}")


print("\nFinished creating model panel.")