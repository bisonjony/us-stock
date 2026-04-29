from pathlib import Path
import shutil
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

CORE_DEDUP_GLOB = ROOT / "data/clean_parquet/daily_core_dup_removed/**/*.parquet"

OUT_BACKTEST = ROOT / "data/clean_parquet/backtesting_data"
OUT_ANALYSIS = ROOT / "data/clean_parquet/backtesting_data_analysis"

if OUT_BACKTEST.exists():
    shutil.rmtree(OUT_BACKTEST)

OUT_BACKTEST.parent.mkdir(parents=True, exist_ok=True)
OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=2")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")


# ---------------------------------------------------------------------
# Step 1. Create compact return source.
#
# We use broad daily_core_dup_removed, not the stock universe, because
# future realized returns should not depend on future universe membership.
# ---------------------------------------------------------------------

con.execute(f"""
CREATE OR REPLACE TEMP VIEW return_source AS
SELECT
    permno,
    dlycaldt,
    year,
    ticker,
    tradingstatusflg,
    securityactiveflg,
    dlydelflg,
    dlyret,
    dlyretx,
    dlyreti,
    dlyretmissflg,
    dlyretdurflg,
    prc,
    dlyprc,
    dlyprcflg,
    dlyvol,
    dlycap
FROM read_parquet('{CORE_DEDUP_GLOB}', hive_partitioning=true)
;
""")


# ---------------------------------------------------------------------
# Step 2. Create forward return columns.
#
# Label timing:
#   At close of day t, day-t information is known.
#   Labels use future returns t+1 through t+5.
#
# target_5d_raw_complete_only:
#   Compounds dlyret(t+1)...dlyret(t+5) only if all five returns exist.
#
# bt_5d_return_zero_after_missing:
#   Compounds valid returns until the first missing future return.
#   After the first missing return, assumes 0 return for remaining days.
#
# bt_5d_return_delist_stress_30:
#   Same as zero-after-missing, except if the first missing return is
#   delisting-related, apply a -30% terminal return.
#
# bt_5d_return_delist_stress_100:
#   Same as zero-after-missing, except if the first missing return is
#   delisting-related, apply a -100% terminal return.
#
# Delisting-related missing flags:
#   DM, DG, DP
# ---------------------------------------------------------------------

print("\nCreating backtesting label data...")

con.execute(f"""
CREATE OR REPLACE TEMP VIEW labels AS
WITH forward_returns AS (
    SELECT
        *,

        LEAD(dlycaldt, 1) OVER w AS fwd_date_1,
        LEAD(dlycaldt, 2) OVER w AS fwd_date_2,
        LEAD(dlycaldt, 3) OVER w AS fwd_date_3,
        LEAD(dlycaldt, 4) OVER w AS fwd_date_4,
        LEAD(dlycaldt, 5) OVER w AS fwd_date_5,

        LEAD(dlyret, 1) OVER w AS ret_1,
        LEAD(dlyret, 2) OVER w AS ret_2,
        LEAD(dlyret, 3) OVER w AS ret_3,
        LEAD(dlyret, 4) OVER w AS ret_4,
        LEAD(dlyret, 5) OVER w AS ret_5,

        LEAD(dlyretmissflg, 1) OVER w AS miss_flag_raw_1,
        LEAD(dlyretmissflg, 2) OVER w AS miss_flag_raw_2,
        LEAD(dlyretmissflg, 3) OVER w AS miss_flag_raw_3,
        LEAD(dlyretmissflg, 4) OVER w AS miss_flag_raw_4,
        LEAD(dlyretmissflg, 5) OVER w AS miss_flag_raw_5,

        LEAD(tradingstatusflg, 1) OVER w AS fwd_tradingstatusflg_1,
        LEAD(tradingstatusflg, 2) OVER w AS fwd_tradingstatusflg_2,
        LEAD(tradingstatusflg, 3) OVER w AS fwd_tradingstatusflg_3,
        LEAD(tradingstatusflg, 4) OVER w AS fwd_tradingstatusflg_4,
        LEAD(tradingstatusflg, 5) OVER w AS fwd_tradingstatusflg_5,

        LEAD(dlydelflg, 1) OVER w AS fwd_dlydelflg_1,
        LEAD(dlydelflg, 2) OVER w AS fwd_dlydelflg_2,
        LEAD(dlydelflg, 3) OVER w AS fwd_dlydelflg_3,
        LEAD(dlydelflg, 4) OVER w AS fwd_dlydelflg_4,
        LEAD(dlydelflg, 5) OVER w AS fwd_dlydelflg_5

    FROM return_source

    WINDOW w AS (
        PARTITION BY permno
        ORDER BY dlycaldt
    )
),

missing_flags AS (
    SELECT
        *,

        CASE
            WHEN fwd_date_1 IS NULL THEN 'NO_FUTURE_ROW'
            WHEN ret_1 IS NULL THEN COALESCE(miss_flag_raw_1, 'NO_FLAG')
            ELSE NULL
        END AS miss_flag_1,

        CASE
            WHEN fwd_date_2 IS NULL THEN 'NO_FUTURE_ROW'
            WHEN ret_2 IS NULL THEN COALESCE(miss_flag_raw_2, 'NO_FLAG')
            ELSE NULL
        END AS miss_flag_2,

        CASE
            WHEN fwd_date_3 IS NULL THEN 'NO_FUTURE_ROW'
            WHEN ret_3 IS NULL THEN COALESCE(miss_flag_raw_3, 'NO_FLAG')
            ELSE NULL
        END AS miss_flag_3,

        CASE
            WHEN fwd_date_4 IS NULL THEN 'NO_FUTURE_ROW'
            WHEN ret_4 IS NULL THEN COALESCE(miss_flag_raw_4, 'NO_FLAG')
            ELSE NULL
        END AS miss_flag_4,

        CASE
            WHEN fwd_date_5 IS NULL THEN 'NO_FUTURE_ROW'
            WHEN ret_5 IS NULL THEN COALESCE(miss_flag_raw_5, 'NO_FLAG')
            ELSE NULL
        END AS miss_flag_5

    FROM forward_returns
),

label_base AS (
    SELECT
        *,

        CASE
            WHEN miss_flag_1 IS NOT NULL THEN 1
            WHEN miss_flag_2 IS NOT NULL THEN 2
            WHEN miss_flag_3 IS NOT NULL THEN 3
            WHEN miss_flag_4 IS NOT NULL THEN 4
            WHEN miss_flag_5 IS NOT NULL THEN 5
            ELSE NULL
        END AS target_first_missing_return_pos,

        CASE
            WHEN miss_flag_1 IS NOT NULL THEN miss_flag_1
            WHEN miss_flag_2 IS NOT NULL THEN miss_flag_2
            WHEN miss_flag_3 IS NOT NULL THEN miss_flag_3
            WHEN miss_flag_4 IS NOT NULL THEN miss_flag_4
            WHEN miss_flag_5 IS NOT NULL THEN miss_flag_5
            ELSE NULL
        END AS target_first_missing_return_flag,

        (
            CASE WHEN ret_1 IS NOT NULL THEN 1 ELSE 0 END
          + CASE WHEN ret_2 IS NOT NULL THEN 1 ELSE 0 END
          + CASE WHEN ret_3 IS NOT NULL THEN 1 ELSE 0 END
          + CASE WHEN ret_4 IS NOT NULL THEN 1 ELSE 0 END
          + CASE WHEN ret_5 IS NOT NULL THEN 1 ELSE 0 END
        ) AS target_n_valid_forward_returns,

        CASE
            WHEN miss_flag_1 IS NOT NULL
              OR miss_flag_2 IS NOT NULL
              OR miss_flag_3 IS NOT NULL
              OR miss_flag_4 IS NOT NULL
              OR miss_flag_5 IS NOT NULL
            THEN 1 ELSE 0
        END AS target_has_missing_return,

        CASE
            WHEN miss_flag_1 IN ('DM', 'DG', 'DP')
              OR miss_flag_2 IN ('DM', 'DG', 'DP')
              OR miss_flag_3 IN ('DM', 'DG', 'DP')
              OR miss_flag_4 IN ('DM', 'DG', 'DP')
              OR miss_flag_5 IN ('DM', 'DG', 'DP')
            THEN 1 ELSE 0
        END AS target_has_delisting_missing_flag,

        CONCAT_WS(
            '|',
            CASE WHEN miss_flag_1 = 'NT' OR miss_flag_2 = 'NT' OR miss_flag_3 = 'NT' OR miss_flag_4 = 'NT' OR miss_flag_5 = 'NT' THEN 'NT' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'NS' OR miss_flag_2 = 'NS' OR miss_flag_3 = 'NS' OR miss_flag_4 = 'NS' OR miss_flag_5 = 'NS' THEN 'NS' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'MP' OR miss_flag_2 = 'MP' OR miss_flag_3 = 'MP' OR miss_flag_4 = 'MP' OR miss_flag_5 = 'MP' THEN 'MP' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'RA' OR miss_flag_2 = 'RA' OR miss_flag_3 = 'RA' OR miss_flag_4 = 'RA' OR miss_flag_5 = 'RA' THEN 'RA' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'DM' OR miss_flag_2 = 'DM' OR miss_flag_3 = 'DM' OR miss_flag_4 = 'DM' OR miss_flag_5 = 'DM' THEN 'DM' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'GP' OR miss_flag_2 = 'GP' OR miss_flag_3 = 'GP' OR miss_flag_4 = 'GP' OR miss_flag_5 = 'GP' THEN 'GP' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'DG' OR miss_flag_2 = 'DG' OR miss_flag_3 = 'DG' OR miss_flag_4 = 'DG' OR miss_flag_5 = 'DG' THEN 'DG' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'DP' OR miss_flag_2 = 'DP' OR miss_flag_3 = 'DP' OR miss_flag_4 = 'DP' OR miss_flag_5 = 'DP' THEN 'DP' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'NO_FLAG' OR miss_flag_2 = 'NO_FLAG' OR miss_flag_3 = 'NO_FLAG' OR miss_flag_4 = 'NO_FLAG' OR miss_flag_5 = 'NO_FLAG' THEN 'NO_FLAG' ELSE NULL END,
            CASE WHEN miss_flag_1 = 'NO_FUTURE_ROW' OR miss_flag_2 = 'NO_FUTURE_ROW' OR miss_flag_3 = 'NO_FUTURE_ROW' OR miss_flag_4 = 'NO_FUTURE_ROW' OR miss_flag_5 = 'NO_FUTURE_ROW' THEN 'NO_FUTURE_ROW' ELSE NULL END
        ) AS target_missing_return_flag_set

    FROM missing_flags
),

label_returns AS (
    SELECT
        *,

        -- One-day raw target.
        CASE
            WHEN ret_1 IS NOT NULL THEN ret_1
            ELSE NULL
        END AS target_1d_raw,

        -- Complete-only 5-day target.
        CASE
            WHEN ret_1 IS NOT NULL
             AND ret_2 IS NOT NULL
             AND ret_3 IS NOT NULL
             AND ret_4 IS NOT NULL
             AND ret_5 IS NOT NULL
            THEN
                (1 + ret_1)
              * (1 + ret_2)
              * (1 + ret_3)
              * (1 + ret_4)
              * (1 + ret_5)
              - 1
            ELSE NULL
        END AS target_5d_raw_complete_only,

        -- Backtest 5-day return:
        -- compound until the first missing future return;
        -- after first missing, assume 0 return.
        CASE
            WHEN ret_1 IS NULL THEN 0.0
            WHEN ret_2 IS NULL THEN (1 + ret_1) - 1
            WHEN ret_3 IS NULL THEN (1 + ret_1) * (1 + ret_2) - 1
            WHEN ret_4 IS NULL THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) - 1
            WHEN ret_5 IS NULL THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) * (1 + ret_4) - 1
            ELSE
                (1 + ret_1)
              * (1 + ret_2)
              * (1 + ret_3)
              * (1 + ret_4)
              * (1 + ret_5)
              - 1
        END AS bt_5d_return_zero_after_missing,

        -- Backtest stress return:
        -- If the first missing return is delisting-related, apply -30%.
        CASE
            WHEN target_first_missing_return_flag IN ('DM', 'DG', 'DP') THEN
                CASE
                    WHEN target_first_missing_return_pos = 1 THEN -0.30
                    WHEN target_first_missing_return_pos = 2 THEN (1 + ret_1) * 0.70 - 1
                    WHEN target_first_missing_return_pos = 3 THEN (1 + ret_1) * (1 + ret_2) * 0.70 - 1
                    WHEN target_first_missing_return_pos = 4 THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) * 0.70 - 1
                    WHEN target_first_missing_return_pos = 5 THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) * (1 + ret_4) * 0.70 - 1
                    ELSE NULL
                END
            ELSE
                CASE
                    WHEN ret_1 IS NULL THEN 0.0
                    WHEN ret_2 IS NULL THEN (1 + ret_1) - 1
                    WHEN ret_3 IS NULL THEN (1 + ret_1) * (1 + ret_2) - 1
                    WHEN ret_4 IS NULL THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) - 1
                    WHEN ret_5 IS NULL THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) * (1 + ret_4) - 1
                    ELSE
                        (1 + ret_1)
                      * (1 + ret_2)
                      * (1 + ret_3)
                      * (1 + ret_4)
                      * (1 + ret_5)
                      - 1
                END
        END AS bt_5d_return_delist_stress_30,

        -- Backtest stress return:
        -- If the first missing return is delisting-related, apply -100%.
        CASE
            WHEN target_first_missing_return_flag IN ('DM', 'DG', 'DP') THEN -1.0
            ELSE
                CASE
                    WHEN ret_1 IS NULL THEN 0.0
                    WHEN ret_2 IS NULL THEN (1 + ret_1) - 1
                    WHEN ret_3 IS NULL THEN (1 + ret_1) * (1 + ret_2) - 1
                    WHEN ret_4 IS NULL THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) - 1
                    WHEN ret_5 IS NULL THEN (1 + ret_1) * (1 + ret_2) * (1 + ret_3) * (1 + ret_4) - 1
                    ELSE
                        (1 + ret_1)
                      * (1 + ret_2)
                      * (1 + ret_3)
                      * (1 + ret_4)
                      * (1 + ret_5)
                      - 1
                END
        END AS bt_5d_return_delist_stress_100

    FROM label_base
)

SELECT
    permno,
    dlycaldt,
    year,
    ticker,

    tradingstatusflg,
    securityactiveflg,
    dlydelflg,

    prc,
    dlyprc,
    dlyprcflg,
    dlyvol,
    dlycap,

    dlyret AS current_dlyret,
    dlyretx AS current_dlyretx,
    dlyreti AS current_dlyreti,
    dlyretmissflg AS current_dlyretmissflg,
    dlyretdurflg AS current_dlyretdurflg,

    fwd_date_1,
    fwd_date_2,
    fwd_date_3,
    fwd_date_4,
    fwd_date_5,

    ret_1,
    ret_2,
    ret_3,
    ret_4,
    ret_5,

    miss_flag_1,
    miss_flag_2,
    miss_flag_3,
    miss_flag_4,
    miss_flag_5,

    target_1d_raw,
    target_5d_raw_complete_only,

    bt_5d_return_zero_after_missing,
    bt_5d_return_delist_stress_30,
    bt_5d_return_delist_stress_100,

    target_has_missing_return,
    target_first_missing_return_pos,
    target_first_missing_return_flag,
    target_missing_return_flag_set,
    target_has_delisting_missing_flag,
    target_n_valid_forward_returns,

    fwd_tradingstatusflg_1,
    fwd_tradingstatusflg_2,
    fwd_tradingstatusflg_3,
    fwd_tradingstatusflg_4,
    fwd_tradingstatusflg_5,

    fwd_dlydelflg_1,
    fwd_dlydelflg_2,
    fwd_dlydelflg_3,
    fwd_dlydelflg_4,
    fwd_dlydelflg_5

FROM label_returns
;
""")


# ---------------------------------------------------------------------
# Step 3. Save backtesting data.
# ---------------------------------------------------------------------

con.execute(f"""
COPY (
    SELECT *
    FROM labels
)
TO '{OUT_BACKTEST}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")

print(f"\nSaved backtesting data to: {OUT_BACKTEST}")


# ---------------------------------------------------------------------
# Step 4. Diagnostics.
# ---------------------------------------------------------------------

BT_GLOB = OUT_BACKTEST / "**/*.parquet"

summary = con.execute(f"""
    SELECT
        COUNT(*) AS n_rows,
        COUNT(DISTINCT permno || '_' || CAST(dlycaldt AS VARCHAR)) AS n_unique_stock_days,

        SUM(CASE WHEN target_1d_raw IS NULL THEN 1 ELSE 0 END) AS n_missing_target_1d_raw,
        SUM(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1 ELSE 0 END) AS n_missing_target_5d_complete,

        AVG(CASE WHEN target_1d_raw IS NULL THEN 1.0 ELSE 0.0 END) AS missing_target_1d_rate,
        AVG(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1.0 ELSE 0.0 END) AS missing_target_5d_complete_rate,

        SUM(target_has_missing_return) AS n_has_missing_forward_return,
        AVG(target_has_missing_return) AS has_missing_forward_return_rate,

        SUM(target_has_delisting_missing_flag) AS n_has_delisting_missing_flag,
        AVG(target_has_delisting_missing_flag) AS has_delisting_missing_flag_rate
    FROM read_parquet('{BT_GLOB}', hive_partitioning=true)
""").df()

summary_path = OUT_ANALYSIS / "backtesting_data_label_summary.csv"
summary.to_csv(summary_path, index=False)

print("\nBacktesting label summary:")
print(summary.to_string(index=False))
print(f"Saved to: {summary_path}")


flag_counts = con.execute(f"""
    SELECT
        COALESCE(target_missing_return_flag_set, '__NO_MISSING__') AS target_missing_return_flag_set,
        COUNT(*) AS n_rows
    FROM read_parquet('{BT_GLOB}', hive_partitioning=true)
    GROUP BY target_missing_return_flag_set
    ORDER BY n_rows DESC
""").df()

flag_counts_path = OUT_ANALYSIS / "backtesting_data_missing_flag_set_counts.csv"
flag_counts.to_csv(flag_counts_path, index=False)

print("\nMissing flag-set counts:")
print(flag_counts.head(30).to_string(index=False))
print(f"Saved to: {flag_counts_path}")


first_flag_counts = con.execute(f"""
    SELECT
        COALESCE(target_first_missing_return_flag, '__NO_MISSING__') AS target_first_missing_return_flag,
        COUNT(*) AS n_rows
    FROM read_parquet('{BT_GLOB}', hive_partitioning=true)
    GROUP BY target_first_missing_return_flag
    ORDER BY n_rows DESC
""").df()

first_flag_counts_path = OUT_ANALYSIS / "backtesting_data_first_missing_flag_counts.csv"
first_flag_counts.to_csv(first_flag_counts_path, index=False)

print("\nFirst missing flag counts:")
print(first_flag_counts.head(30).to_string(index=False))
print(f"Saved to: {first_flag_counts_path}")


yearly_summary = con.execute(f"""
    SELECT
        year,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1 ELSE 0 END)
            AS n_missing_target_5d_complete,
        AVG(CASE WHEN target_5d_raw_complete_only IS NULL THEN 1.0 ELSE 0.0 END)
            AS missing_target_5d_complete_rate,
        SUM(target_has_missing_return) AS n_has_missing_forward_return,
        AVG(target_has_missing_return) AS has_missing_forward_return_rate,
        SUM(target_has_delisting_missing_flag) AS n_has_delisting_missing_flag,
        AVG(target_has_delisting_missing_flag) AS has_delisting_missing_flag_rate
    FROM read_parquet('{BT_GLOB}', hive_partitioning=true)
    GROUP BY year
    ORDER BY year
""").df()

yearly_summary_path = OUT_ANALYSIS / "backtesting_data_yearly_summary.csv"
yearly_summary.to_csv(yearly_summary_path, index=False)

print("\nYearly summary:")
print(yearly_summary.to_string(index=False))
print(f"Saved to: {yearly_summary_path}")


examples = con.execute(f"""
    SELECT *
    FROM read_parquet('{BT_GLOB}', hive_partitioning=true)
    WHERE target_has_missing_return = 1
    ORDER BY dlycaldt, permno
    LIMIT 200
""").df()

examples_path = OUT_ANALYSIS / "backtesting_data_missing_return_examples.csv"
examples.to_csv(examples_path, index=False)

print(f"\nSaved missing-return examples to: {examples_path}")

print("\nFinished creating backtesting data.")