from pathlib import Path
import shutil
import duckdb


ROOT = Path("/home/xul9527/us-stock")

CORE_GLOB = ROOT / "data/clean_parquet/daily_core/**/*.parquet"

OUT_ALL = ROOT / "data/clean_parquet/daily_universe_prepare_all"
OUT_BASE = ROOT / "data/clean_parquet/daily_universe_ready_base"
OUT_TERMINAL = ROOT / "data/clean_parquet/daily_terminal_events"

for out_dir in [OUT_ALL, OUT_BASE, OUT_TERMINAL]:
    if out_dir.exists():
        shutil.rmtree(out_dir)

OUT_ALL.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")


# ---------------------------------------------------------------------
# Step 1. Create full prepared table with flags and derived variables.
# This keeps all rows.
# ---------------------------------------------------------------------

con.execute(f"""
CREATE OR REPLACE TEMP VIEW prepared_all AS
WITH base AS (
    SELECT
        *
    FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
),

flags AS (
    SELECT
        *,

        -- -------------------------------------------------------------
        -- Trading status / terminal-event flags
        -- -------------------------------------------------------------
        CASE
            WHEN tradingstatusflg = 'A' THEN 1 ELSE 0
        END AS is_active_trading_flag,

        CASE
            WHEN tradingstatusflg IN ('X', 'S', 'H') THEN 1 ELSE 0
        END AS non_tradable_status_flag,

        CASE
            WHEN tradingstatusflg = 'D'
              OR dlydelflg = 'Y'
              OR (prc = 0 AND dlyprcflg = 'DA')
            THEN 1 ELSE 0
        END AS terminal_event_flag,

        CASE
            WHEN dlydelflg = 'Y' THEN 1 ELSE 0
        END AS daily_delisting_flag,


        -- -------------------------------------------------------------
        -- Price flags
        -- -------------------------------------------------------------
        CASE
            WHEN prc IS NULL OR prc <= 0 THEN 1 ELSE 0
        END AS invalid_price_flag,

        CASE
            WHEN prc = 0 THEN 1 ELSE 0
        END AS zero_price_flag,

        CASE
            WHEN prc > 100000 THEN 1 ELSE 0
        END AS high_price_flag,

        CASE
            WHEN dlyprcflg = 'BA' THEN 1 ELSE 0
        END AS price_from_bidask_flag,

        CASE
            WHEN dlyprc_negative_flag = TRUE THEN 1 ELSE 0
        END AS negative_raw_price_flag,


        -- -------------------------------------------------------------
        -- OHLC flags
        -- -------------------------------------------------------------
        CASE
            WHEN dlyopen IS NOT NULL
             AND dlyhigh IS NOT NULL
             AND dlylow IS NOT NULL
             AND dlyclose IS NOT NULL
            THEN 1 ELSE 0
        END AS has_ohlc_flag,

        CASE
            WHEN dlyopen IS NULL
              OR dlyhigh IS NULL
              OR dlylow IS NULL
              OR dlyclose IS NULL
            THEN 1 ELSE 0
        END AS ohlc_missing_flag,

        CASE
            WHEN dlyhigh IS NOT NULL
             AND dlylow IS NOT NULL
             AND dlyopen IS NOT NULL
             AND dlyclose IS NOT NULL
             AND (
                    dlyhigh < dlylow
                 OR dlyopen > dlyhigh
                 OR dlyopen < dlylow
                 OR dlyclose > dlyhigh
                 OR dlyclose < dlylow
             )
            THEN 1 ELSE 0
        END AS ohlc_inconsistent_flag,

        CASE
            WHEN (
                    dlyopen IS NULL
                 OR dlyhigh IS NULL
                 OR dlylow IS NULL
                 OR dlyclose IS NULL
                 )
             AND prc IS NOT NULL
             AND prc > 0
             AND tradingstatusflg = 'A'
             AND dlyprcflg = 'BA'
            THEN 1 ELSE 0
        END AS ohlc_imputed_from_prc_flag,


        -- -------------------------------------------------------------
        -- Bid/ask flags
        -- -------------------------------------------------------------
        CASE
            WHEN dlybid IS NULL OR dlyask IS NULL THEN 1 ELSE 0
        END AS bidask_missing_flag,

        CASE
            WHEN dlybid IS NOT NULL
             AND dlyask IS NOT NULL
             AND dlybid > 0
             AND dlyask > 0
             AND dlyask >= dlybid
            THEN 1 ELSE 0
        END AS valid_bidask_flag,


        -- -------------------------------------------------------------
        -- Liquidity / size missing flags
        -- -------------------------------------------------------------
        CASE
            WHEN dlyvol IS NULL OR dlyvol <= 0 THEN 1 ELSE 0
        END AS invalid_volume_flag,

        CASE
            WHEN dlycap IS NULL OR dlycap <= 0 THEN 1 ELSE 0
        END AS invalid_market_cap_flag,

        CASE
            WHEN dlyprcvol IS NULL THEN 1 ELSE 0
        END AS price_volume_missing_flag,


        -- -------------------------------------------------------------
        -- Return flags
        -- -------------------------------------------------------------
        CASE
            WHEN dlyret IS NULL THEN 1 ELSE 0
        END AS return_missing_flag,

        CASE
            WHEN dlyretx IS NULL THEN 1 ELSE 0
        END AS price_return_missing_flag,

        CASE
            WHEN dlyreti IS NULL THEN 1 ELSE 0
        END AS income_return_missing_flag,

        CASE
            WHEN dlyret < -1 OR dlyret > 20 THEN 1 ELSE 0
        END AS extreme_return_flag,


        -- -------------------------------------------------------------
        -- Security/share metadata flags
        -- -------------------------------------------------------------
        CASE
            WHEN securitytype IS NULL
              OR sharetype IS NULL
              OR shrout IS NULL
              OR shrout <= 0
            THEN 1 ELSE 0
        END AS security_metadata_missing_flag,

        CASE
            WHEN securitytype = 'EQTY' THEN 1 ELSE 0
        END AS is_equity_flag,

        CASE
            WHEN securitytype = 'FUND' THEN 1 ELSE 0
        END AS is_fund_flag,

        CASE
            WHEN sharetype = 'NS' THEN 1 ELSE 0
        END AS is_common_share_flag,

        CASE
            WHEN shradrflg = 'Y' THEN 1 ELSE 0
        END AS is_adr_flag,

        CASE
            WHEN usincflg = 'Y' THEN 1 ELSE 0
        END AS is_us_incorporated_flag,


        -- -------------------------------------------------------------
        -- Optional microstructure coverage flags
        -- -------------------------------------------------------------
        CASE
            WHEN dlynumtrd IS NULL THEN 1 ELSE 0
        END AS num_trades_missing_flag,

        CASE
            WHEN dlymmcnt IS NULL THEN 1 ELSE 0
        END AS market_maker_count_missing_flag,

        CASE
            WHEN exchangetier IS NULL THEN 1 ELSE 0
        END AS exchange_tier_missing_flag,


        -- -------------------------------------------------------------
        -- Delisting / distribution / corporate-action flags
        -- -------------------------------------------------------------
        CASE
            WHEN delactiontype IS NOT NULL THEN 1 ELSE 0
        END AS has_delactiontype_flag,

        CASE
            WHEN shareclass IS NOT NULL THEN 1 ELSE 0
        END AS has_shareclass_flag,

        CASE
            WHEN disexdt IS NOT NULL
              OR distype IS NOT NULL
              OR disdivamt IS NOT NULL
              OR disfacpr IS NOT NULL
              OR disfacshr IS NOT NULL
            THEN 1 ELSE 0
        END AS has_distribution_event_flag,

        CASE
            WHEN disdivamt IS NOT NULL THEN 1 ELSE 0
        END AS has_distribution_amount_flag,

        CASE
            WHEN disordinaryflg = 'Y' THEN 1 ELSE 0
        END AS has_ordinary_distribution_flag,

        CASE
            WHEN disfacpr < 0 THEN 1 ELSE 0
        END AS negative_disfacpr_flag,

        CASE
            WHEN disfacshr < 0 THEN 1 ELSE 0
        END AS negative_disfacshr_flag,

        CASE
            WHEN disdivamt < 0 THEN 1 ELSE 0
        END AS negative_disdivamt_flag,

        CASE
            WHEN dlyfacprc IS NOT NULL AND dlyfacprc <> 1 THEN 1 ELSE 0
        END AS price_adjustment_event_flag

    FROM base
),

derived AS (
    SELECT
        *,

        -- -------------------------------------------------------------
        -- Optional OHLC-clean variables.
        -- Raw OHLC is preserved. These are auxiliary variables only.
        -- We only fill from prc in active BA-like rows.
        -- -------------------------------------------------------------
        CASE
            WHEN dlyopen IS NOT NULL THEN dlyopen
            WHEN ohlc_imputed_from_prc_flag = 1 THEN prc
            ELSE NULL
        END AS dlyopen_clean,

        CASE
            WHEN dlyhigh IS NOT NULL THEN dlyhigh
            WHEN ohlc_imputed_from_prc_flag = 1 THEN prc
            ELSE NULL
        END AS dlyhigh_clean,

        CASE
            WHEN dlylow IS NOT NULL THEN dlylow
            WHEN ohlc_imputed_from_prc_flag = 1 THEN prc
            ELSE NULL
        END AS dlylow_clean,

        CASE
            WHEN dlyclose IS NOT NULL THEN dlyclose
            WHEN ohlc_imputed_from_prc_flag = 1 THEN prc
            ELSE NULL
        END AS dlyclose_clean,


        -- -------------------------------------------------------------
        -- Basic derived liquidity / price variables
        -- -------------------------------------------------------------
        CASE
            WHEN prc IS NOT NULL AND prc > 0 THEN LN(prc)
            ELSE NULL
        END AS log_prc,

        CASE
            WHEN dlycap IS NOT NULL AND dlycap > 0 THEN LN(dlycap)
            ELSE NULL
        END AS log_dlycap,

        CASE
            WHEN prc IS NOT NULL
             AND prc > 0
             AND dlyvol IS NOT NULL
             AND dlyvol > 0
            THEN prc * dlyvol
            ELSE NULL
        END AS dollar_volume,

        CASE
            WHEN dlybid IS NOT NULL
             AND dlyask IS NOT NULL
             AND dlybid > 0
             AND dlyask > 0
             AND dlyask >= dlybid
            THEN (dlyask - dlybid) / ((dlyask + dlybid) / 2.0)
            ELSE NULL
        END AS bid_ask_spread,

        CASE
            WHEN dlynumtrd IS NOT NULL AND dlynumtrd >= 0 THEN LN(1 + dlynumtrd)
            ELSE NULL
        END AS log_num_trades,

        CASE
            WHEN dlymmcnt IS NOT NULL AND dlymmcnt >= 0 THEN LN(1 + dlymmcnt)
            ELSE NULL
        END AS log_market_maker_count,


        -- -------------------------------------------------------------
        -- Strict OHLC features: computed only from raw OHLC.
        -- -------------------------------------------------------------
        CASE
            WHEN dlyopen IS NOT NULL
             AND dlyclose IS NOT NULL
             AND dlyopen > 0
             AND dlyclose > 0
            THEN dlyclose / dlyopen - 1
            ELSE NULL
        END AS intraday_ret_strict,

        CASE
            WHEN dlyhigh IS NOT NULL
             AND dlylow IS NOT NULL
             AND dlyhigh > 0
             AND dlylow > 0
             AND dlyhigh >= dlylow
            THEN dlyhigh / dlylow - 1
            ELSE NULL
        END AS intraday_range_strict,


        -- -------------------------------------------------------------
        -- Clean OHLC features: use auxiliary clean OHLC variables.
        -- These are optional; raw strict features remain primary.
        -- -------------------------------------------------------------
        CASE
            WHEN dlyopen_clean IS NOT NULL
             AND dlyclose_clean IS NOT NULL
             AND dlyopen_clean > 0
             AND dlyclose_clean > 0
            THEN dlyclose_clean / dlyopen_clean - 1
            ELSE NULL
        END AS intraday_ret_clean,

        CASE
            WHEN dlyhigh_clean IS NOT NULL
             AND dlylow_clean IS NOT NULL
             AND dlyhigh_clean > 0
             AND dlylow_clean > 0
             AND dlyhigh_clean >= dlylow_clean
            THEN dlyhigh_clean / dlylow_clean - 1
            ELSE NULL
        END AS intraday_range_clean

    FROM flags
)

SELECT *
FROM derived
;
""")


# ---------------------------------------------------------------------
# Step 2. Save full prepared table.
# This keeps all rows and adds flags/derived variables.
# ---------------------------------------------------------------------

con.execute(f"""
COPY (
    SELECT *
    FROM prepared_all
)
TO '{OUT_ALL}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")


# ---------------------------------------------------------------------
# Step 3. Save universe-ready base table.
# This is NOT the final universe.
#
# Important:
#   daily_core / daily_universe_prepare_all may contain multiple rows
#   for the same (permno, dlycaldt), usually because there are multiple
#   distribution-event records on the same stock-date.
#
# For universe creation and modeling, we need one row per stock-date.
# Diagnostics show that duplicate rows do not differ in core trading fields
# such as price, return, volume, market cap, OHLC, bid, or ask. They mainly
# differ in distribution metadata such as distype and disseqnbr.
#
# Therefore:
#   - keep all rows in daily_universe_prepare_all for auditability;
#   - deduplicate only daily_universe_ready_base.
# ---------------------------------------------------------------------

con.execute(f"""
COPY (
    WITH universe_ready_filtered AS (
        SELECT *
        FROM prepared_all
        WHERE is_active_trading_flag = 1
          AND non_tradable_status_flag = 0
          AND terminal_event_flag = 0

          AND invalid_price_flag = 0
          AND invalid_volume_flag = 0
          AND invalid_market_cap_flag = 0

          AND return_missing_flag = 0
          AND security_metadata_missing_flag = 0
    ),

    universe_ready_dedup AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY permno, dlycaldt
                    ORDER BY raw_row_id
                ) AS duplicate_row_number,

                COUNT(*) OVER (
                    PARTITION BY permno, dlycaldt
                ) AS n_rows_same_stock_day
            FROM universe_ready_filtered
        )
        WHERE duplicate_row_number = 1
    )

    SELECT
        * EXCLUDE (duplicate_row_number)
    FROM universe_ready_dedup
)
TO '{OUT_BASE}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")


# ---------------------------------------------------------------------
# Step 4. Save terminal/delisting rows separately.
# These are not trading candidates, but we keep them for later
# backtest / realized-return handling.
# ---------------------------------------------------------------------

con.execute(f"""
COPY (
    SELECT *
    FROM prepared_all
    WHERE terminal_event_flag = 1
)
TO '{OUT_TERMINAL}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")


# ---------------------------------------------------------------------
# Step 5. Quick sanity checks.
# ---------------------------------------------------------------------

print("\nFinished preparing universe-base data.")

print("\nOutput paths:")
print(f"  Full prepared table:        {OUT_ALL}")
print(f"  Universe-ready base table:  {OUT_BASE}")
print(f"  Terminal events table:      {OUT_TERMINAL}")

print("\nRow counts:")
print(con.execute(f"""
    SELECT 'daily_core' AS table_name, COUNT(*) AS n
    FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)

    UNION ALL

    SELECT 'daily_universe_prepare_all' AS table_name, COUNT(*) AS n
    FROM read_parquet('{OUT_ALL}/**/*.parquet', hive_partitioning=true)

    UNION ALL

    SELECT 'daily_universe_ready_base' AS table_name, COUNT(*) AS n
    FROM read_parquet('{OUT_BASE}/**/*.parquet', hive_partitioning=true)

    UNION ALL

    SELECT 'daily_terminal_events' AS table_name, COUNT(*) AS n
    FROM read_parquet('{OUT_TERMINAL}/**/*.parquet', hive_partitioning=true)
""").df())

print("\nUniverse-ready base sample:")
print(con.execute(f"""
    SELECT
        dlycaldt,
        permno,
        ticker,
        primaryexch,
        securitytype,
        sharetype,
        tradingstatusflg,
        prc,
        dlyvol,
        dlycap,
        dlyret,
        has_ohlc_flag,
        ohlc_missing_flag,
        price_from_bidask_flag,
        bidask_missing_flag,
        dollar_volume
    FROM read_parquet('{OUT_BASE}/**/*.parquet', hive_partitioning=true)
    ORDER BY dlycaldt DESC, permno
    LIMIT 20
""").df())