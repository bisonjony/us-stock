from pathlib import Path
import shutil
import duckdb


ROOT = Path("/home/xul9527/us-stock")

BASE_GLOB = ROOT / "data/clean_parquet/daily_universe_ready_base/**/*.parquet"
OUT_UNIVERSE = ROOT / "data/clean_parquet/daily_stock_universe"
OUT_DAILY_SUMMARY = ROOT / "data/clean_parquet/daily_stock_universe_daily_summary.csv"
OUT_YEARLY_SUMMARY = ROOT / "data/clean_parquet/daily_stock_universe_yearly_summary.csv"

if OUT_UNIVERSE.exists():
    shutil.rmtree(OUT_UNIVERSE)

OUT_UNIVERSE.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")


# ---------------------------------------------------------------------
# Universe parameters
# ---------------------------------------------------------------------

# Adjust this if your CRSP primaryexch mapping differs.
MAIN_EXCHANGES = ("N", "Q", "A")

MIN_PRICE = 5.0
MIN_ADV20 = 1_000_000.0
MIN_HISTORY_OBS = 126

MAX_MARKET_CAP_RANK = 3000
MAX_ADV20_RANK = 4000

main_exchange_sql = "(" + ", ".join(f"'{x}'" for x in MAIN_EXCHANGES) + ")"


# ---------------------------------------------------------------------
# Create a ranked candidate table.
#
# Important:
#   - All filters use day-t or past information only.
#   - No future target or future survival information is used.
#   - This table is used to form the daily universe u_t.
# ---------------------------------------------------------------------

con.execute(f"""
CREATE OR REPLACE TEMP VIEW universe_ranked AS
WITH common_equity_candidates AS (
    SELECT
        *,

        CASE
            WHEN prc IS NOT NULL
             AND prc > 0
             AND dlyvol IS NOT NULL
             AND dlyvol > 0
            THEN prc * dlyvol
            ELSE NULL
        END AS dollar_volume_for_universe

    FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)

    WHERE securitytype = 'EQTY'
      AND securitysubtype = 'COM'
      AND sharetype = 'NS'
      AND usincflg = 'Y'
      AND shradrflg = 'N'
      AND primaryexch IN {main_exchange_sql}
),

with_history AS (
    SELECT
        *,

        AVG(dollar_volume_for_universe) OVER (
            PARTITION BY permno
            ORDER BY dlycaldt
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS adv20,

        AVG(dlyvol) OVER (
            PARTITION BY permno
            ORDER BY dlycaldt
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20d,

        COUNT(dlyret) OVER (
            PARTITION BY permno
            ORDER BY dlycaldt
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS hist_ret_obs_252,

        COUNT(*) OVER (
            PARTITION BY permno
            ORDER BY dlycaldt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS trading_age_obs

    FROM common_equity_candidates
),

ranked AS (
    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY dlycaldt
            ORDER BY dlycap DESC NULLS LAST
        ) AS market_cap_rank,

        ROW_NUMBER() OVER (
            PARTITION BY dlycaldt
            ORDER BY adv20 DESC NULLS LAST
        ) AS adv20_rank,

        COUNT(*) OVER (
            PARTITION BY dlycaldt
        ) AS n_common_equity_candidates

    FROM with_history
)

SELECT *
FROM ranked
;
""")


# ---------------------------------------------------------------------
# Save final daily universe.
#
# This is the first actual universe u_t.
# It is still not the modeling table with target_5d.
# ---------------------------------------------------------------------

con.execute(f"""
COPY (
    SELECT
        *,

        1 AS in_universe_flag,

        '{",".join(MAIN_EXCHANGES)}' AS universe_exchange_set,
        {MIN_PRICE} AS universe_min_price,
        {MIN_ADV20} AS universe_min_adv20,
        {MIN_HISTORY_OBS} AS universe_min_history_obs,
        {MAX_MARKET_CAP_RANK} AS universe_max_market_cap_rank,
        {MAX_ADV20_RANK} AS universe_max_adv20_rank

    FROM universe_ranked

    WHERE prc >= {MIN_PRICE}
      AND adv20 IS NOT NULL
      AND adv20 >= {MIN_ADV20}
      AND hist_ret_obs_252 >= {MIN_HISTORY_OBS}
      AND market_cap_rank <= {MAX_MARKET_CAP_RANK}
      AND adv20_rank <= {MAX_ADV20_RANK}
)
TO '{OUT_UNIVERSE}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")


# ---------------------------------------------------------------------
# Diagnostics: daily and yearly universe sizes.
# ---------------------------------------------------------------------

daily_summary = con.execute(f"""
    SELECT
        dlycaldt,
        year,
        COUNT(*) AS n_universe,
        MIN(prc) AS min_prc,
        APPROX_QUANTILE(prc, 0.5) AS median_prc,
        APPROX_QUANTILE(dlycap, 0.5) AS median_mktcap,
        APPROX_QUANTILE(adv20, 0.5) AS median_adv20,
        MIN(market_cap_rank) AS best_market_cap_rank,
        MAX(market_cap_rank) AS worst_market_cap_rank,
        MIN(adv20_rank) AS best_adv20_rank,
        MAX(adv20_rank) AS worst_adv20_rank
    FROM read_parquet('{OUT_UNIVERSE}/**/*.parquet', hive_partitioning=true)
    GROUP BY dlycaldt, year
    ORDER BY dlycaldt
""").df()

daily_summary.to_csv(OUT_DAILY_SUMMARY, index=False)


yearly_summary = con.execute(f"""
    WITH universe AS (
        SELECT *
        FROM read_parquet('{OUT_UNIVERSE}/**/*.parquet', hive_partitioning=true)
    ),

    daily_counts AS (
        SELECT
            year,
            dlycaldt,
            COUNT(*) AS n_daily
        FROM universe
        GROUP BY year, dlycaldt
    ),

    yearly_permnos AS (
        SELECT
            year,
            COUNT(DISTINCT permno) AS n_unique_permnos
        FROM universe
        GROUP BY year
    )

    SELECT
        d.year,
        SUM(d.n_daily) AS n_rows,
        COUNT(*) AS n_days,
        p.n_unique_permnos,
        AVG(d.n_daily) AS avg_daily_universe_size,
        MIN(d.n_daily) AS min_daily_universe_size,
        MAX(d.n_daily) AS max_daily_universe_size
    FROM daily_counts d
    JOIN yearly_permnos p
      ON d.year = p.year
    GROUP BY d.year, p.n_unique_permnos
    ORDER BY d.year
""").df()

yearly_summary.to_csv(OUT_YEARLY_SUMMARY, index=False)


print("\nFinished creating daily stock universe.")
print(f"Universe parquet:       {OUT_UNIVERSE}")
print(f"Daily summary CSV:      {OUT_DAILY_SUMMARY}")
print(f"Yearly summary CSV:     {OUT_YEARLY_SUMMARY}")

print("\nYearly summary:")
print(yearly_summary.to_string(index=False))

print("\nRecent universe sample:")
print(con.execute(f"""
    SELECT
        dlycaldt,
        permno,
        ticker,
        primaryexch,
        securitytype,
        securitysubtype,
        sharetype,
        prc,
        dlyvol,
        dlycap,
        adv20,
        market_cap_rank,
        adv20_rank,
        hist_ret_obs_252,
        has_ohlc_flag,
        price_from_bidask_flag,
        bidask_missing_flag
    FROM read_parquet('{OUT_UNIVERSE}/**/*.parquet', hive_partitioning=true)
    ORDER BY dlycaldt DESC, market_cap_rank
    LIMIT 30
""").df().to_string(index=False))