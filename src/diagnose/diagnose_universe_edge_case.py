from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")
UNIVERSE_GLOB = ROOT / "data/clean_parquet/daily_stock_universe/**/*.parquet"
OUT_DIR = ROOT / "data/clean_parquet/universe_edge_case_diagnostics"
OUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 0)


def save_query(name: str, query: str):
    df = con.execute(query).df()
    out_path = OUT_DIR / f"{name}.csv"
    df.to_csv(out_path, index=False)
    print(f"\n=== {name} ===")
    print(df.head(50).to_string(index=False))
    print(f"Saved to: {out_path}")
    return df


# 1. OHLC missingness by price flag and quote source.
save_query(
    "01_ohlc_missing_by_price_flag",
    f"""
    SELECT
        dlyprcflg,
        price_from_bidask_flag,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN dlyopen IS NULL THEN 1 ELSE 0 END) AS n_missing_open,
        SUM(CASE WHEN dlyclose IS NULL THEN 1 ELSE 0 END) AS n_missing_close,
        SUM(CASE WHEN dlyhigh IS NULL THEN 1 ELSE 0 END) AS n_missing_high,
        SUM(CASE WHEN dlylow IS NULL THEN 1 ELSE 0 END) AS n_missing_low,
        AVG(CASE WHEN dlyclose IS NULL THEN 1.0 ELSE 0.0 END) AS close_missing_rate
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    GROUP BY dlyprcflg, price_from_bidask_flag
    HAVING n_missing_open > 0
        OR n_missing_close > 0
        OR n_missing_high > 0
        OR n_missing_low > 0
    ORDER BY n_missing_close DESC
    """
)


# 2. Random examples where OHLC block is missing.
save_query(
    "02_ohlc_missing_examples",
    f"""
    SELECT *
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE dlyopen IS NULL
       OR dlyclose IS NULL
       OR dlyhigh IS NULL
       OR dlylow IS NULL
    ORDER BY RANDOM()
    LIMIT 50
    """
)


# 3. The 4 rows where open is missing but close/high/low exist.
save_query(
    "03_open_missing_but_other_ohlc_present",
    f"""
    SELECT *
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE dlyopen IS NULL
      AND dlyclose IS NOT NULL
      AND dlyhigh IS NOT NULL
      AND dlylow IS NOT NULL
    ORDER BY dlycaldt, permno
    """
)


# 4. OHLC consistency error examples.
save_query(
    "04_ohlc_inconsistent_examples",
    f"""
    SELECT
        dlycaldt,
        permno,
        ticker,
        dlyopen,
        dlyhigh,
        dlylow,
        dlyclose,
        dlyprc,
        prc,
        dlyprcflg,
        dlyvol,
        dlyret
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE dlyhigh IS NOT NULL
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
    ORDER BY dlycaldt, permno
    """
)


# 5. Diagnose why bid_ask_spread is missing.
save_query(
    "05_bidask_spread_missing_reasons",
    f"""
    SELECT
        CASE
            WHEN dlybid IS NULL OR dlyask IS NULL THEN 'bid_or_ask_missing'
            WHEN dlybid <= 0 THEN 'bid_nonpositive'
            WHEN dlyask <= 0 THEN 'ask_nonpositive'
            WHEN dlyask < dlybid THEN 'ask_less_than_bid'
            ELSE 'other'
        END AS reason,
        COUNT(*) AS n_rows
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE bid_ask_spread IS NULL
    GROUP BY reason
    ORDER BY n_rows DESC
    """
)


# 6. Examples where bid/ask are missing.
save_query(
    "06_bidask_missing_examples",
    f"""
    SELECT *
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE dlybid IS NULL OR dlyask IS NULL
    ORDER BY dlycaldt, permno
    """
)


# 7. Examples where bid/ask exist but spread is missing.
save_query(
    "07_bidask_invalid_examples",
    f"""
    SELECT
        dlycaldt,
        permno,
        ticker,
        dlybid,
        dlyask,
        dlyprc,
        prc,
        dlyprcflg,
        dlyvol,
        dlyret,
        bid_ask_spread
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE bid_ask_spread IS NULL
      AND dlybid IS NOT NULL
      AND dlyask IS NOT NULL
    ORDER BY dlycaldt, permno
    LIMIT 100
    """
)


# 8. Non-missing return-missing flag values.
save_query(
    "08_dlyretmissflg_nonmissing_values",
    f"""
    SELECT
        dlyretmissflg,
        COUNT(*) AS n_rows
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE dlyretmissflg IS NOT NULL
    GROUP BY dlyretmissflg
    ORDER BY n_rows DESC
    """
)


# 9. Examples for non-missing dlyretmissflg.
save_query(
    "09_dlyretmissflg_nonmissing_examples",
    f"""
    SELECT *
    FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true)
    WHERE dlyretmissflg IS NOT NULL
    ORDER BY dlycaldt, permno
    """
)


print(f"\nSaved all diagnostics to: {OUT_DIR}")