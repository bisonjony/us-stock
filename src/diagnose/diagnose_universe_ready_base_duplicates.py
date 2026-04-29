from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

BASE_GLOB = ROOT / "data/clean_parquet/daily_universe_ready_base/**/*.parquet"
OUT_DIR = ROOT / "data/clean_parquet/universe_ready_base_duplicate_diagnostics"
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


# ---------------------------------------------------------------------
# 1. Overall duplicate summary
# ---------------------------------------------------------------------

save_query(
    "01_duplicate_summary",
    f"""
    WITH stock_days AS (
        SELECT
            permno,
            dlycaldt,
            COUNT(*) AS n_rows
        FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
        GROUP BY permno, dlycaldt
    )

    SELECT
        SUM(n_rows) AS n_total_rows,
        COUNT(*) AS n_unique_stock_days,
        SUM(CASE WHEN n_rows > 1 THEN 1 ELSE 0 END) AS n_duplicate_stock_days,
        SUM(CASE WHEN n_rows > 1 THEN n_rows - 1 ELSE 0 END) AS n_extra_duplicate_rows,
        MAX(n_rows) AS max_rows_per_stock_day
    FROM stock_days
    """
)


# ---------------------------------------------------------------------
# 2. Distribution of number of rows per stock-day
# ---------------------------------------------------------------------

save_query(
    "02_rows_per_stock_day_distribution",
    f"""
    WITH stock_days AS (
        SELECT
            permno,
            dlycaldt,
            COUNT(*) AS n_rows
        FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
        GROUP BY permno, dlycaldt
    )

    SELECT
        n_rows,
        COUNT(*) AS n_stock_days
    FROM stock_days
    GROUP BY n_rows
    ORDER BY n_rows
    """
)


# ---------------------------------------------------------------------
# 3. Duplicate count by year
# ---------------------------------------------------------------------

save_query(
    "03_duplicate_summary_by_year",
    f"""
    WITH stock_days AS (
        SELECT
            year,
            permno,
            dlycaldt,
            COUNT(*) AS n_rows
        FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
        GROUP BY year, permno, dlycaldt
    )

    SELECT
        year,
        SUM(n_rows) AS n_total_rows,
        COUNT(*) AS n_unique_stock_days,
        SUM(CASE WHEN n_rows > 1 THEN 1 ELSE 0 END) AS n_duplicate_stock_days,
        SUM(CASE WHEN n_rows > 1 THEN n_rows - 1 ELSE 0 END) AS n_extra_duplicate_rows,
        MAX(n_rows) AS max_rows_per_stock_day
    FROM stock_days
    GROUP BY year
    ORDER BY year
    """
)


# ---------------------------------------------------------------------
# 4. Duplicate stock-days with compact event summary
# ---------------------------------------------------------------------

save_query(
    "04_duplicate_stock_days_summary",
    f"""
    SELECT
        permno,
        dlycaldt,
        ticker,
        year,
        COUNT(*) AS n_rows,

        MIN(raw_row_id) AS min_raw_row_id,
        MAX(raw_row_id) AS max_raw_row_id,

        STRING_AGG(DISTINCT COALESCE(CAST(distype AS VARCHAR), '__NULL__'), ',') AS distype_set,
        STRING_AGG(DISTINCT COALESCE(CAST(disseqnbr AS VARCHAR), '__NULL__'), ',') AS disseqnbr_set,
        STRING_AGG(DISTINCT COALESCE(CAST(disordinaryflg AS VARCHAR), '__NULL__'), ',') AS disordinaryflg_set,
        STRING_AGG(DISTINCT COALESCE(CAST(dispaymenttype AS VARCHAR), '__NULL__'), ',') AS dispaymenttype_set,
        STRING_AGG(DISTINCT COALESCE(CAST(disdetailtype AS VARCHAR), '__NULL__'), ',') AS disdetailtype_set,

        SUM(CASE WHEN distype IS NOT NULL THEN 1 ELSE 0 END) AS n_distribution_rows,
        SUM(CASE WHEN disdivamt IS NOT NULL THEN 1 ELSE 0 END) AS n_disdivamt_rows,

        MIN(disdivamt) AS min_disdivamt,
        MAX(disdivamt) AS max_disdivamt,
        MIN(disfacpr) AS min_disfacpr,
        MAX(disfacpr) AS max_disfacpr,
        MIN(disfacshr) AS min_disfacshr,
        MAX(disfacshr) AS max_disfacshr

    FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
    GROUP BY permno, dlycaldt, ticker, year
    HAVING COUNT(*) > 1
    ORDER BY n_rows DESC, dlycaldt, permno
    """
)


# ---------------------------------------------------------------------
# 5. Check whether duplicated rows differ in core trading fields.
# If these counts are > 1, duplicates differ beyond distribution metadata.
# ---------------------------------------------------------------------

save_query(
    "05_duplicate_core_field_differences",
    f"""
    SELECT
        permno,
        dlycaldt,
        ticker,
        year,
        COUNT(*) AS n_rows,

        COUNT(DISTINCT COALESCE(CAST(dlyprc AS VARCHAR), '__NULL__')) AS n_distinct_dlyprc,
        COUNT(DISTINCT COALESCE(CAST(prc AS VARCHAR), '__NULL__')) AS n_distinct_prc,
        COUNT(DISTINCT COALESCE(CAST(dlyret AS VARCHAR), '__NULL__')) AS n_distinct_dlyret,
        COUNT(DISTINCT COALESCE(CAST(dlyretx AS VARCHAR), '__NULL__')) AS n_distinct_dlyretx,
        COUNT(DISTINCT COALESCE(CAST(dlyreti AS VARCHAR), '__NULL__')) AS n_distinct_dlyreti,
        COUNT(DISTINCT COALESCE(CAST(dlyvol AS VARCHAR), '__NULL__')) AS n_distinct_dlyvol,
        COUNT(DISTINCT COALESCE(CAST(dlycap AS VARCHAR), '__NULL__')) AS n_distinct_dlycap,

        COUNT(DISTINCT COALESCE(CAST(dlyopen AS VARCHAR), '__NULL__')) AS n_distinct_dlyopen,
        COUNT(DISTINCT COALESCE(CAST(dlyhigh AS VARCHAR), '__NULL__')) AS n_distinct_dlyhigh,
        COUNT(DISTINCT COALESCE(CAST(dlylow AS VARCHAR), '__NULL__')) AS n_distinct_dlylow,
        COUNT(DISTINCT COALESCE(CAST(dlyclose AS VARCHAR), '__NULL__')) AS n_distinct_dlyclose,

        COUNT(DISTINCT COALESCE(CAST(dlybid AS VARCHAR), '__NULL__')) AS n_distinct_dlybid,
        COUNT(DISTINCT COALESCE(CAST(dlyask AS VARCHAR), '__NULL__')) AS n_distinct_dlyask,

        COUNT(DISTINCT COALESCE(CAST(distype AS VARCHAR), '__NULL__')) AS n_distinct_distype,
        COUNT(DISTINCT COALESCE(CAST(disseqnbr AS VARCHAR), '__NULL__')) AS n_distinct_disseqnbr

    FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
    GROUP BY permno, dlycaldt, ticker, year
    HAVING COUNT(*) > 1
    ORDER BY
        n_distinct_dlyret DESC,
        n_distinct_dlyprc DESC,
        n_distinct_dlyvol DESC,
        n_rows DESC,
        dlycaldt,
        permno
    """
)


# ---------------------------------------------------------------------
# 6. Aggregate how many duplicate stock-days differ in each core field.
# This tells us whether duplicates are mostly distribution metadata only.
# ---------------------------------------------------------------------

save_query(
    "06_duplicate_difference_rate_summary",
    f"""
    WITH dup_diff AS (
        SELECT
            permno,
            dlycaldt,
            COUNT(*) AS n_rows,

            COUNT(DISTINCT COALESCE(CAST(dlyprc AS VARCHAR), '__NULL__')) AS n_distinct_dlyprc,
            COUNT(DISTINCT COALESCE(CAST(prc AS VARCHAR), '__NULL__')) AS n_distinct_prc,
            COUNT(DISTINCT COALESCE(CAST(dlyret AS VARCHAR), '__NULL__')) AS n_distinct_dlyret,
            COUNT(DISTINCT COALESCE(CAST(dlyretx AS VARCHAR), '__NULL__')) AS n_distinct_dlyretx,
            COUNT(DISTINCT COALESCE(CAST(dlyreti AS VARCHAR), '__NULL__')) AS n_distinct_dlyreti,
            COUNT(DISTINCT COALESCE(CAST(dlyvol AS VARCHAR), '__NULL__')) AS n_distinct_dlyvol,
            COUNT(DISTINCT COALESCE(CAST(dlycap AS VARCHAR), '__NULL__')) AS n_distinct_dlycap,
            COUNT(DISTINCT COALESCE(CAST(dlyopen AS VARCHAR), '__NULL__')) AS n_distinct_dlyopen,
            COUNT(DISTINCT COALESCE(CAST(dlyhigh AS VARCHAR), '__NULL__')) AS n_distinct_dlyhigh,
            COUNT(DISTINCT COALESCE(CAST(dlylow AS VARCHAR), '__NULL__')) AS n_distinct_dlylow,
            COUNT(DISTINCT COALESCE(CAST(dlyclose AS VARCHAR), '__NULL__')) AS n_distinct_dlyclose,
            COUNT(DISTINCT COALESCE(CAST(dlybid AS VARCHAR), '__NULL__')) AS n_distinct_dlybid,
            COUNT(DISTINCT COALESCE(CAST(dlyask AS VARCHAR), '__NULL__')) AS n_distinct_dlyask,
            COUNT(DISTINCT COALESCE(CAST(distype AS VARCHAR), '__NULL__')) AS n_distinct_distype,
            COUNT(DISTINCT COALESCE(CAST(disseqnbr AS VARCHAR), '__NULL__')) AS n_distinct_disseqnbr

        FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
        GROUP BY permno, dlycaldt
        HAVING COUNT(*) > 1
    )

    SELECT
        COUNT(*) AS n_duplicate_stock_days,

        SUM(CASE WHEN n_distinct_dlyprc > 1 THEN 1 ELSE 0 END) AS n_diff_dlyprc,
        SUM(CASE WHEN n_distinct_prc > 1 THEN 1 ELSE 0 END) AS n_diff_prc,
        SUM(CASE WHEN n_distinct_dlyret > 1 THEN 1 ELSE 0 END) AS n_diff_dlyret,
        SUM(CASE WHEN n_distinct_dlyretx > 1 THEN 1 ELSE 0 END) AS n_diff_dlyretx,
        SUM(CASE WHEN n_distinct_dlyreti > 1 THEN 1 ELSE 0 END) AS n_diff_dlyreti,
        SUM(CASE WHEN n_distinct_dlyvol > 1 THEN 1 ELSE 0 END) AS n_diff_dlyvol,
        SUM(CASE WHEN n_distinct_dlycap > 1 THEN 1 ELSE 0 END) AS n_diff_dlycap,
        SUM(CASE WHEN n_distinct_dlyopen > 1 THEN 1 ELSE 0 END) AS n_diff_dlyopen,
        SUM(CASE WHEN n_distinct_dlyhigh > 1 THEN 1 ELSE 0 END) AS n_diff_dlyhigh,
        SUM(CASE WHEN n_distinct_dlylow > 1 THEN 1 ELSE 0 END) AS n_diff_dlylow,
        SUM(CASE WHEN n_distinct_dlyclose > 1 THEN 1 ELSE 0 END) AS n_diff_dlyclose,
        SUM(CASE WHEN n_distinct_dlybid > 1 THEN 1 ELSE 0 END) AS n_diff_dlybid,
        SUM(CASE WHEN n_distinct_dlyask > 1 THEN 1 ELSE 0 END) AS n_diff_dlyask,
        SUM(CASE WHEN n_distinct_distype > 1 THEN 1 ELSE 0 END) AS n_diff_distype,
        SUM(CASE WHEN n_distinct_disseqnbr > 1 THEN 1 ELSE 0 END) AS n_diff_disseqnbr
    FROM dup_diff
    """
)


# ---------------------------------------------------------------------
# 7. Full duplicate examples.
# Useful for manual inspection.
# ---------------------------------------------------------------------

save_query(
    "07_duplicate_full_examples",
    f"""
    WITH duplicate_keys AS (
        SELECT
            permno,
            dlycaldt
        FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
        GROUP BY permno, dlycaldt
        HAVING COUNT(*) > 1
    )

    SELECT b.*
    FROM read_parquet('{BASE_GLOB}', hive_partitioning=true) b
    INNER JOIN duplicate_keys k
      ON b.permno = k.permno
     AND b.dlycaldt = k.dlycaldt
    ORDER BY b.dlycaldt, b.permno, b.raw_row_id
    LIMIT 500
    """
)


# ---------------------------------------------------------------------
# 8. Compact duplicate examples focused on fields likely causing duplicates.
# ---------------------------------------------------------------------

save_query(
    "08_duplicate_compact_examples",
    f"""
    WITH duplicate_keys AS (
        SELECT
            permno,
            dlycaldt
        FROM read_parquet('{BASE_GLOB}', hive_partitioning=true)
        GROUP BY permno, dlycaldt
        HAVING COUNT(*) > 1
    )

    SELECT
        b.dlycaldt,
        b.permno,
        b.ticker,
        b.raw_row_id,
        b.prc,
        b.dlyret,
        b.dlyretx,
        b.dlyreti,
        b.dlyvol,
        b.dlycap,
        b.distype,
        b.disseqnbr,
        b.disordinaryflg,
        b.dispaymenttype,
        b.disdetailtype,
        b.disdivamt,
        b.disfacpr,
        b.disfacshr,
        b.dlyorddivamt,
        b.dlynonorddivamt,
        b.dlyfacprc,
        b.dlydistretflg
    FROM read_parquet('{BASE_GLOB}', hive_partitioning=true) b
    INNER JOIN duplicate_keys k
      ON b.permno = k.permno
     AND b.dlycaldt = k.dlycaldt
    ORDER BY b.dlycaldt, b.permno, b.raw_row_id
    LIMIT 500
    """
)


print(f"\nSaved duplicate diagnostics to: {OUT_DIR}")