from pathlib import Path
import shutil
import duckdb


ROOT = Path("/home/xul9527/us-stock")

CORE_GLOB = ROOT / "data/clean_parquet/daily_core/**/*.parquet"

OUT_DEDUP = ROOT / "data/clean_parquet/daily_core_dup_removed"
OUT_ANALYSIS = ROOT / "data/clean_parquet/return_missing_analysis"

if OUT_DEDUP.exists():
    shutil.rmtree(OUT_DEDUP)

OUT_DEDUP.parent.mkdir(parents=True, exist_ok=True)
OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))

# More stable for large DuckDB operations in WSL.
con.execute("PRAGMA threads=2")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")


# ---------------------------------------------------------------------
# Step 1. De-duplicate daily_core.
#
# We preserve exactly one row for each (permno, dlycaldt).
# If duplicated rows exist, keep the first row according to:
#   source_file, raw_row_id
#
# This is deterministic and avoids duplicated stock-date pairs in later
# return-label construction.
# ---------------------------------------------------------------------

print("\nCreating de-duplicated daily_core...")

con.execute(f"""
COPY (
    SELECT * EXCLUDE (dup_row_number)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY permno, dlycaldt
                ORDER BY source_file, raw_row_id
            ) AS dup_row_number
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
    )
    WHERE dup_row_number = 1
)
TO '{OUT_DEDUP}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")

print(f"Saved de-duplicated daily_core to: {OUT_DEDUP}")


# ---------------------------------------------------------------------
# Step 2. Basic duplicate-removal sanity check.
# ---------------------------------------------------------------------

dedup_glob = OUT_DEDUP / "**/*.parquet"

dup_check = con.execute(f"""
    WITH stock_days AS (
        SELECT
            permno,
            dlycaldt,
            COUNT(*) AS n_rows
        FROM read_parquet('{dedup_glob}', hive_partitioning=true)
        GROUP BY permno, dlycaldt
    )

    SELECT
        SUM(n_rows) AS n_total_rows,
        COUNT(*) AS n_unique_stock_days,
        SUM(CASE WHEN n_rows > 1 THEN 1 ELSE 0 END) AS n_duplicate_stock_days,
        SUM(CASE WHEN n_rows > 1 THEN n_rows - 1 ELSE 0 END) AS n_extra_duplicate_rows,
        MAX(n_rows) AS max_rows_per_stock_day
    FROM stock_days
""").df()

dup_check_path = OUT_ANALYSIS / "daily_core_dup_removed_duplicate_check.csv"
dup_check.to_csv(dup_check_path, index=False)

print("\nDuplicate check after de-duplication:")
print(dup_check.to_string(index=False))
print(f"Saved to: {dup_check_path}")


# ---------------------------------------------------------------------
# Step 3. For all missing dlyret, check whether dlyretmissflg is missing.
#
# If dlyret is missing and dlyretmissflg is also NULL, save full rows
# for manual inspection.
# ---------------------------------------------------------------------

print("\nSaving rows where dlyret is missing and dlyretmissflg is NULL...")

missing_ret_no_flag_path = OUT_ANALYSIS / "missing_dlyret_without_dlyretmissflg_rows.csv"

con.execute(f"""
COPY (
    SELECT *
    FROM read_parquet('{dedup_glob}', hive_partitioning=true)
    WHERE dlyret IS NULL
      AND dlyretmissflg IS NULL
    ORDER BY dlycaldt, permno
)
TO '{missing_ret_no_flag_path}'
(HEADER, DELIMITER ',');
""")

print(f"Saved to: {missing_ret_no_flag_path}")


# ---------------------------------------------------------------------
# Step 4. For missing dlyret rows with non-missing dlyretmissflg,
# value-count tradingstatusflg.
# ---------------------------------------------------------------------

print("\nSaving tradingstatusflg counts for missing dlyret WITH dlyretmissflg...")

with_flag_counts = con.execute(f"""
    SELECT
        tradingstatusflg,
        COUNT(*) AS n_rows
    FROM read_parquet('{dedup_glob}', hive_partitioning=true)
    WHERE dlyret IS NULL
      AND dlyretmissflg IS NOT NULL
    GROUP BY tradingstatusflg
    ORDER BY n_rows DESC
""").df()

with_flag_counts_path = OUT_ANALYSIS / "missing_dlyret_with_dlyretmissflg_tradingstatus_counts.csv"
with_flag_counts.to_csv(with_flag_counts_path, index=False)

print(with_flag_counts.to_string(index=False))
print(f"Saved to: {with_flag_counts_path}")


# ---------------------------------------------------------------------
# Step 5. For missing dlyret rows without dlyretmissflg,
# value-count tradingstatusflg.
# ---------------------------------------------------------------------

print("\nSaving tradingstatusflg counts for missing dlyret WITHOUT dlyretmissflg...")

without_flag_counts = con.execute(f"""
    SELECT
        tradingstatusflg,
        COUNT(*) AS n_rows
    FROM read_parquet('{dedup_glob}', hive_partitioning=true)
    WHERE dlyret IS NULL
      AND dlyretmissflg IS NULL
    GROUP BY tradingstatusflg
    ORDER BY n_rows DESC
""").df()

without_flag_counts_path = OUT_ANALYSIS / "missing_dlyret_without_dlyretmissflg_tradingstatus_counts.csv"
without_flag_counts.to_csv(without_flag_counts_path, index=False)

print(without_flag_counts.to_string(index=False))
print(f"Saved to: {without_flag_counts_path}")


# ---------------------------------------------------------------------
# Extra helpful summaries.
# ---------------------------------------------------------------------

print("\nSaving additional return-missing summaries...")

overall_summary = con.execute(f"""
    SELECT
        COUNT(*) AS n_total,
        SUM(CASE WHEN dlyret IS NULL THEN 1 ELSE 0 END) AS n_missing_dlyret,
        SUM(CASE WHEN dlyret IS NULL AND dlyretmissflg IS NOT NULL THEN 1 ELSE 0 END)
            AS n_missing_dlyret_with_dlyretmissflg,
        SUM(CASE WHEN dlyret IS NULL AND dlyretmissflg IS NULL THEN 1 ELSE 0 END)
            AS n_missing_dlyret_without_dlyretmissflg,
        AVG(CASE WHEN dlyret IS NULL THEN 1.0 ELSE 0.0 END) AS missing_dlyret_rate
    FROM read_parquet('{dedup_glob}', hive_partitioning=true)
""").df()

overall_summary_path = OUT_ANALYSIS / "missing_dlyret_overall_summary.csv"
overall_summary.to_csv(overall_summary_path, index=False)

print("\nOverall missing dlyret summary:")
print(overall_summary.to_string(index=False))
print(f"Saved to: {overall_summary_path}")


dlyretmissflg_counts = con.execute(f"""
    SELECT
        COALESCE(CAST(dlyretmissflg AS VARCHAR), '__NULL__') AS dlyretmissflg,
        COUNT(*) AS n_rows
    FROM read_parquet('{dedup_glob}', hive_partitioning=true)
    WHERE dlyret IS NULL
    GROUP BY dlyretmissflg
    ORDER BY n_rows DESC
""").df()

dlyretmissflg_counts_path = OUT_ANALYSIS / "missing_dlyret_dlyretmissflg_counts.csv"
dlyretmissflg_counts.to_csv(dlyretmissflg_counts_path, index=False)

print("\ndlyretmissflg counts among missing dlyret rows:")
print(dlyretmissflg_counts.to_string(index=False))
print(f"Saved to: {dlyretmissflg_counts_path}")


print("\nFinished return missing screening.")
print(f"De-duplicated daily_core: {OUT_DEDUP}")
print(f"Analysis outputs:         {OUT_ANALYSIS}")