from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

MODEL_PANEL_GLOB = ROOT / "data/clean_parquet/model_panel/**/*.parquet"
OUT_DIR = ROOT / "data/clean_parquet/model_panel_analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=2")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")
con.execute("SET max_temp_directory_size='150GB'")


# ------------------------------------------------------------
# Read schema
# ------------------------------------------------------------

schema = con.execute(f"""
    DESCRIBE SELECT *
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
""").df()

schema.columns = [c.lower() for c in schema.columns]

all_cols = schema["column_name"].tolist()

ID_COLS = {
    "permno",
    "dlycaldt",
    "year",
    "ticker",
    "primaryexch",
    "siccd",
    "naics",
    "icbindustry",
}

TARGET_COLS = {
    "target_5d_cs_zscore",
}

numeric_type_keywords = [
    "INTEGER",
    "BIGINT",
    "DOUBLE",
    "FLOAT",
    "REAL",
    "DECIMAL",
    "HUGEINT",
    "UBIGINT",
    "UINTEGER",
]

numeric_cols = []
for _, row in schema.iterrows():
    col = row["column_name"]
    typ = str(row["column_type"]).upper()

    if any(k in typ for k in numeric_type_keywords):
        numeric_cols.append(col)

feature_numeric_cols = [
    c for c in numeric_cols
    if c not in ID_COLS and c not in TARGET_COLS
]

binary_like_cols = [
    c for c in feature_numeric_cols
    if c.endswith("_flag")
]

continuous_feature_cols = [
    c for c in feature_numeric_cols
    if c not in binary_like_cols
]

categorical_cols = [
    c for c in all_cols
    if c not in numeric_cols
]


# ------------------------------------------------------------
# 1. Overall table summary
# ------------------------------------------------------------

summary = con.execute(f"""
    SELECT
        COUNT(*) AS n_rows,
        COUNT(DISTINCT permno) AS n_unique_permnos,
        COUNT(DISTINCT dlycaldt) AS n_dates,
        MIN(dlycaldt) AS min_date,
        MAX(dlycaldt) AS max_date
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
""").df()

summary_path = OUT_DIR / "model_panel_overall_summary.csv"
summary.to_csv(summary_path, index=False)

print("\nOverall summary:")
print(summary.to_string(index=False))
print(f"Saved to: {summary_path}")


# ------------------------------------------------------------
# 2. Missing report for all columns
# ------------------------------------------------------------

total_n = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
""").fetchone()[0]

missing_rows = []

for _, row in schema.iterrows():
    col = row["column_name"]
    typ = row["column_type"]

    n_missing = con.execute(f"""
        SELECT SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    """).fetchone()[0]

    n_missing = int(n_missing or 0)

    missing_rows.append({
        "variable": col,
        "type": typ,
        "n_total": total_n,
        "n_missing": n_missing,
        "missing_pct": n_missing / total_n if total_n > 0 else None,
    })

missing_report = pd.DataFrame(missing_rows)
missing_report = missing_report.sort_values(
    ["missing_pct", "variable"],
    ascending=[False, True],
)

missing_path = OUT_DIR / "model_panel_missing_report.csv"
missing_report.to_csv(missing_path, index=False)

print("\nTop missing variables:")
print(missing_report.head(30).to_string(index=False))
print(f"Saved to: {missing_path}")


# ------------------------------------------------------------
# 3. Numeric distribution report
# ------------------------------------------------------------

dist_rows = []

for col in numeric_cols:
    df = con.execute(f"""
        SELECT
            '{col}' AS variable,
            COUNT(*) AS n_total,
            SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
            MIN({col}) AS min_value,
            APPROX_QUANTILE({col}, 0.001) AS p001,
            APPROX_QUANTILE({col}, 0.01) AS p01,
            APPROX_QUANTILE({col}, 0.05) AS p05,
            APPROX_QUANTILE({col}, 0.10) AS p10,
            APPROX_QUANTILE({col}, 0.25) AS p25,
            APPROX_QUANTILE({col}, 0.50) AS p50,
            APPROX_QUANTILE({col}, 0.75) AS p75,
            APPROX_QUANTILE({col}, 0.90) AS p90,
            APPROX_QUANTILE({col}, 0.95) AS p95,
            APPROX_QUANTILE({col}, 0.99) AS p99,
            APPROX_QUANTILE({col}, 0.999) AS p999,
            MAX({col}) AS max_value,
            AVG(CAST({col} AS DOUBLE)) AS mean_value
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    """).df()

    dist_rows.append(df)

numeric_dist = pd.concat(dist_rows, ignore_index=True)

numeric_dist["missing_pct"] = (
    numeric_dist["n_missing"] / numeric_dist["n_total"]
)

numeric_dist_path = OUT_DIR / "model_panel_numeric_distribution.csv"
numeric_dist.to_csv(numeric_dist_path, index=False)

print("\nNumeric distribution saved to:")
print(numeric_dist_path)


# ------------------------------------------------------------
# 4. Feature-only distribution report
# ------------------------------------------------------------

feature_dist = numeric_dist[
    numeric_dist["variable"].isin(feature_numeric_cols + list(TARGET_COLS))
].copy()

feature_dist_path = OUT_DIR / "model_panel_feature_distribution.csv"
feature_dist.to_csv(feature_dist_path, index=False)

print("Feature distribution saved to:")
print(feature_dist_path)


# ------------------------------------------------------------
# 5. Yearly missingness for important variables
# ------------------------------------------------------------

important_cols = [
    "target_5d_cs_zscore",
]

important_cols += binary_like_cols
important_cols += continuous_feature_cols

yearly_missing_parts = []

for col in important_cols:
    df = con.execute(f"""
        SELECT
            year,
            '{col}' AS variable,
            COUNT(*) AS n_total,
            SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
            AVG(CASE WHEN {col} IS NULL THEN 1.0 ELSE 0.0 END) AS missing_pct
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        GROUP BY year
        ORDER BY year
    """).df()

    yearly_missing_parts.append(df)

yearly_missing = pd.concat(yearly_missing_parts, ignore_index=True)

yearly_missing_path = OUT_DIR / "model_panel_yearly_missing_report.csv"
yearly_missing.to_csv(yearly_missing_path, index=False)

print("Yearly missing report saved to:")
print(yearly_missing_path)


# ------------------------------------------------------------
# 6. Value counts for binary flags
# ------------------------------------------------------------

binary_value_count_parts = []

for col in binary_like_cols:
    df = con.execute(f"""
        SELECT
            '{col}' AS variable,
            CAST({col} AS VARCHAR) AS value,
            COUNT(*) AS n_rows,
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pct
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        GROUP BY value
        ORDER BY value
    """).df()

    binary_value_count_parts.append(df)

if binary_value_count_parts:
    binary_counts = pd.concat(binary_value_count_parts, ignore_index=True)
else:
    binary_counts = pd.DataFrame(
        columns=["variable", "value", "n_rows", "pct"]
    )

binary_counts_path = OUT_DIR / "model_panel_binary_feature_value_counts.csv"
binary_counts.to_csv(binary_counts_path, index=False)

print("Binary feature value counts saved to:")
print(binary_counts_path)


# ------------------------------------------------------------
# 7. Value counts for selected categorical variables
# ------------------------------------------------------------

selected_categorical_cols = [
    c for c in ["primaryexch", "siccd", "naics", "icbindustry"]
    if c in all_cols
]

cat_parts = []

for col in selected_categorical_cols:
    df = con.execute(f"""
        SELECT
            '{col}' AS variable,
            COALESCE(CAST({col} AS VARCHAR), '__MISSING__') AS value,
            COUNT(*) AS n_rows,
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pct
        FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
        GROUP BY value
        ORDER BY n_rows DESC
        LIMIT 100
    """).df()

    cat_parts.append(df)

if cat_parts:
    categorical_counts = pd.concat(cat_parts, ignore_index=True)
else:
    categorical_counts = pd.DataFrame(
        columns=["variable", "value", "n_rows", "pct"]
    )

categorical_counts_path = OUT_DIR / "model_panel_categorical_value_counts.csv"
categorical_counts.to_csv(categorical_counts_path, index=False)

print("Categorical value counts saved to:")
print(categorical_counts_path)


# ------------------------------------------------------------
# 8. Daily target distribution and cross-sectional size
# ------------------------------------------------------------

daily_target = con.execute(f"""
    SELECT
        dlycaldt,
        year,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT permno) AS n_permnos,
        AVG(target_5d_cs_zscore) AS target_mean,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.01) AS target_p01,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.25) AS target_p25,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.50) AS target_p50,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.75) AS target_p75,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.99) AS target_p99
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    GROUP BY dlycaldt, year
    ORDER BY dlycaldt
""").df()

daily_target_path = OUT_DIR / "model_panel_daily_target_distribution.csv"
daily_target.to_csv(daily_target_path, index=False)

print("Daily target distribution saved to:")
print(daily_target_path)


# ------------------------------------------------------------
# 9. Yearly row counts and target distribution
# ------------------------------------------------------------

yearly_summary = con.execute(f"""
    SELECT
        year,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT dlycaldt) AS n_dates,
        COUNT(DISTINCT permno) AS n_permnos,
        AVG(target_5d_cs_zscore) AS target_mean,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.01) AS target_p01,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.50) AS target_p50,
        APPROX_QUANTILE(target_5d_cs_zscore, 0.99) AS target_p99
    FROM read_parquet('{MODEL_PANEL_GLOB}', hive_partitioning=true)
    GROUP BY year
    ORDER BY year
""").df()

yearly_summary_path = OUT_DIR / "model_panel_yearly_summary.csv"
yearly_summary.to_csv(yearly_summary_path, index=False)

print("\nYearly summary:")
print(yearly_summary.to_string(index=False))
print(f"Saved to: {yearly_summary_path}")


print("\nFinished analyzing model_panel.")
print(f"All outputs saved under: {OUT_DIR}")