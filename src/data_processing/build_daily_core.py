from pathlib import Path
import shutil
import duckdb


ROOT = Path("/home/xul9527/us-stock")

RAW_GLOB = ROOT / "data/data_parquet/us_stock_19_26_raw_part_*.parquet"
OUT_DIR = ROOT / "data/clean_parquet/daily_core"
FAIL_DIR = ROOT / "data/clean_parquet/daily_core_cast_failures"
FAIL_SAMPLE_CSV = ROOT / "data/clean_parquet/daily_core_cast_failures_sample.csv"

if OUT_DIR.exists():
    shutil.rmtree(OUT_DIR)

if FAIL_DIR.exists():
    shutil.rmtree(FAIL_DIR)

OUT_DIR.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))

con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")


# ---------------------------------------------------------------------
# Variable dictionary
# ---------------------------------------------------------------------

VARIABLES = [
    ("permno", "int"),
    ("secinfostartdt", "date"),
    ("secinfoenddt", "date"),
    ("securitybegdt", "date"),
    ("securityenddt", "date"),
    ("securityhdrflg", "char"),
    ("hdrcusip", "char"),
    ("hdrcusip9", "char"),
    ("cusip", "char"),
    ("cusip9", "char"),
    ("primaryexch", "char"),
    ("conditionaltype", "char"),
    ("exchangetier", "char"),
    ("tradingstatusflg", "char"),
    ("securitynm", "char"),
    ("shareclass", "char"),
    ("usincflg", "char"),
    ("issuertype", "char"),
    ("securitytype", "char"),
    ("securitysubtype", "char"),
    ("sharetype", "char"),
    ("securityactiveflg", "char"),
    ("delactiontype", "char"),
    ("delstatustype", "char"),
    ("delreasontype", "char"),
    ("delpaymenttype", "char"),
    ("ticker", "char"),
    ("tradingsymbol", "char"),
    ("permco", "int"),
    ("siccd", "int"),
    ("naics", "char"),
    ("icbindustry", "char"),
    ("issuernm", "char"),
    ("yyyymmdd", "int"),
    ("dlycaldt", "date"),
    ("dlydelflg", "char"),
    ("dlyprc", "decimal"),
    ("dlyprcflg", "char"),
    ("dlycap", "decimal"),
    ("dlycapflg", "char"),
    ("dlyprevprc", "decimal"),
    ("dlyprevprcflg", "char"),
    ("dlyprevdt", "date"),
    ("dlyprevcap", "decimal"),
    ("dlyprevcapflg", "char"),
    ("dlyret", "decimal"),
    ("dlyretx", "decimal"),
    ("dlyreti", "decimal"),
    ("dlyretmissflg", "char"),
    ("dlyretdurflg", "char"),
    ("dlyorddivamt", "decimal"),
    ("dlynonorddivamt", "decimal"),
    ("dlyfacprc", "decimal"),
    ("dlydistretflg", "char"),
    ("dlyvol", "decimal"),
    ("dlyclose", "decimal"),
    ("dlylow", "decimal"),
    ("dlyhigh", "decimal"),
    ("dlybid", "decimal"),
    ("dlyask", "decimal"),
    ("dlyopen", "decimal"),
    ("dlynumtrd", "int"),
    ("dlymmcnt", "int"),
    ("dlyprcvol", "decimal"),
    ("shrstartdt", "date"),
    ("shrenddt", "date"),
    ("shrout", "int"),
    ("shrsource", "char"),
    ("shrfactype", "char"),
    ("shradrflg", "char"),
    ("disexdt", "date"),
    ("disseqnbr", "int"),
    ("disordinaryflg", "char"),
    ("distype", "char"),
    ("disfreqtype", "char"),
    ("dispaymenttype", "char"),
    ("disdetailtype", "char"),
    ("distaxtype", "char"),
    ("disorigcurtype", "char"),
    ("disdivamt", "decimal"),
    ("disfacpr", "decimal"),
    ("disfacshr", "decimal"),
    ("disdeclaredt", "date"),
    ("disrecorddt", "date"),
    ("dispaydt", "date"),
    ("dispermno", "int"),
    ("dispermco", "int"),
    ("nasdcompno", "int"),
    ("nasdissuno", "int"),
    ("vwretd", "decimal"),
    ("vwretx", "decimal"),
    ("ewretd", "decimal"),
    ("ewretx", "decimal"),
    ("sprtrn", "decimal"),
]


# ---------------------------------------------------------------------
# SQL expression builders
# ---------------------------------------------------------------------

def cleaned(col: str) -> str:
    return f"NULLIF(TRIM({col}), '')"


def typed_expr(col: str, typ: str) -> str:
    c = cleaned(col)

    if typ == "int":
        return f"TRY_CAST({c} AS BIGINT) AS {col}"

    if typ == "decimal":
        return f"TRY_CAST({c} AS DOUBLE) AS {col}"

    if typ == "date":
        return f"TRY_CAST({c} AS DATE) AS {col}"

    if typ == "char":
        return f"{c} AS {col}"

    raise ValueError(f"Unknown type: {typ}")


def fail_condition(col: str, typ: str) -> str | None:
    c = cleaned(col)

    if typ == "int":
        return f"({c} IS NOT NULL AND TRY_CAST({c} AS BIGINT) IS NULL)"

    if typ == "decimal":
        return f"({c} IS NOT NULL AND TRY_CAST({c} AS DOUBLE) IS NULL)"

    if typ == "date":
        return f"({c} IS NOT NULL AND TRY_CAST({c} AS DATE) IS NULL)"

    return None


raw_lower_exprs = ",\n        ".join(
    [f"{col} AS {col}" for col, _ in VARIABLES]
)

typed_exprs = ",\n        ".join(
    [typed_expr(col, typ) for col, typ in VARIABLES]
)

cast_fail_exprs = []
cast_fail_cols = []
failed_column_name_exprs = []

for col, typ in VARIABLES:
    cond = fail_condition(col, typ)

    if cond is not None:
        cast_fail_exprs.append(f"{cond} AS {col}_cast_failed")
        cast_fail_cols.append(f"{col}_cast_failed")
        failed_column_name_exprs.append(
            f"CASE WHEN {cond} THEN '{col};' ELSE '' END"
        )

cast_fail_exprs_sql = ",\n        ".join(cast_fail_exprs)

any_cast_fail_sql = " OR\n        ".join(cast_fail_cols)

failed_columns_sql = " ||\n            ".join(failed_column_name_exprs)


# ---------------------------------------------------------------------
# 1. Lowercase raw column names
# ---------------------------------------------------------------------
# DuckDB identifiers are case-insensitive, so this works even if the
# source columns are HdrCUSIP, HdrCUSIP9, DlyRetMissFlg, etc.
# The output aliases are lowercase.

con.execute(f"""
CREATE OR REPLACE VIEW raw_norm AS
SELECT
    filename AS source_file,
    ROW_NUMBER() OVER () AS raw_row_id,
    {raw_lower_exprs}
FROM read_parquet(
    '{RAW_GLOB}',
    union_by_name=true,
    filename=true
);
""")


# ---------------------------------------------------------------------
# 2. Build cast-failure table
# ---------------------------------------------------------------------
# Missing values such as empty strings are not counted as cast failures.
# A cast failure means: raw value is non-empty, but TRY_CAST returns NULL.

con.execute(f"""
CREATE OR REPLACE TEMP VIEW cast_check AS
SELECT
    source_file,
    raw_row_id,
    {raw_lower_exprs},
    {cast_fail_exprs_sql},
    REGEXP_REPLACE(
        {failed_columns_sql},
        ';$',
        ''
    ) AS failed_columns
FROM raw_norm;
""")

failure_count = con.execute(f"""
SELECT COUNT(*)
FROM cast_check
WHERE {any_cast_fail_sql};
""").fetchone()[0]

print(f"Number of rows with at least one cast failure: {failure_count:,}")

if failure_count > 0:
    con.execute(f"""
    COPY (
        SELECT *
        FROM cast_check
        WHERE {any_cast_fail_sql}
    )
    TO '{FAIL_DIR}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    failure_sample = con.execute(f"""
        SELECT
            source_file,
            raw_row_id,
            failed_columns
        FROM cast_check
        WHERE {any_cast_fail_sql}
        LIMIT 200
    """).df()

    failure_sample.to_csv(FAIL_SAMPLE_CSV, index=False)
    print(f"Saved cast-failure rows to: {FAIL_DIR}")
    print(f"Saved cast-failure sample to: {FAIL_SAMPLE_CSV}")


# ---------------------------------------------------------------------
# 3. Build typed daily_core table
# ---------------------------------------------------------------------
# This step does not drop any original variable.
# It adds:
#   year
#   prc
#   dlyprc_negative_flag
#
# dlyprc remains the original typed CRSP price value.
# prc is the positive price magnitude for later modeling/filtering.

con.execute(f"""
COPY (
    WITH typed AS (
        SELECT
            source_file,
            raw_row_id,
            {typed_exprs}
        FROM raw_norm
    )

    SELECT
        *,
        DATE_PART('year', dlycaldt)::INTEGER AS year,

        CASE
            WHEN dlyprc IS NULL THEN NULL
            WHEN dlyprc < 0 THEN TRUE
            ELSE FALSE
        END AS dlyprc_negative_flag,

        ABS(dlyprc) AS prc

    FROM typed
)
TO '{OUT_DIR}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD);
""")

print(f"Finished building daily_core: {OUT_DIR}")


# ---------------------------------------------------------------------
# 4. Quick sanity checks
# ---------------------------------------------------------------------

print("\nDaily core schema:")
print(con.execute(f"""
DESCRIBE SELECT *
FROM read_parquet('{OUT_DIR}/**/*.parquet', hive_partitioning=true)
""").df())

print("\nExample rows:")
print(con.execute(f"""
SELECT *
FROM read_parquet('{OUT_DIR}/**/*.parquet', hive_partitioning=true)
LIMIT 5
""").df())