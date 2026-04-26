from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")

UNIVERSE_GLOB = ROOT / "data/clean_parquet/daily_stock_universe/**/*.parquet"
OUT_CSV = ROOT / "data/clean_parquet/daily_stock_universe_missing_abnormal_report.csv"
OUT_EXAMPLES_CSV = ROOT / "data/clean_parquet/daily_stock_universe_abnormal_examples.csv"

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")


# ---------------------------------------------------------------------
# Read universe data as the diagnostic base.
# No extra filtering here: this dataset is already the selected universe.
# ---------------------------------------------------------------------

con.execute(f"""
CREATE OR REPLACE TEMP VIEW analysis_base AS
SELECT *
FROM read_parquet('{UNIVERSE_GLOB}', hive_partitioning=true);
""")


# ---------------------------------------------------------------------
# Get all columns automatically.
# This is better here because daily_stock_universe contains both original
# variables and derived universe/flag variables.
# ---------------------------------------------------------------------

schema = con.execute("""
    DESCRIBE SELECT *
    FROM analysis_base
""").df()

VARIABLES = [
    (row["column_name"], row["column_type"])
    for _, row in schema.iterrows()
]


ABNORMAL_RULES = {
    # Identifiers
    "permno": "permno <= 0",
    "permco": "permco <= 0",
    "yyyymmdd": "yyyymmdd < 19000101 OR yyyymmdd > 21000101",

    # Dates
    "dlycaldt": "dlycaldt < DATE '1900-01-01' OR dlycaldt > DATE '2100-01-01'",
    "secinfostartdt": "secinfostartdt < DATE '1900-01-01' OR secinfostartdt > DATE '2100-01-01'",
    "secinfoenddt": "secinfoenddt < DATE '1900-01-01' OR secinfoenddt > DATE '2100-01-01'",
    "securitybegdt": "securitybegdt < DATE '1900-01-01' OR securitybegdt > DATE '2100-01-01'",
    "securityenddt": "securityenddt < DATE '1900-01-01' OR securityenddt > DATE '2100-01-01'",
    "dlyprevdt": "dlyprevdt < DATE '1900-01-01' OR dlyprevdt > DATE '2100-01-01'",
    "shrstartdt": "shrstartdt < DATE '1900-01-01' OR shrstartdt > DATE '2100-01-01'",
    "shrenddt": "shrenddt < DATE '1900-01-01' OR shrenddt > DATE '2100-01-01'",
    "disexdt": "disexdt < DATE '1900-01-01' OR disexdt > DATE '2100-01-01'",
    "disdeclaredt": "disdeclaredt < DATE '1900-01-01' OR disdeclaredt > DATE '2100-01-01'",
    "disrecorddt": "disrecorddt < DATE '1900-01-01' OR disrecorddt > DATE '2100-01-01'",
    "dispaydt": "dispaydt < DATE '1900-01-01' OR dispaydt > DATE '2100-01-01'",

    # Prices / volumes / shares
    "dlyprc": "ABS(dlyprc) <= 0 OR ABS(dlyprc) > 100000",
    "prc": "prc <= 0 OR prc > 100000",
    "dlyopen": "dlyopen <= 0 OR dlyopen > 100000",
    "dlyclose": "dlyclose <= 0 OR dlyclose > 100000",
    "dlyhigh": "dlyhigh <= 0 OR dlyhigh > 100000",
    "dlylow": "dlylow <= 0 OR dlylow > 100000",
    "dlybid": "dlybid < 0 OR dlybid > 100000",
    "dlyask": "dlyask < 0 OR dlyask > 100000",
    "dlyvol": "dlyvol < 0",
    "dlynumtrd": "dlynumtrd < 0",
    "dlymmcnt": "dlymmcnt < 0",
    "dlyprcvol": "dlyprcvol < 0",
    "shrout": "shrout <= 0",
    "dlycap": "dlycap <= 0",
    "dlyprevcap": "dlyprevcap <= 0",
    "dlyprevprc": "ABS(dlyprevprc) <= 0 OR ABS(dlyprevprc) > 100000",

    # Derived variables
    "dollar_volume": "dollar_volume <= 0",
    "dollar_volume_for_universe": "dollar_volume_for_universe <= 0",
    "adv20": "adv20 <= 0",
    "avg_volume_20d": "avg_volume_20d <= 0",
    "market_cap_rank": "market_cap_rank <= 0",
    "adv20_rank": "adv20_rank <= 0",
    "hist_ret_obs_252": "hist_ret_obs_252 < 0",
    "trading_age_obs": "trading_age_obs <= 0",

    # Returns
    "dlyret": "dlyret < -1 OR dlyret > 20",
    "dlyretx": "dlyretx < -1 OR dlyretx > 20",
    "dlyreti": "dlyreti < -1 OR dlyreti > 20",
    "vwretd": "vwretd < -1 OR vwretd > 1",
    "vwretx": "vwretx < -1 OR vwretx > 1",
    "ewretd": "ewretd < -1 OR ewretd > 1",
    "ewretx": "ewretx < -1 OR ewretx > 1",
    "sprtrn": "sprtrn < -1 OR sprtrn > 1",

    # Distribution / adjustment fields
    "dlyfacprc": "dlyfacprc <= 0",
    "disdivamt": "disdivamt < 0",
    "disfacpr": "disfacpr < 0",
    "disfacshr": "disfacshr < 0",
    "dlyorddivamt": "dlyorddivamt < 0",
    "dlynonorddivamt": "dlynonorddivamt < 0",

    # Industry code
    "siccd": "siccd < 0 OR siccd > 9999",
}


CATEGORICAL_RULES = {
    "primaryexch": "LENGTH(primaryexch) > 10",
    "ticker": "LENGTH(ticker) > 20",
    "tradingsymbol": "LENGTH(tradingsymbol) > 30",
    "cusip": "LENGTH(cusip) NOT IN (6, 8, 9)",
    "cusip9": "LENGTH(cusip9) != 9",
    "hdrcusip": "LENGTH(hdrcusip) NOT IN (6, 8, 9)",
    "hdrcusip9": "LENGTH(hdrcusip9) != 9",
    "securitynm": "LENGTH(securitynm) > 200",
    "issuernm": "LENGTH(issuernm) > 200",
}


def sql_count_missing(col: str) -> str:
    return f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)"


def sql_count_abnormal(col: str) -> str:
    if col in ABNORMAL_RULES:
        rule = ABNORMAL_RULES[col]
        return f"SUM(CASE WHEN {col} IS NOT NULL AND ({rule}) THEN 1 ELSE 0 END)"

    if col in CATEGORICAL_RULES:
        rule = CATEGORICAL_RULES[col]
        return f"SUM(CASE WHEN {col} IS NOT NULL AND ({rule}) THEN 1 ELSE 0 END)"

    return "0"


rows = []

total_n = con.execute("""
    SELECT COUNT(*)
    FROM analysis_base
""").fetchone()[0]

print(f"Number of rows in daily_stock_universe: {total_n:,}")


for col, typ in VARIABLES:
    abnormal_rule = ABNORMAL_RULES.get(col, CATEGORICAL_RULES.get(col, ""))

    result = con.execute(f"""
        SELECT
            {sql_count_missing(col)} AS missing_count,
            {sql_count_abnormal(col)} AS abnormal_count
        FROM analysis_base
    """).fetchone()

    missing_count = int(result[0] or 0)
    abnormal_count = int(result[1] or 0)

    rows.append({
        "variable": col,
        "type": typ,
        "n_total": total_n,
        "missing_count": missing_count,
        "missing_pct": missing_count / total_n if total_n > 0 else None,
        "abnormal_count": abnormal_count,
        "abnormal_pct": abnormal_count / total_n if total_n > 0 else None,
        "abnormal_rule": abnormal_rule,
    })


# ---------------------------------------------------------------------
# Cross-column abnormal rule: OHLC consistency.
# Only added if all required columns exist.
# ---------------------------------------------------------------------

all_cols = {col for col, _ in VARIABLES}

if {"dlyhigh", "dlylow", "dlyopen", "dlyclose"}.issubset(all_cols):
    ohlc_rule = """
        dlyhigh IS NOT NULL AND dlylow IS NOT NULL
        AND dlyopen IS NOT NULL AND dlyclose IS NOT NULL
        AND (
            dlyhigh < dlylow
            OR dlyopen > dlyhigh
            OR dlyopen < dlylow
            OR dlyclose > dlyhigh
            OR dlyclose < dlylow
        )
    """

    abnormal_count = con.execute(f"""
        SELECT
            SUM(CASE WHEN ({ohlc_rule}) THEN 1 ELSE 0 END) AS abnormal_count
        FROM analysis_base
    """).fetchone()[0]

    abnormal_count = int(abnormal_count or 0)

    rows.append({
        "variable": "price_ohlc_consistency",
        "type": "cross_column_rule",
        "n_total": total_n,
        "missing_count": None,
        "missing_pct": None,
        "abnormal_count": abnormal_count,
        "abnormal_pct": abnormal_count / total_n if total_n > 0 else None,
        "abnormal_rule": ohlc_rule.strip(),
    })


report = pd.DataFrame(rows)
report = report.sort_values(
    ["abnormal_pct", "missing_pct"],
    ascending=[False, False],
)

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
report.to_csv(OUT_CSV, index=False)

print(f"\nSaved missing/abnormal report to: {OUT_CSV}")
print(report.head(40).to_string(index=False))


# ---------------------------------------------------------------------
# Save small abnormal examples.
# Safer than UNION ALL because each query can have LIMIT.
# ---------------------------------------------------------------------

example_dfs = []

for col, _ in VARIABLES:
    rule = ABNORMAL_RULES.get(col, CATEGORICAL_RULES.get(col))

    if rule is None:
        continue

    df = con.execute(f"""
        SELECT
            '{col}' AS variable,
            permno,
            dlycaldt,
            ticker,
            CAST({col} AS VARCHAR) AS value
        FROM analysis_base
        WHERE {col} IS NOT NULL
          AND ({rule})
        LIMIT 20
    """).df()

    if len(df) > 0:
        example_dfs.append(df)


if {"dlyhigh", "dlylow", "dlyopen", "dlyclose"}.issubset(all_cols):
    df = con.execute(f"""
        SELECT
            'price_ohlc_consistency' AS variable,
            permno,
            dlycaldt,
            ticker,
            'open=' || CAST(dlyopen AS VARCHAR)
                || ', high=' || CAST(dlyhigh AS VARCHAR)
                || ', low=' || CAST(dlylow AS VARCHAR)
                || ', close=' || CAST(dlyclose AS VARCHAR) AS value
        FROM analysis_base
        WHERE {ohlc_rule}
        LIMIT 20
    """).df()

    if len(df) > 0:
        example_dfs.append(df)


if len(example_dfs) > 0:
    examples = pd.concat(example_dfs, ignore_index=True)
else:
    examples = pd.DataFrame(
        columns=["variable", "permno", "dlycaldt", "ticker", "value"]
    )

examples.to_csv(OUT_EXAMPLES_CSV, index=False)

print(f"Saved abnormal examples to: {OUT_EXAMPLES_CSV}")