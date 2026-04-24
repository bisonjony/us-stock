from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path("/home/xul9527/us-stock")
CORE_GLOB = ROOT / "data/clean_parquet/daily_core/**/*.parquet"

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 0)
pd.set_option("display.max_colwidth", 80)


def get_all_columns():
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
    """).df()
    return schema["column_name"].tolist()


ALL_COLUMNS = get_all_columns()

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

    # Price / volume / share variables
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

    # String length checks
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


def print_missing_examples(
    variable: str,
    n: int = 20,
    save_csv: bool = True,
):
    """
    Randomly sample rows where `variable` is missing.

    This prints all columns, so you can inspect the full context.
    It also optionally saves the examples to CSV.
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in daily_core columns.")

    df = con.execute(f"""
        SELECT *
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
        WHERE {variable} IS NULL
        ORDER BY RANDOM()
        LIMIT {n}
    """).df()

    print(f"\nRandom missing examples for variable: {variable}")
    print(f"Number of examples shown: {len(df)}")
    print(df.to_string(index=False))

    if save_csv:
        out_path = ROOT / f"data/clean_parquet/missing_examples_{variable}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved to: {out_path}")

    return df


def summarize_missing_by_group(variable: str, group_cols=None):
    """
    Show where missingness is concentrated.
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in daily_core columns.")

    if group_cols is None:
        group_cols = ["year", "primaryexch", "securitytype", "sharetype", "tradingstatusflg"]

    for col in group_cols:
        if col not in ALL_COLUMNS:
            raise ValueError(f"{col} is not in daily_core columns.")

    group_sql = ", ".join(group_cols)

    df = con.execute(f"""
        SELECT
            {group_sql},
            COUNT(*) AS n_rows,
            SUM(CASE WHEN {variable} IS NULL THEN 1 ELSE 0 END) AS n_missing,
            AVG(CASE WHEN {variable} IS NULL THEN 1.0 ELSE 0.0 END) AS missing_rate
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
        GROUP BY {group_sql}
        HAVING n_missing > 0
        ORDER BY n_missing DESC
        LIMIT 100
    """).df()

    print(f"\nMissing pattern for variable: {variable}")
    print(df.to_string(index=False))

    out_path = ROOT / f"data/clean_parquet/missing_pattern_{variable}.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to: {out_path}")

    return df


def print_abnormal_examples(
    variable: str,
    n: int = 20,
    rule: str | None = None,
    save_csv: bool = True,
):
    """
    Print abnormal examples for a given variable.

    If total abnormal count <= n, print all abnormal rows.
    If total abnormal count > n, randomly sample n abnormal rows.

    The rule can be passed manually. If not passed, the function uses
    ABNORMAL_RULES[variable].
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in daily_core columns.")

    if rule is None:
        if variable not in ABNORMAL_RULES:
            raise ValueError(
                f"No abnormal rule found for {variable}. "
                f"Please pass rule='...' manually."
            )
        rule = ABNORMAL_RULES[variable]

    abnormal_count = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
        WHERE {variable} IS NOT NULL
          AND ({rule})
    """).fetchone()[0]

    print(f"\nAbnormal examples for variable: {variable}")
    print(f"Abnormal rule: {rule}")
    print(f"Total abnormal count: {abnormal_count}")

    if abnormal_count == 0:
        print("No abnormal rows found.")
        return pd.DataFrame()

    if abnormal_count <= n:
        query = f"""
            SELECT *
            FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
            WHERE {variable} IS NOT NULL
              AND ({rule})
            ORDER BY dlycaldt, permno
        """
    else:
        query = f"""
            SELECT *
            FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
            WHERE {variable} IS NOT NULL
              AND ({rule})
            ORDER BY RANDOM()
            LIMIT {n}
        """

    df = con.execute(query).df()

    print(f"Number of examples shown: {len(df)}")
    print(df.to_string(index=False))

    if save_csv:
        out_path = ROOT / f"data/clean_parquet/abnormal_examples_{variable}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved to: {out_path}")

    return df


def summarize_abnormal_by_group(
    variable: str,
    group_cols=None,
    rule: str | None = None,
):
    """
    Show where abnormal values are concentrated.
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in daily_core columns.")

    if rule is None:
        if variable not in ABNORMAL_RULES:
            raise ValueError(
                f"No abnormal rule found for {variable}. "
                f"Please pass rule='...' manually."
            )
        rule = ABNORMAL_RULES[variable]

    if group_cols is None:
        group_cols = ["year", "primaryexch", "securitytype", "sharetype", "tradingstatusflg"]

    for col in group_cols:
        if col not in ALL_COLUMNS:
            raise ValueError(f"{col} is not in daily_core columns.")

    group_sql = ", ".join(group_cols)

    df = con.execute(f"""
        SELECT
            {group_sql},
            COUNT(*) AS n_rows,
            SUM(
                CASE
                    WHEN {variable} IS NOT NULL AND ({rule})
                    THEN 1 ELSE 0
                END
            ) AS n_abnormal,
            AVG(
                CASE
                    WHEN {variable} IS NOT NULL AND ({rule})
                    THEN 1.0 ELSE 0.0
                END
            ) AS abnormal_rate
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
        GROUP BY {group_sql}
        HAVING n_abnormal > 0
        ORDER BY n_abnormal DESC
        LIMIT 100
    """).df()

    print(f"\nAbnormal pattern for variable: {variable}")
    print(f"Abnormal rule: {rule}")
    print(df.to_string(index=False))

    out_path = ROOT / f"data/clean_parquet/abnormal_pattern_{variable}.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to: {out_path}")

    return df

# Example usage:
if __name__ == "__main__":
    print_abnormal_examples("disdivamt", n=20)
    # summarize_abnormal_by_group("price_ohlc_consistency")
    # print_missing_examples("disdivamt", n=20)
    # summarize_missing_by_group("disdivamt")
    # print_missing_examples("dlyret", n=20)
    # print_missing_examples("shrout", n=20)
    # print_missing_examples("dlynumtrd", n=20)