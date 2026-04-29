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


# ---------------------------------------------------------------------
# Filtered diagnostic population:
# active, security-active, non-delisting stock-day rows only.
#
# This is consistent with the second-pass missing/abnormal analysis.
# It does not modify daily_core.
# ---------------------------------------------------------------------

con.execute(f"""
CREATE OR REPLACE TEMP VIEW analysis_base AS
SELECT *
FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
WHERE tradingstatusflg = 'A'
  AND securityactiveflg = 'Y'
  AND dlydelflg = 'N'
;
""")


def get_all_columns():
    schema = con.execute("""
        DESCRIBE SELECT *
        FROM analysis_base
    """).df()
    return schema["column_name"].tolist()


ALL_COLUMNS = get_all_columns()


def print_nonmissing_examples(
    variable: str,
    n: int = 20,
    save_csv: bool = True,
):
    """
    Print examples where `variable` is non-missing.

    If total non-missing count <= n, print all examples.
    If total non-missing count > n, randomly sample n examples.

    This prints all columns so you can inspect the full context.
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in analysis_base columns.")

    nonmissing_count = con.execute(f"""
        SELECT COUNT(*)
        FROM analysis_base
        WHERE {variable} IS NOT NULL
    """).fetchone()[0]

    print(f"\nNon-missing examples for variable: {variable}")
    print(f"Total non-missing count: {nonmissing_count}")

    if nonmissing_count == 0:
        print("No non-missing rows found.")
        return pd.DataFrame()

    if nonmissing_count <= n:
        query = f"""
            SELECT *
            FROM analysis_base
            WHERE {variable} IS NOT NULL
            ORDER BY dlycaldt, permno
        """
    else:
        query = f"""
            SELECT *
            FROM analysis_base
            WHERE {variable} IS NOT NULL
            ORDER BY RANDOM()
            LIMIT {n}
        """

    df = con.execute(query).df()

    print(f"Number of examples shown: {len(df)}")
    print(df.to_string(index=False))

    if save_csv:
        out_path = ROOT / f"data/clean_parquet/nonmissing_examples_{variable}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved to: {out_path}")

    return df


def summarize_nonmissing_by_group(variable: str, group_cols=None):
    """
    Show where non-missing values are concentrated.
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in analysis_base columns.")

    if group_cols is None:
        group_cols = ["year", "primaryexch", "securitytype", "sharetype", "tradingstatusflg"]

    for col in group_cols:
        if col not in ALL_COLUMNS:
            raise ValueError(f"{col} is not in analysis_base columns.")

    group_sql = ", ".join(group_cols)

    df = con.execute(f"""
        SELECT
            {group_sql},
            COUNT(*) AS n_rows,
            SUM(CASE WHEN {variable} IS NOT NULL THEN 1 ELSE 0 END) AS n_nonmissing,
            AVG(CASE WHEN {variable} IS NOT NULL THEN 1.0 ELSE 0.0 END) AS nonmissing_rate
        FROM analysis_base
        GROUP BY {group_sql}
        HAVING n_nonmissing > 0
        ORDER BY n_nonmissing DESC
        LIMIT 100
    """).df()

    print(f"\nNon-missing pattern for variable: {variable}")
    print(df.to_string(index=False))

    out_path = ROOT / f"data/clean_parquet/nonmissing_pattern_{variable}.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to: {out_path}")

    return df


def summarize_nonmissing_values(
    variable: str,
    top_n: int = 50,
    save_csv: bool = True,
):
    """
    For categorical or event variables, show the most common non-missing values.

    Useful for variables like:
      distype, dispaymenttype, disdetailtype, delactiontype, shareclass.
    """

    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in analysis_base columns.")

    df = con.execute(f"""
        SELECT
            CAST({variable} AS VARCHAR) AS value,
            COUNT(*) AS n_rows
        FROM analysis_base
        WHERE {variable} IS NOT NULL
        GROUP BY value
        ORDER BY n_rows DESC
        LIMIT {top_n}
    """).df()

    print(f"\nTop non-missing values for variable: {variable}")
    print(df.to_string(index=False))

    if save_csv:
        out_path = ROOT / f"data/clean_parquet/nonmissing_values_{variable}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved to: {out_path}")

    return df


# Example usage:
if __name__ == "__main__":
    # Example 1: distribution event variable
    # print_nonmissing_examples("disdivamt", n=20)
    # summarize_nonmissing_by_group("disdivamt")
    # summarize_nonmissing_values("disdivamt", top_n=50)

    # Other useful examples:
    print_nonmissing_examples("shrfactype", n=20)
    summarize_nonmissing_by_group("shrfactype")
    summarize_nonmissing_values("shrfactype", top_n=50)

    # print_nonmissing_examples("delactiontype", n=20)
    # summarize_nonmissing_by_group("delactiontype")
    # summarize_nonmissing_values("delactiontype", top_n=50)

    # print_nonmissing_examples("shareclass", n=20)
    # summarize_nonmissing_by_group("shareclass")
    # summarize_nonmissing_values("shareclass", top_n=50)