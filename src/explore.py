# import duckdb
# import pandas as pd

# con = duckdb.connect()

# # show column names and types
# schema_df = con.execute("""
#     DESCRIBE SELECT * 
#     FROM 'data/data_parquet/*.parquet'
# """).df()

# print(schema_df)



# df = pd.read_parquet("data/data_parquet/us_stock_19_26_raw_part_0000.parquet")

# print(df.head(10))


from pathlib import Path
import duckdb
import pandas as pd
import matplotlib.pyplot as plt


ROOT = Path("/home/xul9527/us-stock")
CORE_GLOB = ROOT / "data/clean_parquet/daily_core/**/*.parquet"
OUT_DIR = ROOT / "data/clean_parquet/variable_distribution_plots"
OUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))
con.execute("PRAGMA threads=4")
con.execute("SET memory_limit='6GB'")

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", None)


def get_all_columns():
    schema = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
    """).df()
    return schema["column_name"].tolist()


ALL_COLUMNS = get_all_columns()


def check_variable(variable: str):
    if variable not in ALL_COLUMNS:
        raise ValueError(f"{variable} is not in daily_core columns.")


def plot_continuous(variable: str, bins: int = 50):
    """
    Plot histogram for a continuous variable.

    Missing values are ignored in the histogram.
    No row filtering is applied.
    """

    check_variable(variable)

    summary = con.execute(f"""
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN {variable} IS NULL THEN 1 ELSE 0 END) AS n_missing,
            MIN({variable}) AS min_value,
            APPROX_QUANTILE({variable}, 0.01) AS p01,
            APPROX_QUANTILE({variable}, 0.05) AS p05,
            APPROX_QUANTILE({variable}, 0.50) AS p50,
            APPROX_QUANTILE({variable}, 0.95) AS p95,
            APPROX_QUANTILE({variable}, 0.99) AS p99,
            MAX({variable}) AS max_value
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
    """).df()

    print(f"\nSummary for {variable}:")
    print(summary.to_string(index=False))

    min_value = summary.loc[0, "min_value"]
    max_value = summary.loc[0, "max_value"]

    if pd.isna(min_value) or pd.isna(max_value):
        print(f"{variable} has no non-missing values.")
        return summary

    if min_value == max_value:
        print(f"{variable} has only one non-missing value: {min_value}")
        return summary

    hist = con.execute(f"""
        WITH stats AS (
            SELECT
                MIN({variable}) AS min_value,
                MAX({variable}) AS max_value
            FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
            WHERE {variable} IS NOT NULL
        ),

        binned AS (
            SELECT
                CASE
                    WHEN {variable} = stats.max_value THEN {bins - 1}
                    ELSE FLOOR(
                        ({variable} - stats.min_value)
                        / NULLIF(stats.max_value - stats.min_value, 0)
                        * {bins}
                    )::INTEGER
                END AS bin_id
            FROM read_parquet('{CORE_GLOB}', hive_partitioning=true), stats
            WHERE {variable} IS NOT NULL
        )

        SELECT
            bin_id,
            COUNT(*) AS count
        FROM binned
        GROUP BY bin_id
        ORDER BY bin_id
    """).df()

    hist["bin_left"] = min_value + hist["bin_id"] * (max_value - min_value) / bins
    hist["bin_right"] = min_value + (hist["bin_id"] + 1) * (max_value - min_value) / bins
    hist["bin_mid"] = (hist["bin_left"] + hist["bin_right"]) / 2

    hist_csv = OUT_DIR / f"{variable}_histogram.csv"
    hist.to_csv(hist_csv, index=False)

    plt.figure(figsize=(10, 6))
    plt.bar(
        hist["bin_mid"],
        hist["count"],
        width=(max_value - min_value) / bins,
        align="center",
    )
    plt.xlabel(variable)
    plt.ylabel("Count")
    plt.title(f"Histogram of {variable}")
    plt.tight_layout()

    out_png = OUT_DIR / f"{variable}_histogram.png"
    plt.savefig(out_png, dpi=200)
    plt.show()

    print(f"\nSaved histogram data to: {hist_csv}")
    print(f"Saved histogram plot to: {out_png}")

    return hist


def plot_categorical(variable: str, top_n: int = 30):
    """
    Plot bar chart for a categorical variable.

    Missing values are included as '__MISSING__'.
    No row filtering is applied.
    """

    check_variable(variable)

    counts = con.execute(f"""
        SELECT
            COALESCE(CAST({variable} AS VARCHAR), '__MISSING__') AS value,
            COUNT(*) AS count
        FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
        GROUP BY value
        ORDER BY count DESC
    """).df()

    print(f"\nValue counts for {variable}:")
    print(counts.head(top_n).to_string(index=False))

    counts_csv = OUT_DIR / f"{variable}_value_counts.csv"
    counts.to_csv(counts_csv, index=False)

    plot_df = counts.head(top_n).copy()

    plt.figure(figsize=(12, 6))
    plt.bar(plot_df["value"].astype(str), plot_df["count"])
    plt.xlabel(variable)
    plt.ylabel("Count")
    plt.title(f"Top {top_n} values of {variable}")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_png = OUT_DIR / f"{variable}_bar_chart.png"
    plt.savefig(out_png, dpi=200)
    plt.show()

    print(f"\nSaved value counts to: {counts_csv}")
    print(f"Saved bar chart to: {out_png}")

    return counts


if __name__ == "__main__":
    # Choose variable here.

    # Continuous examples:
    # plot_continuous("dlycap", bins=50)
    # plot_continuous("prc", bins=50)
    # plot_continuous("dlycap", bins=50)
    # plot_continuous("dlyret", bins=50)

    # Categorical examples:
    plot_categorical("delactiontype", top_n=30)
    # plot_categorical("securitytype", top_n=30)
    # plot_categorical("sharetype", top_n=30)
    # plot_categorical("tradingstatusflg", top_n=30)
  
#     summary_temp = con.execute(f"""
#     WITH base AS (
#         SELECT
#             ABS(prc) * dlyvol AS dollar_volume
#         FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
#         WHERE prc IS NOT NULL
#           AND dlyvol IS NOT NULL
#     )
#     SELECT
#         MIN(dollar_volume) AS min_value,
#         APPROX_QUANTILE(dollar_volume, 0.01) AS p01,
#         APPROX_QUANTILE(dollar_volume, 0.25) AS p05,
#         APPROX_QUANTILE(dollar_volume, 0.50) AS p50,
#         APPROX_QUANTILE(dollar_volume, 0.75) AS p95,
#         APPROX_QUANTILE(dollar_volume, 0.99) AS p99,
#         MAX(dollar_volume) AS max_value
#     FROM base
# """).df()

    # summary_temp = con.execute(f"""
    #     SELECT COUNT(DISTINCT permno) AS n_stocks
    #     FROM read_parquet('{CORE_GLOB}', hive_partitioning=true)
    # """).df()

    # print(summary_temp.to_string(index=False))


