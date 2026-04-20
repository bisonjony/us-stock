import duckdb
import pandas as pd

con = duckdb.connect()

# show column names and types
schema_df = con.execute("""
    DESCRIBE SELECT * 
    FROM 'data/data_parquet/*.parquet'
""").df()

print(schema_df)



df = pd.read_parquet("data/data_parquet/us_stock_19_26_raw_part_0000.parquet")

print(df.head(10))