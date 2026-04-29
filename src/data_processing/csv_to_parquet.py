import pandas as pd
from pathlib import Path

csv_path = "data/us_stock_19_26_raw.csv"
out_dir = Path("data/data_parquet")
out_dir.mkdir(exist_ok=True)

reader = pd.read_csv(
    csv_path,
    chunksize=300_000,
    dtype="string",
    low_memory=False,
)

# 52 chunks in total
for i, chunk in enumerate(reader):
    chunk.to_parquet(
        out_dir / f"us_stock_19_26_raw_part_{i:04d}.parquet",
        index=False,
    )
    print(f"chunk {i} done")