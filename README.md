# us-stock

Utilities for preparing local US stock CSV data for analysis.

## Setup

Create the Python virtual environment and install dependencies:

```bash
bash env/setup_venv.sh
```

Activate the environment:

```bash
source .venv/bin/activate
```

## Prepare Data

Place the raw CSV file under the `data/` folder. The CSV converter currently
expects this file:

```text
data/us_stock_19_26_raw.csv
```

Convert the CSV into parquet files:

```bash
python src/csv_to_parquet.py
```

The converted parquet files are written to:

```text
data/data_parquet/
```

The `data/` folder is ignored by git, so local CSV and parquet files will not be
committed.
