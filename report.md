# Data processing

This document summarizes the data-processing work completed so far for the U.S. stock daily dataset. The goal of these steps is to convert the raw data into a reliable typed daily panel, diagnose missing and abnormal values, and prepare the data for later universe construction, feature engineering, model training, and backtesting.

## 1. Raw CSV to Parquet conversion

The raw U.S. stock daily data from 2019--2026 was originally stored as a large CSV file. Since the file is too large to load directly into pandas, we first converted it into separate Parquet files for efficient downstream processing.

The conversion step is implemented in:

```text
src/csv_to_parquet.py
```

Main design choices:

- Read the raw CSV in chunks of **300,000 rows per iteration** to avoid memory overflow.
- Store the output as **52 separate Parquet files**, named from:

```text
data/data_parquet/us_stock_19_26_raw_part_0000.parquet
```

to:

```text
data/data_parquet/us_stock_19_26_raw_part_0052.parquet
```

- Read all variables as `String` during this first conversion step.
- Avoid pandas automatic type inference at this stage because columns such as CUSIP-like identifiers and CRSP flags may contain mixed representations.
- Avoid date/time conversion at this stage to prevent conversion errors and preserve raw values.

This step produces a memory-safe raw Parquet archive that can be queried efficiently by DuckDB.

## 2. Type conversion and `daily_core` construction

After the raw Parquet files were created, we used DuckDB SQL queries to process the data and construct a typed `daily_core` table.

The processing step is implemented in:

```text
src/build_daily_core.py
```

Main tasks in this step:

- Read all 52 raw Parquet files using DuckDB.
- Normalize variable names to lowercase.
- Use the CRSP variable dictionary to assign each variable to an intended type, such as:
  - integer
  - decimal / double
  - date
  - character
- Cast each variable from raw string format into its intended type.
- Preserve all original variables from the raw data.
- Keep the raw CRSP price variable `dlyprc` unchanged.
<!-- - Create a cleaned price magnitude variable:

```text
prc = ABS(dlyprc)
```

- Add a flag indicating whether the raw CRSP price was negative:

```text
dlyprc_negative_flag
```

This is important because negative CRSP prices are often a data convention indicating bid/ask average prices, not economically negative prices. -->

For casting failures:

- If a non-empty raw value failed to cast into the intended type, the row would be saved separately for manual inspection.
- In the actual run, all casting was successful.

Output from this step:

```text
data/clean_parquet/daily_core/
```

The resulting `daily_core` table is a typed, mostly lossless daily stock panel. It is not yet filtered into a trading universe.

## 3. Missingness and abnormal-value scan

After constructing `daily_core`, we scanned each variable for missing values and abnormal values.

This step is implemented in:

```text
src/scan_missing_abnormal.py
```

Main tasks in this step:

- Use DuckDB to scan the full `daily_core` table.
- For each variable, compute:
  - total number of rows
  - missing count
  - missing percentage
  - abnormal count
  - abnormal percentage
  - abnormal rule used, if applicable
- Save the resulting summary table as:

```text
data/clean_parquet/daily_core_missing_abnormal_report.csv
```

Selected rows from the missing/abnormal report:

| variable | type | total rows | missing count | missing % | abnormal count | abnormal % | abnormal rule |
|---|---|---:|---:|---:|---:|---:|---|
| `disfacpr` | decimal | 15,749,137 | 15,553,778 | 98.7596% | 5,986 | 0.0380% | `disfacpr < 0` |
| `disfacshr` | decimal | 15,749,137 | 15,553,778 | 98.7596% | 5,985 | 0.0380% | `disfacshr < 0` |
| `dlyprc` | decimal | 15,749,137 | 47,018 | 0.2985% | 5,426 | 0.0345% | `ABS(dlyprc) <= 0 OR ABS(dlyprc) > 100000` |
| `prc` | decimal | 15,749,137 | 47,018 | 0.2985% | 5,426 | 0.0345% | `prc <= 0 OR prc > 100000` |
| `dlyclose` | decimal | 15,749,137 | 386,149 | 2.4519% | 1,760 | 0.0112% | `dlyclose <= 0 OR dlyclose > 100000` |
| `dlylow` | decimal | 15,749,137 | 386,149 | 2.4519% | 1,760 | 0.0112% | `dlylow <= 0 OR dlylow > 100000` |
| `dlyhigh` | decimal | 15,749,137 | 386,149 | 2.4519% | 1,760 | 0.0112% | `dlyhigh <= 0 OR dlyhigh > 100000` |
| `dlyopen` | decimal | 15,749,137 | 386,146 | 2.4519% | 1,760 | 0.0112% | `dlyopen <= 0 OR dlyopen > 100000` |
| `dlyask` | decimal | 15,749,137 | 51,628 | 0.3278% | 1,760 | 0.0112% | `dlyask < 0 OR dlyask > 100000` |
| `dlybid` | decimal | 15,749,137 | 51,625 | 0.3278% | 1,760 | 0.0112% | `dlybid < 0 OR dlybid > 100000` |
| `dlyret` | decimal | 15,749,137 | 54,680 | 0.3472% | 2 | 0.000013% | `dlyret < -1 OR dlyret > 20` |
| `dlyretx` | decimal | 15,749,137 | 54,680 | 0.3472% | 2 | 0.000013% | `dlyretx < -1 OR dlyretx > 20` |
| `disdivamt` | decimal | 15,749,137 | 15,556,843 | 98.7790% | 1 | 0.000006% | `disdivamt < 0` |
| `dlynumtrd` | int | 15,749,137 | 9,039,956 | 57.3997% | 0 | 0.0000% | `dlynumtrd < 0` |
| `exchangetier` | char | 15,749,137 | 9,035,459 | 57.3711% | 0 | 0.0000% | none |
| `dlymmcnt` | int | 15,749,137 | 9,035,459 | 57.3711% | 0 | 0.0000% | `dlymmcnt < 0` |
| `shareclass` | char | 15,749,137 | 14,096,039 | 89.5036% | 0 | 0.0000% | none |

This scan gives a global view of missingness and abnormality, but it does not by itself determine whether a value is invalid. Many missing values are structural, especially for event-specific variables such as distribution and delisting fields.

## 4. Missingness and abnormality investigation

After generating the missing/abnormal report, we manually investigated the reason for missingness and abnormality for important variable groups.

This step is implemented in:

```text
src/investigate_missing_examples.py
```

The investigation script supports:

- Randomly sampling rows where a given variable is missing.
- Printing all columns for each sampled row so that the surrounding context can be inspected.
- Summarizing missingness by groups such as:
  - `year`
  - `primaryexch`
  - `securitytype`
  - `sharetype`
  - `tradingstatusflg`
- Randomly sampling abnormal rows for a given variable.
- Saving both examples and grouped summaries to CSV files.

The detailed investigation reports are stored in:

```text
missing_investigation.md
abnormal_investigation.md
```

Main conclusions from the missingness investigation:

- OHLC variables (`dlyopen`, `dlyhigh`, `dlylow`, `dlyclose`) are often missing together. Many of these rows still have valid `dlyprc`, especially when the price comes from bid/ask quotes. These rows should generally be kept with indicators rather than dropped automatically.
- Missing `prc` is more serious because `prc = ABS(dlyprc)`. Rows with missing or zero `prc` generally cannot be used as day-*t* trading candidates.
- Missing bid/ask variables are mostly concentrated in inactive, suspended, halted, or delisting rows. For active rows, bid/ask missingness is rare and can be handled with a missingness flag.
- Missing `dlyvol`, `dlycap`, and `dlyprcvol` is rare and mostly occurs in non-tradable or terminal rows. These variables are essential for universe preparation, so rows missing them are not suitable as trading candidates.
- Missing return variables (`dlyret`, `dlyretx`, `dlyreti`) are mostly associated with non-trading or inactive rows. Active rows with missing returns should not have returns imputed as zero.
- Variables such as `dlynumtrd`, `dlymmcnt`, and `exchangetier` have high missingness, but this reflects limited field coverage rather than data failure. They should be optional microstructure variables, not mandatory universe filters.
- Distribution-event variables are mostly missing because most stock-date rows do not have a dividend, split, or other distribution event. Missingness should generally be interpreted as no recorded event.
- Delisting-related variables are event metadata and should not be used as mandatory filters by themselves.

Main conclusions from the abnormal-value investigation:

- Abnormal `dlyprc` values split into two cases: zero-price terminal/delisting rows and valid high-priced active equities. High prices above 100,000 should not be treated as invalid by themselves.
- Negative `disfacpr` values are usually legitimate corporate-action records, including delisting distributions and reverse-split-style events.
- The two abnormal `dlyret` rows are internally consistent extreme price jumps and should be preserved in `daily_core`; they may need special handling during model training.
- The single negative `disdivamt` row appears to be a corporate-action adjustment record, not a systematic data-quality problem.

The purpose of this investigation was not to directly build the final universe. Instead, it determined how each type of missing or abnormal value should be handled later during universe preparation, feature engineering, model training, and backtesting.

## 5. Current state before universe construction

At this point, the data has been converted, typed, and diagnosed.

The current clean base table is:

```text
data/clean_parquet/daily_core/
```

The next step is not yet final universe construction. The next step is to create a universe-preparation table that:

- keeps raw information from `daily_core`,
- creates indicator variables for important missingness and abnormality patterns,
- creates basic derived variables such as `dollar_volume`, `log_prc`, and bid-ask spread,
- removes only structurally unusable rows from the universe-ready base,
- preserves terminal/delisting rows separately for later backtest handling.

Only after that should we construct the final daily tradable universe using rules such as price filters, liquidity filters, market-cap rank, ADV rank, common-stock filters, and minimum history requirements.
