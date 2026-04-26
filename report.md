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


## 6. Universe-preparation table construction

After the initial missingness and abnormality investigation, we created a universe-preparation layer. This step is implemented in:

```text
src/prepare_universe_base.py
```

This script constructs three outputs:

```text
data/clean_parquet/daily_universe_prepare_all/
data/clean_parquet/daily_universe_ready_base/
data/clean_parquet/daily_terminal_events/
```

The purpose of this step is to separate data-quality preparation from final investment-universe construction.

### 6.1 Full prepared table

`daily_universe_prepare_all` keeps all rows from `daily_core` and adds diagnostic flags and basic derived variables. Important flags include:

- trading-status and terminal-event flags, such as `is_active_trading_flag`, `non_tradable_status_flag`, and `terminal_event_flag`;
- price-quality flags, such as `invalid_price_flag`, `zero_price_flag`, `high_price_flag`, `price_from_bidask_flag`, and `negative_raw_price_flag`;
- OHLC flags, such as `has_ohlc_flag`, `ohlc_missing_flag`, `ohlc_inconsistent_flag`, and `ohlc_imputed_from_prc_flag`;
- bid/ask flags, such as `bidask_missing_flag` and `valid_bidask_flag`;
- return and liquidity flags, such as `return_missing_flag`, `invalid_volume_flag`, and `invalid_market_cap_flag`;
- security/share metadata flags;
- distribution and corporate-action event flags.

The script also creates basic derived variables such as:

```text
log_prc
log_dlycap
dollar_volume = prc * dlyvol
bid_ask_spread
intraday_ret_strict
intraday_range_strict
intraday_ret_clean
intraday_range_clean
```

The raw variables are preserved. The derived variables and flags are auxiliary variables for later universe construction and feature engineering.

### 6.2 Universe-ready base table

`daily_universe_ready_base` removes only stock-day rows that are structurally unsuitable as day-*t* trading candidates. The filtering rule is:

```text
is_active_trading_flag = 1
non_tradable_status_flag = 0
terminal_event_flag = 0
invalid_price_flag = 0
invalid_volume_flag = 0
invalid_market_cap_flag = 0
return_missing_flag = 0
security_metadata_missing_flag = 0
```

This filter removes inactive, suspended, halted, terminal/delisting, missing-price, missing-volume, missing-market-cap, missing-return, and missing-essential-metadata rows. It does not apply final universe rules such as common-stock filters, price thresholds, market-cap rank, ADV rank, or minimum history requirements.

This filtering uses only contemporaneous day-*t* information and therefore does not introduce look-ahead bias. Terminal and delisting rows are not deleted from the full prepared data; they are saved separately for later realized-return and backtest handling.

### 6.3 Terminal-event table

`daily_terminal_events` stores terminal or delisting-related rows separately. These rows should not be used as new trading candidates, but they may be needed later when computing realized returns for stocks selected before a terminal event.

This separation avoids the following mistake:

```text
Use only future universe membership to compute realized returns.
```

Future realized returns should be computed from a broader return source, not only from future universe membership, to avoid survivorship bias.

## 7. Duplicate stock-date diagnosis and treatment

Before final universe construction, we diagnosed duplicate `(permno, dlycaldt)` rows in `daily_universe_ready_base` using:

```text
src/diagnose_universe_ready_base_duplicates.py
```

The duplicate diagnostic showed:

| quantity | value |
|---|---:|
| total rows before duplicate handling | 15,469,047 |
| unique stock-days | 15,466,218 |
| duplicate stock-days | 2,789 |
| extra duplicate rows | 2,829 |
| maximum rows per stock-day | 3 |

A follow-up diagnostic showed that duplicate rows did not differ in the core trading fields:

```text
dlyprc, prc, dlyret, dlyretx, dlyreti, dlyvol, dlycap,
OHLC variables, dlybid, dlyask
```

The differences were concentrated in distribution metadata, especially:

```text
distype
disseqnbr
```

This suggests that duplicates are mostly caused by multiple distribution-event records attached to the same stock-date, rather than by conflicting price/return observations.

For the universe-ready base table, we require one row per stock-date. Therefore, `prepare_universe_base.py` was updated to preserve exactly one row for each `(permno, dlycaldt)` pair and remove the extra duplicate rows. The full prepared table still keeps the raw distribution-event records for auditability.

## 8. Daily stock universe construction

The daily stock universe was created from:

```text
data/clean_parquet/daily_universe_ready_base/
```

using:

```text
src/create_daily_universe.py
```

This script creates:

```text
data/clean_parquet/daily_stock_universe/
data/clean_parquet/daily_stock_universe_daily_summary.csv
data/clean_parquet/daily_stock_universe_yearly_summary.csv
```

The universe is constructed independently for each date. A stock is included in the daily universe if it satisfies the following conditions:

| rule | reason |
|---|---|
| Start from `daily_universe_ready_base` | Remove structurally unusable stock-day rows before final universe construction. |
| `securitytype = 'EQTY'` | Keep equity securities. |
| `securitysubtype = 'COM'` | Keep common-stock-like equities. |
| `sharetype = 'NS'` | Keep normal share type used for the common-stock universe. |
| `usincflg = 'Y'` | Focus on U.S.-incorporated stocks. |
| `shradrflg = 'N'` | Exclude ADRs. |
| `primaryexch IN ('N', 'Q', 'A')` | Focus on major U.S. exchanges. |
| `prc >= 5` | Exclude penny-stock-like names with severe microstructure noise and poor tradability. |
| `adv20 >= 1,000,000` | Require minimum 20-day average dollar volume. |
| `market_cap_rank <= 3000` | Keep a broad but reasonably investable market-cap universe. |
| `adv20_rank <= 4000` | Remove the most illiquid tail while allowing liquidity rank to be looser than market-cap rank. |
| `hist_ret_obs_252 >= 126` | Require roughly half a year of historical return observations for stable feature construction. |

Here,

```text
adv20 = 20-day rolling average of dollar_volume
dollar_volume = prc * dlyvol
market_cap_rank = daily cross-sectional rank of dlycap, descending
adv20_rank = daily cross-sectional rank of adv20, descending
```

The resulting universe contains 3,969,391 stock-day observations. This universe is intended to be the candidate set for later feature construction, prediction, and portfolio formation. It is not yet the final modeling table because labels and engineered features still need to be merged in.

## 9. Universe-level missingness and abnormality checks

After constructing `daily_stock_universe`, we scanned the universe table for missingness and abnormal values using:

```text
src/scan_universe_missing_abnormal.py
```

This produced:

```text
data/clean_parquet/daily_stock_universe_missing_abnormal_report.csv
data/clean_parquet/daily_stock_universe_abnormal_examples.csv
```

Important results:

| variable | missing count | missing percentage | interpretation |
|---|---:|---:|---|
| `prc` | 0 | 0.0000% | Good: all universe rows have usable price. |
| `dlyret` | 0 | 0.0000% | Good: all universe rows have current daily return. |
| `dlyretx` | 0 | 0.0000% | Good: all universe rows have current price return. |
| `dlyreti` | 0 | 0.0000% | Good: all universe rows have current income-return component. |
| `dollar_volume` | 0 | 0.0000% | Good: all universe rows have usable dollar volume. |
| `adv20` | 0 | 0.0000% | Good: all universe rows have valid rolling dollar-volume history. |
| `market_cap_rank` | 0 | 0.0000% | Good: all universe rows have valid market-cap rank. |
| `adv20_rank` | 0 | 0.0000% | Good: all universe rows have valid ADV rank. |
| `dlyclose`, `dlyhigh`, `dlylow` | 850 | about 0.0214% | Mostly bid/ask-priced observations without trade-based OHLC. |
| `dlyopen` | 854 | about 0.0215% | Same as above, plus four rows where open is missing but other OHLC fields exist. |
| `dlybid`, `dlyask` | 4 | about 0.0001% | Rare quote-field missingness. |
| `bid_ask_spread` | 608 | about 0.0153% | Mostly caused by crossed quotes or invalid bid/ask ordering. |
| `price_ohlc_consistency` | 1 | about 0.00003% | One OHLC consistency violation. |

The universe is therefore usable for the next step. The remaining missingness is small and should be handled in feature engineering rather than by deleting stock-days from the universe.

## 10. Universe edge-case diagnostics

We then examined universe edge cases using:

```text
src/diagnose_universe_edge_case.py
```

This produced targeted diagnostics for OHLC missingness, bid/ask missingness, bid-ask spread missingness, OHLC consistency, and return-missing flags.

Main findings:

- There are 850 OHLC-missing rows with `dlyprcflg = BA`, meaning that the daily price is based on bid/ask rather than a trade-based OHLC record.
- There are four additional rows where `dlyopen` is missing but `dlyclose`, `dlyhigh`, and `dlylow` are present.
- The 608 missing `bid_ask_spread` values consist of:

```text
604 rows with ask < bid
4 rows with bid or ask missing
```

- There is one OHLC consistency violation:

```text
2023-06-05, JOBY: open=5.72, high=6.09, low=5.76, close=5.99
```

Here `open < low`. This row should be kept for return/backtest purposes, but OHLC-derived features should be set to missing or flagged for this stock-date.

- There are three non-missing `dlyretmissflg` rows with value `MV`, but `dlyret` itself is present. These should not be used as a universe filter.

- High-price abnormal values are due to valid high-priced stocks such as Berkshire Hathaway Class A. These should not be removed. Raw price should not be used directly as a model feature; log or cross-sectional rank transformations are preferred.

## 11. Current conclusions and next step

The cleaned universe is now ready for label and feature construction.

Current decisions:

- Keep OHLC-missing rows in the universe.
- Keep the four bid/ask-missing rows in the universe.
- Keep the one OHLC-inconsistent row in the universe, but invalidate OHLC-derived features for that row.
- Keep high-price stocks such as Berkshire Hathaway Class A.
- Do not use raw `prc` directly as an alpha feature; use log, rank, or normalized transformations.
- Compute OHLC-derived features only when OHLC fields are complete and internally consistent.
- Compute bid-ask-spread features only when `dlybid > 0`, `dlyask > 0`, and `dlyask >= dlybid`.
- Use missingness and validity indicators as candidate model features.

The next planned step is response-variable construction. The recommended approach is to create labels from a broad return source such as `daily_core` or `daily_universe_prepare_all`, not from future universe membership. The main target should be a future compounded total return, such as:

```text
target_5d_raw(t) = product_{k=1}^5 (1 + dlyret(t+k)) - 1
```

Then the label table can be merged onto `daily_stock_universe` by `(permno, dlycaldt)`. This avoids survivorship bias because future realized returns will not be conditioned on whether a stock remains in the future universe.
