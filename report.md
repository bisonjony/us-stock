# Daily U.S. Equity Alpha Research: Data Engineering, Cross-Sectional Return Prediction, and Backtesting

**Author:** Xuhui Liu  
**Affiliation:** Ph.D. Student in Statistics, University of Illinois Urbana-Champaign  
**Last updated:** April 2026

This project builds an end-to-end daily U.S. equity alpha research pipeline, covering raw CRSP-style daily stock data processing, investable universe construction, 5-day forward return label creation, alpha feature engineering, LightGBM model validation, and 2024--2025 out-of-sample portfolio backtesting. The final validation-selected model is a shallow LightGBM trained on 2019--2023 data and evaluated on 2024--2025. It achieves an out-of-sample daily Rank IC of **0.0207** with a t-statistic of **5.62**. In the best gross backtest case, the long-only top-decile portfolio has a cumulative return of **41.7%**, annualized return of **19.3%**, Sharpe ratio of **1.91**, and maximum drawdown of **-23.3%** before transaction costs. After a 10 bps one-way transaction-cost assumption, the same long-only strategy has a cumulative return of **16.4%**, annualized return of **8.0%**, and Sharpe ratio of **0.86**, showing that the signal is statistically meaningful but highly sensitive to turnover and implementation costs.

## Table of Contents

- [Data processing](#data-processing)
  - [1. Raw CSV to Parquet conversion](#1-raw-csv-to-parquet-conversion)
  - [2. Type conversion and `daily_core` construction](#2-type-conversion-and-daily_core-construction)
  - [3. Missingness and abnormal-value scan](#3-missingness-and-abnormal-value-scan)
  - [4. Missingness and abnormality investigation](#4-missingness-and-abnormality-investigation)
  - [5. Duplicate stock-date removal](#5-duplicate-stock-date-removal)
- [Universe construction](#universe-construction)
  - [1. Universe source table](#1-universe-source-table)
  - [2. Daily universe rules](#2-daily-universe-rules)
  - [3. Universe-level missingness checks](#3-universe-level-missingness-checks)
  - [4. Universe edge-case diagnostics](#4-universe-edge-case-diagnostics)
  - [5. Universe decision summary](#5-universe-decision-summary)
- [Label and backtesting data creation](#label-and-backtesting-data-creation)
  - [1. Response variable choice](#1-response-variable-choice)
  - [2. Return source and survivorship-bias control](#2-return-source-and-survivorship-bias-control)
  - [3. Missing forward returns](#3-missing-forward-returns)
  - [4. Backtesting return definitions](#4-backtesting-return-definitions)
- [Feature engineering](#feature-engineering)
  - [1. Feature source and timing](#1-feature-source-and-timing)
  - [2. Basic validity and event flags](#2-basic-validity-and-event-flags)
  - [3. Return-history features](#3-return-history-features)
  - [4. Volatility and downside-risk features](#4-volatility-and-downside-risk-features)
  - [5. Liquidity and volume features](#5-liquidity-and-volume-features)
  - [6. Price-pressure features](#6-price-pressure-features)
  - [7. Optional features excluded from the first baseline](#7-optional-features-excluded-from-the-first-baseline)
- [Model panel construction](#model-panel-construction)
  - [1. Model-panel joins](#1-model-panel-joins)
  - [2. Label filtering](#2-label-filtering)
  - [3. Cross-sectional normalization](#3-cross-sectional-normalization)
  - [4. Final model-panel columns](#4-final-model-panel-columns)
- [Model training, validation, and model selection](#model-training-validation-and-model-selection)
  - [1. Single-feature IC diagnostics](#1-single-feature-ic-diagnostics)
  - [2. Feature-score baselines](#2-feature-score-baselines)
  - [3. Controlled LightGBM validation experiment](#3-controlled-lightgbm-validation-experiment)
- [Final model training and 2024--2025 backtest](#final-model-training-and-2024--2025-backtest)
  - [1. Backtesting model panel](#1-backtesting-model-panel)
  - [2. Test IC](#2-test-ic)
  - [3. Backtest design and assumptions](#3-backtest-design-and-assumptions)
  - [4. Backtest result table](#4-backtest-result-table)
  - [5. Best case and interpretation](#5-best-case-and-interpretation)
- [Future improvements](#future-improvements)
  - [1. Data quality and data coverage](#1-data-quality-and-data-coverage)
  - [2. Alpha design](#2-alpha-design)
  - [3. Model design](#3-model-design)
  - [4. Portfolio construction](#4-portfolio-construction)

# Data processing

This section summarizes the data-processing work completed for the U.S. stock daily dataset. The goal of these steps is to convert the raw data into a reliable typed daily panel, diagnose missing and abnormal values, and prepare the data for later universe construction, feature engineering, model training, and backtesting.

## 1. Raw CSV to Parquet conversion

The raw U.S. stock daily data from 2019--2026 was originally stored as a large CSV file. Since the file is too large to load directly into pandas, we first converted it into separate Parquet files for efficient downstream processing.

The conversion step is implemented in:

```text
python src/data_processing/csv_to_parquet.py
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
python src/data_processing/build_daily_core.py
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
python src/diagnose/scan_missing_abnormal.py
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
python src/diagnose/investigate_missing_examples.py
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

## 5. Duplicate stock-date removal

Before label construction and feature engineering, we created a de-duplicated core table with exactly one row per stock-date pair.

Script:

```text
python src/data_processing/label_creation_screening.py
```

Output:

```text
data/clean_parquet/daily_core_dup_removed/
```

The script preserves the first occurrence of each `(permno, dlycaldt)` pair and removes extra duplicate rows. This is necessary because some duplicate stock-date pairs arise from multiple distribution-event records attached to the same stock-day. Diagnostics showed that duplicated rows did not differ in core trading fields such as price, return, volume, market cap, OHLC, bid, or ask; differences were mainly in distribution metadata such as `distype` and `disseqnbr`.

This de-duplicated table is now the central broad source for:

- daily universe construction,
- backtesting label construction,
- time-series feature engineering.

# Universe construction

Universe construction creates the daily tradable candidate set. This is separate from feature engineering and label construction. The universe should be determined using only information available on or before date `t`.

## 1. Universe source table

The active universe workflow now starts directly from:

```text
data/clean_parquet/daily_core_dup_removed/
```

The previous intermediate `prepare_universe_base.py` workflow has been retired. We no longer maintain `daily_universe_prepare_all`, `daily_universe_ready_base`, or `daily_terminal_events` as active pipeline dependencies. The functionality we need is now covered by:

- `daily_core_dup_removed` for de-duplicated broad daily data,
- `create_daily_universe.py` for universe construction,
- `create_features.py` for feature-level flags and engineered features,
- `create_backtesting_data.py` for future-return and backtesting labels.

## 2. Daily universe rules

Script:

```text
python src/create_universe/create_daily_universe.py
```

Output:

```text
data/clean_parquet/daily_stock_universe/
data/clean_parquet/daily_stock_universe_daily_summary.csv
data/clean_parquet/daily_stock_universe_yearly_summary.csv
```

The universe is constructed independently for each trading date. A stock is included if it satisfies the following conditions.

| rule | reason |
|---|---|
| `tradingstatusflg = 'A'` | Keep stocks that are actively trading on date `t`. |
| `securityactiveflg = 'Y'` | Exclude inactive security records. |
| `dlydelflg = 'N'` | Exclude same-day terminal/delisting rows from new trading candidates. |
| `prc > 0` and non-missing | Require a usable day-`t` price. |
| `dlyvol > 0` and non-missing | Require a usable day-`t` volume field. |
| `dlycap > 0` and non-missing | Require a usable day-`t` market capitalization. |
| `dlyret` non-missing | Require current return availability for historical features and data integrity. |
| `securitytype = 'EQTY'` | Keep equity securities. |
| `securitysubtype = 'COM'` | Keep common-stock-like equities. |
| `sharetype = 'NS'` | Keep normal shares. |
| `usincflg = 'Y'` | Focus on U.S.-incorporated stocks. |
| `shradrflg = 'N'` | Exclude ADRs. |
| `primaryexch IN ('N', 'Q', 'A')` | Focus on NYSE, Nasdaq, and NYSE American style primary exchanges. |
| `prc >= 5` | Remove penny-stock-like names with severe microstructure noise and poor tradability. |
| `adv20 >= 1,000,000` | Require minimum 20-day average dollar volume. |
| `market_cap_rank <= 3000` | Keep a broad but investable market-cap universe. |
| `adv20_rank <= 4000` | Remove the least-liquid tail while allowing the liquidity rank to be looser than the market-cap rank. |
| `hist_ret_obs_252 >= 126` | Require roughly half a year of historical return observations for stable feature construction. |

Important definitions:

```text
dollar_volume_for_universe = prc * dlyvol
adv20 = 20-day rolling average of dollar_volume_for_universe
market_cap_rank = daily descending rank of dlycap
adv20_rank = daily descending rank of adv20
hist_ret_obs_252 = number of non-missing returns in the trailing 252-row window
```

The resulting universe contains about 3.97 million stock-day observations. This table is the candidate set for prediction and portfolio formation, not yet the final model table.

## 3. Universe-level missingness checks

After constructing `daily_stock_universe`, we checked missingness and abnormality again.

Script:

```text
python src/diagnose/scan_universe_missing_abnormal.py
```

Outputs:

```text
data/clean_parquet/daily_stock_universe_missing_abnormal_report.csv
data/clean_parquet/daily_stock_universe_abnormal_examples.csv
```

Important results:

| variable | missing count | interpretation |
|---|---:|---|
| `prc` | 0 | All universe rows have a usable price. |
| `dlyret`, `dlyretx`, `dlyreti` | 0 | All universe rows have current return information. |
| `dollar_volume_for_universe` | 0 | All universe rows have usable dollar volume. |
| `adv20` | 0 | All universe rows pass the rolling liquidity requirement. |
| `market_cap_rank`, `adv20_rank` | 0 | All universe rows have valid daily ranks. |
| `dlyclose`, `dlyhigh`, `dlylow` | about 850 | Mostly bid/ask-priced observations without trade-based OHLC. |
| `dlyopen` | about 854 | Same as above, plus a few rows where open is missing while high/low/close exist. |
| `dlybid`, `dlyask` | 4 | Very rare quote-field missingness. |
| `bid_ask_spread` | about 608 | Mostly caused by crossed quotes or invalid bid/ask ordering. |
| OHLC consistency | 1 | One row violates normal OHLC ordering. |

The universe is usable. Remaining missingness is small and should be handled in feature engineering rather than through row deletion.

## 4. Universe edge-case diagnostics

Script:

```text
python src/diagnose/diagnose_universe_edge_case.py
```

Main findings:

- The OHLC-missing rows mostly have `dlyprcflg = BA`, meaning the price is based on bid/ask rather than trade-based OHLC.
- The 608 missing `bid_ask_spread` values are mostly due to `ask < bid`, not missing bid/ask fields.
- There are four rows where `dlybid` or `dlyask` is missing while core price, volume, and return fields are valid.
- There is one OHLC inconsistency:

One OHLC inconsistency was detected in the universe-level diagnostics. The row was kept for return and backtest purposes, but OHLC-derived features are invalidated when open/high/low/close fields are internally inconsistent.

Here `open < low`. We keep the row for return and backtest purposes, but OHLC-derived features should be invalidated or set to missing for this row.

- High-price abnormal values are mostly valid high-priced stocks such as Berkshire Hathaway Class A. These should not be removed. Raw price is not used directly as a model feature; log and cross-sectional transformations are preferred.

## 5. Universe decision summary

Current universe decisions:

- Keep OHLC-missing rows in the universe.
- Keep rare bid/ask-missing rows in the universe.
- Keep the one OHLC-inconsistent row, but let feature engineering flag or invalidate OHLC-derived features.
- Keep high-priced stocks such as Berkshire Hathaway Class A.
- Do not use raw `prc` directly as a model feature.
- Compute OHLC-derived features only when OHLC fields are complete and internally valid.
- Compute bid-ask-spread features only when `dlybid > 0`, `dlyask > 0`, and `dlyask >= dlybid`.

# Label and backtesting data creation

The label and backtesting stage creates future-return outcomes from the broad return source. The key principle is that future realized returns should not be conditioned on future universe membership.

Script:

```text
python src/create_backtest_data/create_backtesting_data.py
```

Output:

```text
data/clean_parquet/backtesting_data/
data/clean_parquet/backtesting_data_analysis/
```

## 1. Response variable choice

The main modeling response is next 5-trading-day total return.

For stock `i` on date `t`, the raw complete 5-day target is:

```text
target_5d_raw(t) = product_{k=1}^5 (1 + dlyret(t+k)) - 1
```

We choose next 5-day return instead of next-day return for the first baseline because:

- next-day returns are extremely noisy and lead to high-turnover signals,
- a 5-day horizon is more stable for daily cross-sectional alpha research,
- many economically meaningful daily features, such as momentum, reversal, volatility, and volume pressure, may play out over several days,
- a 5-day horizon is still short enough to be useful for medium-frequency systematic trading.

The model-panel target later becomes a cross-sectionally standardized version of this 5-day raw target.

## 2. Return source and survivorship-bias control

Labels are created from:

```text
data/clean_parquet/daily_core_dup_removed/
```

not from future `daily_stock_universe` membership.

This matters because if a stock is in the universe on date `t`, then disappears from the universe later due to delisting, missing price, liquidity deterioration, suspension, or another adverse event, the backtest still needs to account for the realized outcome. Computing future returns only from future universe rows would introduce survivorship bias.

## 3. Missing forward returns

Before label creation, we screened missing `dlyret` in the de-duplicated core table. All missing returns had a documented `dlyretmissflg`.

Observed missing-return flags included:

| flag | interpretation | treatment |
|---|---|---|
| `NT` | Not tracked | Do not impute; not valid as an observed realized return. |
| `NS` | New security | Do not impute; first return after listing may be unavailable. |
| `MP` | Missing price | Do not impute; cannot compute reliable return. |
| `RA` | Return after not-tracked period | Treat as unreliable for label construction. |
| `DM` | Delisting price/amount missing | Delisting-related; handle with backtest stress rules. |
| `DG` | Delisting price more than 10 periods from delisting date | Delisting-related; handle with backtest stress rules. |
| `DP` | Delisting pending | Delisting-related; handle with backtest stress rules. |
| `GP` | Gap between prices too large | Treat as unreliable for label construction. |

For supervised training, we use only rows where all five future daily returns exist:

```text
target_5d_raw_complete_only
```

Rows with incomplete future returns are excluded from the training label, but they are not silently ignored in the backtesting return table.

## 4. Backtesting return definitions

`create_backtesting_data.py` creates multiple future-return columns:

| column | purpose |
|---|---|
| `target_1d_raw` | One-day forward return for diagnostics or future robustness checks. |
| `target_5d_raw_complete_only` | Complete 5-day compounded return; used as the supervised learning label. |
| `bt_5d_return_zero_after_missing` | Main conservative backtest return: compound observed returns until the first missing return, then assume zero return for remaining days. |
| `bt_5d_return_delist_stress_30` | Stress scenario: if the first missing return is delisting-related (`DM`, `DG`, `DP`), apply a -30% terminal return. |
| `bt_5d_return_delist_stress_100` | Severe stress scenario: if the first missing return is delisting-related, apply a -100% terminal return. |
| `target_has_missing_return` | Indicator for any missing return in the next 5 trading observations. |
| `target_first_missing_return_flag` | First missing-return reason in the forward window. |
| `target_missing_return_flag_set` | Set of missing-return flags appearing in the forward window. |
| `target_has_delisting_missing_flag` | Indicator for delisting-related missing-return flags. |

This design separates training labels from backtesting outcomes. Training uses complete labels, while backtesting can later evaluate robustness under multiple missing-return and delisting assumptions.

# Feature engineering

Feature engineering creates compact engineered features from the broad de-duplicated core table.

Script:

```text
python src/create_features/create_features.py
```

Output:

```text
data/clean_parquet/daily_features/
```

## 1. Feature source and timing

Features are computed from:

```text
data/clean_parquet/daily_core_dup_removed/
```

This is important because rolling features require historical observations before a stock enters the final universe. For example, if a stock enters the universe on date `t`, its 60-day volatility or 120-day return should be computed using its own historical data before `t`, not only from dates where it was already in the final universe.

All features use day-`t` or past information only. Future returns are not used in feature construction.

## 2. Basic validity and event flags

The feature table includes several binary indicators. These flags preserve data-quality and event information without dropping otherwise usable observations.

| feature | reasoning |
|---|---|
| `active_non_delisting_flag` | Indicates whether the row is active, non-delisting, and therefore aligned with tradable-row logic. |
| `price_from_bidask_flag` | Identifies rows where the price source is bid/ask based. This is useful because bid/ask-based prices may have different microstructure properties than trade-based prices. |
| `ohlc_missing_flag` | Indicates missing OHLC fields. Missing OHLC should not force row deletion, but OHLC-derived features should be treated carefully. |
| `valid_ohlc_flag` | Indicates OHLC fields are complete and internally consistent. |
| `ohlc_inconsistent_flag` | Flags rows where open/high/low/close violate normal ordering. |
| `bidask_missing_flag` | Flags missing bid or ask fields. |
| `valid_bidask_flag` | Indicates bid and ask are positive and satisfy `ask >= bid`. |
| `crossed_quote_flag` | Flags crossed quotes where `ask < bid`. These should invalidate spread-based features. |
| `distribution_event_flag` | Indicates a dividend, split, or other distribution event is recorded. Such events may affect returns and short-term price dynamics. |
| `cash_distribution_event_flag` | Flags cash-distribution event types. |
| `split_distribution_event_flag` | Flags split-like distribution events. |
| `ordinary_distribution_event_flag` | Flags ordinary distributions. |
| `share_factor_event_flag` | Flags share-factor events. These are rare and may capture corporate-action or share-adjustment effects. |
| `strong_up_high_volume_flag` | Event flag for a strong positive return day with high volume. |
| `strong_down_high_volume_flag` | Event flag for a strong negative return day with high volume. |

These flags are kept as raw 0/1 features in the model panel.

## 3. Return-history features

Return-history features capture short-term reversal and medium-term momentum.

| feature | definition | intuition |
|---|---|---|
| `ret_1d` | Cumulative return through day `t` over 1 day. | Captures immediate reversal or continuation. |
| `ret_2d` | Cumulative return over 2 days. | Captures very short-term price movement. |
| `ret_5d` | Cumulative return over 5 days. | Weekly reversal/momentum signal. |
| `ret_10d` | Cumulative return over 10 days. | Two-week momentum/reversal. |
| `ret_20d` | Cumulative return over 20 days. | About one-month momentum. |
| `ret_60d` | Cumulative return over 60 days. | Medium-term momentum. |
| `ret_120d` | Cumulative return over 120 days. | Longer medium-term momentum, within the maximum rolling window. |
| `ret_20_5` | Cumulative return from `t-20` to `t-5`. | Momentum signal skipping the most recent days to reduce short-term reversal contamination. |

The maximum rolling window for feature creation is 120 days. We do not include `ret_252d` in the active feature set.

Cumulative returns are computed using compounded daily returns:

```text
prod(1 + dlyret) - 1
```

If a required return in the window is missing, the rolling feature is set to missing. If a return in the window is less than or equal to -100%, the cumulative return is set to -1.

## 4. Volatility and downside-risk features

Risk features capture instability, lottery-like behavior, and downside risk.

For windows 5, 10, 20, and 60 days, the script creates:

| feature family | definition | intuition |
|---|---|---|
| `vol_*d` | Rolling standard deviation of daily returns. | Higher volatility may predict reversals, risk premia, or lower future risk-adjusted returns. |
| `skew_*d` | Rolling skewness of daily returns. | Captures lottery preference and asymmetric return behavior. |
| `max_ret_*d` | Maximum daily return in the window. | Captures jump-like upside behavior and potential lottery demand. |
| `min_ret_*d` | Minimum daily return in the window. | Captures recent crash or downside shock. |
| `downside_vol_*d` | Square root of average squared negative returns, with nonnegative returns contributing zero. | Focuses on downside risk rather than total volatility. |
| `hl_range_avg_*d` | Rolling average of daily high-low range. | Intraday range is a volatility proxy when OHLC is available. |

These features are computed only when the required number of observations is available. OHLC range features are missing when high/low fields are missing or invalid.

## 5. Liquidity and volume features

Liquidity and volume variables are central to daily alpha research because they capture tradability, investor attention, crowding, and trading pressure.

| feature | definition | intuition |
|---|---|---|
| `dollar_volume` | `prc * dlyvol`. | Daily dollar trading activity. |
| `log_dollar_volume` | `log(prc * dlyvol)`. | Log-scaled liquidity measure, less dominated by mega-cap stocks. |
| `log_prc` | `log(prc)`. | Price-level information after reducing scale effects. |
| `log_dlycap` | `log(dlycap)`. | Size proxy. |
| `bid_ask_spread` | `(ask - bid) / midpoint`, only if bid/ask are valid. | Trading-cost and liquidity proxy. |
| `hl_range` | `high / low - 1`, only if high/low are valid. | Intraday volatility and range proxy. |
| `open_close_ret` | `close / open - 1`, only if open/close are valid. | Intraday return pressure. |
| `turnover` | `dlyvol / (shrout * 1000)`. | Volume scaled by shares outstanding; proxy for trading intensity. |
| `amihud_illiq` | `abs(dlyret) / dollar_volume`. | Price impact / illiquidity proxy. |
| `adv5`, `adv20`, `adv60` | Rolling average dollar volume over 5/20/60 days. | Short- and medium-term liquidity. |
| `avg_volume_5d`, `avg_volume_20d`, `avg_volume_60d` | Rolling average share volume. | Baseline trading volume. |
| `volume_shock` | `dlyvol / avg_volume_20d`. | Abnormal share-volume activity. |
| `dollar_volume_shock` | `dollar_volume / adv20`. | Abnormal dollar-volume activity. |
| `turnover_avg_5d`, `turnover_avg_20d`, `turnover_avg_60d` | Rolling average turnover. | Persistent trading intensity. |
| `amihud_illiq_avg_5d`, `amihud_illiq_avg_20d`, `amihud_illiq_avg_60d` | Rolling average Amihud illiquidity. | Persistent price impact / illiquidity. |

The universe already enforces minimum liquidity, but these features still matter because relative liquidity, volume shocks, and turnover dynamics can predict future cross-sectional returns.

## 6. Price-pressure features

Price-pressure features combine return direction with abnormal trading activity.

| feature | definition | intuition |
|---|---|---|
| `ret_1d_x_volume_shock` | `ret_1d * volume_shock`. | A recent return is more informative when accompanied by abnormal volume. |
| `ret_5d_x_volume_shock` | `ret_5d * volume_shock`. | Weekly momentum or reversal conditional on abnormal volume. |
| `signed_abnormal_volume` | Positive abnormal volume on positive-return days, negative abnormal volume on negative-return days. | Captures directionally signed volume pressure. |
| `signed_abnormal_volume_ratio` | Signed version of `volume_shock - 1`. | Scale-free abnormal-volume pressure. |
| `signed_dollar_volume_shock` | Signed version of `dollar_volume_shock - 1`. | Directional dollar-volume pressure. |
| `strong_up_high_volume_flag` | `ret_1d >= 2%` and `volume_shock >= 2`. | Event indicator for strong up move with high volume. |
| `strong_down_high_volume_flag` | `ret_1d <= -2%` and `volume_shock >= 2`. | Event indicator for strong down move with high volume. |
| `up_high_volume_pressure` | `ret_1d * (volume_shock - 1)` on positive-return, high-volume days. | Continuous pressure measure for strong buying pressure. |
| `down_high_volume_pressure` | `ret_1d * (volume_shock - 1)` on negative-return, high-volume days. | Continuous pressure measure for strong selling pressure. |

The high-volume pressure variables are event-style variables. Missing values often mean the event did not occur, not that the raw data is unavailable. In the first model panel, their transformed values are filled with zero after cross-sectional transformation.

## 7. Optional features excluded from the first baseline

`create_features.py` creates `log_num_trades` and `log_market_maker_count`, but the first model panel excludes their transformed versions because they have high structural missingness. These features may be tested later in an ablation study.

# Model panel construction

The model panel combines the universe, the training label, and engineered features into a compact table ready for LightGBM.

Script:

```text
python src/create_model_panel/create_model_panel.py
```

Output:

```text
data/clean_parquet/model_panel/
data/clean_parquet/model_panel_analysis/model_panel_feature_columns.csv
data/clean_parquet/model_panel_analysis/model_panel_target_column.csv
```

## 1. Model-panel joins

The model panel is rooted in:

```text
data/clean_parquet/daily_stock_universe/
```

Then it joins:

```text
data/clean_parquet/backtesting_data/
```

to obtain only:

```text
target_5d_raw_complete_only
```

Then it joins:

```text
data/clean_parquet/daily_features/
```

to obtain engineered features.

The join keys are:

```text
permno, dlycaldt
```

## 2. Label filtering

After joining, the model panel keeps only rows with complete 5-day future return labels:

```text
target_5d_raw_complete_only IS NOT NULL
```

This automatically removes:

- the final observations for each stock that do not have enough future returns,
- rows where any of the next five daily returns is missing,
- rows that cannot be used for clean supervised training.

Backtesting should not use this complete-label-only model panel directly. Backtesting should use `backtesting_data`, which contains missing-return and delisting stress scenarios.

## 3. Cross-sectional normalization

For the response, the model panel applies date-wise robust normalization:

1. winsorize `target_5d_raw_complete_only` at the daily 1st and 99th percentiles,
2. center by the daily median,
3. scale by daily interquartile range divided by 1.349.

The final response is:

```text
target_5d_cs_zscore
```

For continuous features, the model panel applies the same daily robust transformation:

1. compute daily `p01`, `p25`, `p50`, `p75`, and `p99`,
2. winsorize each feature at `p01`/`p99`,
3. standardize by `(p75 - p25) / 1.349`,
4. output only the transformed feature:

```text
<feature>_cs_winsor_zscore
```

This cross-sectional transformation is done after joining to the stock universe, so each day’s normalization is computed on the actual tradable model universe rather than on the full raw CRSP panel.

Binary and event flags are kept as raw 0/1 features.

For the first baseline:

- `up_high_volume_pressure_cs_winsor_zscore` and `down_high_volume_pressure_cs_winsor_zscore` are filled with 0 when missing, because missing means the pressure event did not occur.
- `log_num_trades` and `log_market_maker_count` features are excluded because they have high structural missingness.
- The model panel is processed year by year to avoid memory problems in WSL.

## 4. Final model-panel columns

The final model panel keeps only necessary columns:

```text
permno
dlycaldt
year
ticker
primaryexch
siccd
naics
icbindustry
target_5d_cs_zscore
model features
```

There is only one response column:

```text
target_5d_cs_zscore
```

The feature list is saved to:

```text
data/clean_parquet/model_panel_analysis/model_panel_feature_columns.csv
```

The target metadata is saved to:

```text
data/clean_parquet/model_panel_analysis/model_panel_target_column.csv
```

# Model training, validation, and model selection

After constructing the model panel, we used a time-series split rather than a random split. This is necessary because stock-return prediction is a temporal forecasting problem, and random splitting would leak future market regimes into training.

| split | years | purpose |
|---|---|---|
| training | 2019--2022 | Fit candidate models and select stable features. |
| validation | 2023 | Select feature subsets and LightGBM hyperparameters. |
| out-of-sample test / backtest | 2024--2025 | Final evaluation after the model pipeline is fixed. |

Because the label is a forward 5-trading-day return, we remove the last 5 trading dates at relevant split boundaries. In particular, the last 5 trading dates of 2022 are removed when training models validated on 2023, and the last 5 trading dates of 2023 are removed when training the final 2019--2023 model for 2024--2025 backtesting. This prevents label windows from crossing into the next evaluation period.

## 1. Single-feature IC diagnostics

Script:

```text
python src/train_model/compute_single_feature_ic.py
```

Before tuning complex models, we computed daily single-feature Rank IC for every model feature. This diagnostic showed that the feature set contains real predictive information: the best single-feature validation Rank IC in 2023 reached about **0.0263**. This means the weak performance of the first all-feature model was not simply caused by an empty or useless feature set.

The strongest signals were mostly related to size, liquidity, bid-ask spread, illiquidity, volatility, and high-low range. This suggests that the first-generation alpha set is capturing a broad liquidity/risk/style effect more than a diverse set of independent alpha sources.

## 2. Feature-score baselines

Script:

```text
python src/train_model/compute_feature_score_baselines.py
```

We then tested simple signed feature-score baselines. Features were selected using only the 2019--2022 training-period IC table, and feature directions were assigned according to the sign of training-period IC. This avoids selecting features directly from the 2023 validation period.

The best simple feature-score baseline achieved validation Rank IC around **0.020** in 2023. This confirmed that some features remain predictive out of sample, but it also showed that naively adding more correlated features can reduce performance. In other words, the signals combine imperfectly and require careful model regularization.

## 3. Controlled LightGBM validation experiment

Script:

```text
python src/train_model/train_lgbm_baseline.py
```

Given the feature diagnostics, we moved away from a large all-feature LightGBM and instead trained controlled shallow LightGBM models using only the top stable features selected from 2019--2022. We tested top-K feature sets and strongly regularized tree settings.

The best validation model was:

```text
top20_medium_l2_20_leaf15
```

Main setup:

| item | value |
|---|---|
| feature set | top 20 stable features selected from 2019--2022 single-feature IC |
| training period | 2019--2022 |
| validation period | 2023 |
| objective | LightGBM regression with MSE/RMSE metric |
| learning rate | 0.02 |
| number of leaves | 15 |
| max depth | 4 |
| min data in leaf | 2000 |
| L2 regularization | 20 |
| feature fraction | 0.9 |
| bagging fraction | 0.8 |
| best iteration | 28 |
| validation daily Rank IC | 0.02453 |

This validation result beat the simple feature-score baseline and became the chosen model pipeline for final testing.

# Final model training and 2024--2025 backtest

After selecting the model pipeline using 2023 validation data, we froze the setup and trained the final model using all available pre-test data from 2019--2023.

Final training script:

```text
python src/train_model/train_final_lgbm_for_backtest.py
```

Backtesting script:

```text
python src/backtest/run_backtest.py
```

The final model is saved under:

```text
model_outputs/lgbm_final_top20_train_2019_2023_for_backtest/
```

The backtest result is saved under:

```text
backtest_result/lgbm_final_top20_train_2019_2023_test_2024_2025/
```

## 1. Backtesting model panel

For backtesting, we created a separate `backtesting_model_panel` rather than using the complete-label-only training panel.

Script:

```text
python src/create_backtest_data/create_backtesting_model_panel.py
```

The backtesting panel is rooted in the daily universe and joins:

- model features from `daily_features`,
- complete-only label `target_5d_raw_complete_only` and `target_5d_cs_zscore` for test IC evaluation,
- backtesting return columns from `backtesting_data` for portfolio PnL.

The test IC uses:

```text
target_5d_cs_zscore
```

The portfolio backtest uses realized 5-day backtesting returns, not the standardized label.

## 2. Test IC

The final model achieved the following 2024--2025 out-of-sample IC performance:

| metric | value |
|---|---:|
| prediction rows | 1,197,404 |
| IC rows | 1,184,295 |
| unique dates | 502 |
| overall Spearman IC | 0.0214 |
| daily Rank IC mean | 0.0207 |
| daily Rank IC standard deviation | 0.0822 |
| daily Rank IC t-stat | 5.62 |
| daily Rank IC positive fraction | 63.18% |

The test daily Rank IC is about **0.0207**, with a t-stat of **5.62**, indicating that the model has a statistically meaningful out-of-sample ranking signal.

## 3. Backtest design and assumptions

The strategy is a daily-rebalanced, overlapping 5-day holding-period strategy designed to match the 5-day prediction horizon.

Main assumptions:

| component | design |
|---|---|
| signal | LightGBM prediction score from the final top-20 model |
| test period | 2024--2025 |
| ranking | daily cross-sectional ranking within the stock universe |
| long-only portfolio | long the top 10% predicted stocks |
| long-short portfolio | long top 10%, short bottom 10% |
| weighting | equal weight within selected long and short legs |
| holding period | 5 trading days |
| sleeve design | 5 overlapping sleeves; each day opens a new 5-day sleeve and closes the sleeve opened 5 trading days earlier |
| total gross exposure | 1.0 |
| long-short exposure | +0.5 long, -0.5 short in steady state |
| long-only exposure | +1.0 long in steady state |
| NAV convention | initial NAV = 1.0 |
| reporting capital | $1,000,000 notional for dollar PnL interpretation |
| transaction costs | 0, 5, 10, and 20 bps one-way cost |

The backtest is currently a realized 5-day sleeve backtest. It is appropriate as a first evaluation for a 5-day prediction model, but it is not yet a full daily mark-to-market position-level simulation.

## 4. Backtest result table

The table below reports the severe delisting-stress return definition:

```text
bt_5d_return_delist_stress_100
```

The no-stress and -30% delisting-stress cases were effectively identical in this run, because selected universe stocks rarely had delisting-related missing forward returns.

| strategy | one-way cost | cumulative return | annualized return | annualized volatility | Sharpe | max drawdown | final NAV |
|---|---:|---:|---:|---:|---:|---:|---:|
| `long_only` | 0 bps | 41.66% | 19.31% | 9.47% | 1.91 | -23.30% | 1.4166 |
| `long_only` | 5 bps | 28.39% | 13.51% | 9.47% | 1.39 | -24.58% | 1.2839 |
| `long_only` | 10 bps | 16.36% | 7.99% | 9.47% | 0.86 | -25.84% | 1.1636 |
| `long_only` | 20 bps | -4.42% | -2.27% | 9.46% | -0.19 | -28.30% | 0.9558 |
| `long_short` | 0 bps | -1.66% | -0.85% | 4.28% | -0.18 | -13.65% | 0.9834 |
| `long_short` | 5 bps | -10.88% | -5.67% | 4.28% | -1.34 | -15.85% | 0.8912 |
| `long_short` | 10 bps | -19.23% | -10.26% | 4.28% | -2.51 | -21.23% | 0.8077 |
| `long_short` | 20 bps | -33.67% | -18.79% | 4.28% | -4.84 | -34.86% | 0.6633 |

## 5. Best case and interpretation

The best case is the **long-only portfolio with 0 bps transaction cost**:

| metric | value |
|---|---:|
| cumulative return | 41.66% |
| annualized return | 19.31% |
| annualized volatility | 9.47% |
| Sharpe | 1.91 |
| max drawdown | -23.30% |
| final NAV | 1.4166 |
| PnL on $1,000,000 notional | $416,597 |

This result is encouraging because the model has real test-period Rank IC and the long-only gross portfolio has a strong Sharpe. However, the performance is highly sensitive to transaction costs. At 10 bps one-way cost, the long-only cumulative return drops to **16.36%** and the annualized return drops to **7.99%**. At 20 bps, the long-only strategy becomes negative.

The long-short portfolio is weak even before costs. This suggests that the model is better at selecting relatively attractive long candidates than at identifying a profitable short basket. The current signal should therefore be viewed as a long-only ranking signal rather than a robust dollar-neutral long-short alpha.

# Future improvements

The current project successfully builds a full data-processing, feature-engineering, model-training, and backtesting pipeline. It also finds a statistically meaningful out-of-sample Rank IC. However, the portfolio result is not yet strong enough to claim a practical trading strategy after realistic transaction costs. Future work should improve the project in four directions.

## 1. Data quality and data coverage

The current dataset contains daily CRSP-style price, return, volume, quote, share, and corporate-action variables. This is useful for building a clean cross-sectional baseline, but the data source is limited.

Important missing data categories include:

- company fundamentals,
- analyst forecasts and revisions,
- earnings events and guidance,
- short interest and institutional ownership,
- news and sentiment data,
- intraday order-book and trade data,
- sector-level and macro variables.

Without company-specific and event-specific information, the model mostly learns broad size, liquidity, volatility, and microstructure effects. Adding richer data could help create more independent and economically interpretable alpha signals.

## 2. Alpha design

The current alpha design is intentionally simple. Most features are basic return-history, volatility, liquidity, range, and price-pressure variables. The single-feature IC analysis shows that some of them are predictive, but many are highly correlated with each other.

Current limitations:

- many features measure similar liquidity/risk effects,
- feature diversity is limited,
- there are few fundamental or event-driven signals,
- the signal is not strong enough to survive high turnover and high cost assumptions,
- the short side does not work well.

Future feature work should focus on more differentiated alpha families, such as event-based features, industry-relative features, residualized style factors, regime-conditioned features, and interaction features that are motivated by market microstructure or behavioral hypotheses.

## 3. Model design

The current best model is a shallow LightGBM trained with MSE/RMSE objective. LightGBM is a good first choice because it is fast, interpretable, robust on tabular data, and easier to regularize than deep learning models.

However, the model is not yet optimized for portfolio performance. Future directions include:

- changing the objective from MSE to a more rank-oriented or portfolio-oriented loss,
- adding an IC-like term to the loss function,
- testing pairwise or listwise ranking objectives,
- adding monotonic constraints for features with stable economic directions,
- using rolling or expanding-window retraining instead of a single static model,
- testing deep learning models such as LSTM, temporal convolution, Transformer, or attention-based architectures with strong overfitting control.

More complex models should only be adopted if they improve out-of-sample IC and portfolio performance after transaction costs.

## 4. Portfolio construction

The current portfolio construction is deliberately simple and has very high turnover. The average daily turnover is about **39.6%**, which causes transaction costs to dominate performance.

Future improvements should focus on lowering turnover and improving implementation realism:

- rebalance weekly instead of daily,
- trade only when rank changes are large enough,
- add no-trade bands around current holdings,
- test different long proportions such as top 5%, top 10%, and top 20%,
- compare long-only returns against S&P 500 and equal-weight universe benchmarks,
- report long-only excess return rather than only raw return,
- add sector, beta, size, volatility, and liquidity exposure diagnostics,
- add position-level daily mark-to-market returns instead of only realized 5-day sleeve returns,
- add capacity constraints such as maximum position size and maximum ADV participation.

The main next practical goal is to preserve the test Rank IC around 0.02 while reducing turnover enough that the strategy remains attractive under 10 bps one-way transaction cost.
