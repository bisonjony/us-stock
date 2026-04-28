# U.S. Stock Daily Data Processing and Modeling Pipeline Report

This report summarizes the data-processing and modeling-data construction pipeline for the U.S. stock daily dataset from 2019 to 2025-12-31. The goal is to build a daily cross-sectional stock-return prediction project using engineered alpha features and a LightGBM model, with later evaluation through IC, Sharpe, PnL, and backtesting metrics.

## Table of Contents

- [Data processing](#data-processing)
  - [1. Raw CSV to Parquet conversion](#1-raw-csv-to-parquet-conversion)
  - [2. Type conversion](#2-type-conversion-and-daily_core-construction)
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
- [Planned first LightGBM experiment](#planned-first-lightgbm-experiment)

# Data processing

The data-processing stage converts the raw 8 GB CSV into a typed and queryable daily panel. The goal of this stage is not to build the final model table, but to create a reliable base dataset for universe construction, feature engineering, label construction, and backtesting.

## 1. Raw CSV to Parquet conversion

The raw U.S. stock daily data was originally stored as one large CSV file. Since the file is too large to load directly into pandas, we first converted it into multiple Parquet files.

Script:

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

- Read all variables as strings during this first conversion step.
- Avoid pandas automatic type inference because identifier and flag columns such as CUSIP-like fields, CRSP flags, and exchange variables can contain mixed representations.
- Avoid date/time conversion during this step so that raw values are preserved for controlled casting later.

This step creates a memory-safe raw Parquet archive that DuckDB can scan efficiently.

## 2. Type conversion and `daily_core` construction

After raw Parquet conversion, we used DuckDB SQL to cast each variable to its intended type and construct a typed `daily_core` table.

Script:

```text
src/build_daily_core.py
```

Main tasks:

- Read all 52 raw Parquet files using DuckDB.
- Normalize variable names to lowercase.
- Use the CRSP variable dictionary to assign intended types such as integer, double/decimal, date, and character.
- Cast each raw string column into its intended type.
- Preserve all original variables from the raw data.
- Preserve the raw CRSP price field `dlyprc`.
- Create a price magnitude variable `prc` from `dlyprc` for downstream analysis and universe construction.
- Add a flag for negative raw CRSP prices, because negative CRSP prices are a data convention rather than economically negative prices.

For casting failures, non-empty raw values that failed conversion would be saved separately for manual inspection. In the actual run, all casting was successful.

Output:

```text
data/clean_parquet/daily_core/
```

The resulting `daily_core` table is a typed, mostly lossless daily stock panel. It is not yet a trading universe.

## 3. Missingness and abnormal-value scan

After constructing `daily_core`, we scanned each variable for missingness and abnormal values.

Script:

```text
src/scan_missing_abnormal.py
```

Main outputs:

```text
data/clean_parquet/daily_core_missing_abnormal_report.csv
```

For each variable, the scan reports:

- total number of rows,
- missing count,
- missing percentage,
- abnormal count,
- abnormal percentage,
- abnormal rule used.

Selected results from the scan:

| variable | total rows | missing count | missing % | abnormal count | abnormal % | interpretation |
|---|---:|---:|---:|---:|---:|---|
| `dlyprc` / `prc` | 15,749,137 | 47,018 | 0.2985% | 5,426 | 0.0345% | Missing/zero prices are mostly non-tradable or terminal rows; very high prices include valid stocks such as BRK.A. |
| `dlyopen`, `dlyhigh`, `dlylow`, `dlyclose` | 15,749,137 | about 386,000 | about 2.45% | 1,760 | about 0.011% | OHLC missingness is often structural and related to non-trading or quote-priced rows. |
| `dlybid`, `dlyask` | 15,749,137 | about 51,600 | about 0.328% | 1,760 | about 0.011% | Mostly non-tradable or abnormal quote records. |
| `dlyret`, `dlyretx` | 15,749,137 | 54,680 | 0.3472% | 2 | 0.000013% | Missing returns are explained by CRSP return-missing flags. |
| distribution variables | 15,749,137 | about 98.7% missing | high | low | low | Distribution fields are event-specific; missing usually means no distribution event. |
| `dlynumtrd`, `dlymmcnt`, `exchangetier` | 15,749,137 | about 57% missing | high | 0 | 0% | These fields have limited coverage and should be optional, not mandatory. |

This scan provided a global view. It did not by itself determine whether a value was unusable, because many missing values are structural rather than errors.

## 4. Missingness and abnormality investigation

We then manually investigated missingness and abnormal values for key variable groups.

Script:

```text
src/investigate_missing_examples.py
```

The investigation script supports:

- random sampling of rows where a variable is missing,
- printing all columns for sampled rows,
- grouped missingness summaries by variables such as `year`, `primaryexch`, `securitytype`, `sharetype`, and `tradingstatusflg`,
- abnormal-example extraction,
- CSV export for manual inspection.

Detailed reports:

```text
missing_investigation.md
abnormal_investigation.md
```

Main conclusions:

- OHLC variables are often missing together. Many such rows still have valid `dlyprc`, especially when the price is based on bid/ask quotes. These rows should generally be kept and handled with feature-level indicators rather than automatically dropped.
- Missing `prc` is more serious because `prc` is required for tradability, market-cap logic, and dollar-volume logic.
- Missing bid/ask fields are mostly concentrated in inactive, suspended, halted, or delisting rows. In the final universe, bid/ask missingness is very rare.
- Missing `dlyvol`, `dlycap`, and `dlyprcvol` is mostly associated with non-tradable or terminal rows. `dlyvol` and `dlycap` are essential for universe construction.
- Missing return variables are mostly associated with non-trading or inactive rows. Active rows with missing returns should not be imputed as zero.
- Distribution-event variables are mostly missing because most stock-days do not have dividends, splits, or other distribution events. Missingness should generally be interpreted as no recorded event.
- Abnormal high `prc` values are not automatically errors; valid high-priced stocks such as Berkshire Hathaway Class A can exceed simple abnormal thresholds.
- Extreme `dlyret` values were rare and internally explainable by major price changes. They are kept in the raw return source.

## 5. Duplicate stock-date removal

Before label construction and feature engineering, we created a de-duplicated core table with exactly one row per stock-date pair.

Script:

```text
src/label_creation_screening.py
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
src/create_daily_universe.py
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
src/scan_universe_missing_abnormal.py
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
src/diagnose_universe_edge_case.py
```

Main findings:

- The OHLC-missing rows mostly have `dlyprcflg = BA`, meaning the price is based on bid/ask rather than trade-based OHLC.
- The 608 missing `bid_ask_spread` values are mostly due to `ask < bid`, not missing bid/ask fields.
- There are four rows where `dlybid` or `dlyask` is missing while core price, volume, and return fields are valid.
- There is one OHLC inconsistency:

```text
2023-06-05, JOBY: open=5.72, high=6.09, low=5.76, close=5.99
```

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
src/create_backtesting_data.py
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
src/create_features.py
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
src/create_model_panel.py
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

# Planned first LightGBM experiment

The next planned step is to train a baseline LightGBM model.

Recommended split:

| split | years | purpose |
|---|---|---|
| training | 2019--2022 | Fit model parameters. |
| validation | 2023 | Tune hyperparameters and inspect IC. |
| out-of-sample test / backtest | 2024--2025 | Final performance evaluation after model choices are fixed. |

Because the response is a 5-day forward return, the last 5 trading dates near split boundaries should be removed from the training or validation split when necessary to avoid label-window leakage across periods.

The first evaluation should focus on:

- validation daily Rank IC,
- mean IC,
- ICIR,
- feature importance,
- later 2024--2025 portfolio backtest using `backtesting_data` return scenarios.
