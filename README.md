# Daily U.S. Equity Alpha Research

**Author:** Xuhui Liu  
**Affiliation:** Ph.D. Student in Statistics, University of Illinois Urbana-Champaign  
**Last updated:** April 2026

This repository contains an end-to-end daily U.S. equity alpha research pipeline, including raw daily stock data processing, investable universe construction, 5-day forward return label creation, alpha feature engineering, LightGBM model validation, and out-of-sample portfolio backtesting.

The final validation-selected model is a shallow LightGBM trained on 2019--2023 data and evaluated on 2024--2025. It achieves an out-of-sample daily Rank IC of **0.0207** with a t-statistic of **5.62**. In the best gross backtest case, the long-only top-decile portfolio has a cumulative return of **41.7%**, annualized return of **19.3%**, Sharpe ratio of **1.91**, and maximum drawdown of **-23.3%** before transaction costs. Under a 10 bps one-way transaction-cost assumption, the same long-only strategy has a cumulative return of **16.4%**, annualized return of **8.0%**, and Sharpe ratio of **0.86**, showing that the signal is statistically meaningful but sensitive to turnover and implementation costs.

For the full technical report, see [`report.md`](report.md).

---

## Project overview

The goal is to build a realistic medium-frequency cross-sectional stock-return prediction pipeline:

1. Process raw U.S. daily stock data into typed Parquet tables.
2. Construct a daily investable universe using price, liquidity, exchange, and security-type filters.
3. Create next-5-day forward return labels while handling missing returns and delisting-related events.
4. Engineer return, volatility, liquidity, volume, and price-pressure features.
5. Train and validate LightGBM models using time-based splits.
6. Backtest long-only and long-short top-decile strategies on 2024--2025 out-of-sample data.

---

## Data split

| period | role |
|---|---|
| 2019--2022 | training |
| 2023 | validation / model selection |
| 2024--2025 | out-of-sample testing and backtesting |

Because the response is a 5-day forward return, the last 5 trading dates near split boundaries are removed when needed to avoid label-window leakage.

---

## Headline results

### Model signal quality

| metric | value |
|---|---:|
| Test period | 2024--2025 |
| Daily Rank IC | 0.0207 |
| Rank IC t-stat | 5.62 |
| Positive Rank IC fraction | 63.2% |

### Best gross portfolio result

| strategy | cost | cumulative return | annualized return | Sharpe | max drawdown |
|---|---:|---:|---:|---:|---:|
| Long-only top decile | 0 bps | 41.7% | 19.3% | 1.91 | -23.3% |
| Long-only top decile | 10 bps | 16.4% | 8.0% | 0.86 | -25.8% |

The long-short version is not yet satisfactory after transaction costs. The current strongest result is the long-only top-decile strategy, but turnover remains too high.

---

## Repository structure

```text
src/
  data_processing/
    csv_to_parquet.py
    build_daily_core.py
    label_creation_screening.py

  create_universe/
    create_daily_universe.py

  create_features/
    create_features.py

  create_backtest_data/
    create_backtesting_data.py
    create_backtesting_model_panel.py

  create_model_panel/
    create_model_panel.py

  diagnose/
    scan_missing_abnormal.py
    investigate_missing_examples.py
    investigate_nonmissing_examples.py
    scan_universe_missing_abnormal.py
    scan_model_panel_missing.py
    diagnose_universe_edge_case.py
    diagnose_universe_ready_base_duplicates.py

  train_model/
    compute_single_feature_ic.py
    compute_feature_score_baselines.py
    train_lgbm_baseline.py
    train_ridge_top_features_baseline.py
    train_final_lgbm_for_backtest.py

  backtest/
    run_backtest.py
````

---

## Pipeline

### 1. Data processing

```bash
python src/data_processing/csv_to_parquet.py
python src/data_processing/build_daily_core.py
python src/data_processing/label_creation_screening.py
```

### 2. Universe construction

```bash
python src/create_universe/create_daily_universe.py
```

### 3. Feature engineering

```bash
python src/create_features/create_features.py
```

### 4. Label and backtesting data creation

```bash
python src/create_backtest_data/create_backtesting_data.py
```

### 5. Model panel construction

```bash
python src/create_model_panel/create_model_panel.py
python src/create_backtest_data/create_backtesting_model_panel.py
```

### 6. Model validation

```bash
python src/train_model/compute_single_feature_ic.py
python src/train_model/compute_feature_score_baselines.py
python src/train_model/train_lgbm_baseline.py
```

### 7. Final training and backtest

```bash
python src/train_model/train_final_lgbm_for_backtest.py
python src/backtest/run_backtest.py
```

---

## Modeling approach

The final model is a shallow LightGBM trained on the top 20 stable features selected using 2019--2022 single-feature IC diagnostics. The validation-selected configuration uses:

```text
objective = regression
learning_rate = 0.02
num_leaves = 15
max_depth = 4
min_data_in_leaf = 2000
lambda_l2 = 20
feature_fraction = 0.9
bagging_fraction = 0.8
max_bin = 63
```

The model target is the date-wise robustly standardized next-5-day return:

```text
target_5d_cs_zscore
```

Continuous features are also transformed cross-sectionally by date using p01/p99 winsorization and median-IQR standardization.

---

## Backtest design

The main backtest uses a daily signal with 5-day overlapping holding sleeves.

For each signal date:

1. Predict a score for each stock in the daily universe.
2. Rank stocks by prediction score.
3. For the long-only strategy, buy the top 10%.
4. For the long-short strategy, buy the top 10% and short the bottom 10%.
5. Hold each daily sleeve for 5 trading days.
6. Evaluate 0, 5, 10, and 20 bps one-way transaction-cost assumptions.

The reported backtest uses normalized NAV starting at 1.0. Dollar PnL can be obtained by multiplying returns by the chosen initial capital.

---

## Important limitations

This project is a research prototype, not a production trading system.

Current limitations include:

* The feature set uses only daily price, return, volume, liquidity, and security metadata.
* No company fundamentals, earnings data, analyst revisions, intraday data, or news data are used.
* Many current alpha features are basic and highly correlated.
* The final model is intentionally shallow to reduce overfitting, but more advanced architectures may be explored later.
* The current strategy has high turnover and is sensitive to transaction costs.
* Long-short performance is weak; the strongest current result is long-only.

---

## Future work

Planned improvements include:

1. **Data quality and data expansion**

   * Add fundamentals, earnings, analyst revision, news, or alternative data.
   * Improve corporate-action and delisting handling.
   * Add stricter execution-price modeling.

2. **Alpha design**

   * Create less correlated alpha features.
   * Add industry-relative and market-neutral signals.
   * Improve momentum, reversal, volatility, liquidity, and event-driven features.

3. **Model design**

   * Test ranking-oriented objectives instead of MSE.
   * Explore custom losses involving IC or portfolio utility.
   * Evaluate more complex models such as LSTM, temporal CNNs, or attention models with strong overfitting control.

4. **Portfolio construction**

   * Reduce turnover through weekly rebalancing or trade thresholds.
   * Test different long proportions.
   * Evaluate long-only excess return relative to S&P 500 or an equal-weight universe benchmark.
   * Add explicit beta, sector, size, and liquidity exposure controls.

---

## Data availability

The raw and processed stock-level data are not included in this repository because the source data are licensed. The repository contains code and documentation only. To reproduce the full pipeline, users need access to their own licensed daily U.S. equity dataset with comparable fields.


