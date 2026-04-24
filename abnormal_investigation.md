## Variables investigated

We investigate:

* `dlyprc`: CRSP daily price
* `prc`: cleaned price magnitude, constructed as `ABS(dlyprc)`
* related fields:

  * `dlyprcflg`
  * `tradingstatusflg`
  * `dlydelflg`
  * `dlyret`
  * `dlyvol`
  * `dlycap`

The abnormal rule used in the diagnostic report was:

```text
ABS(dlyprc) <= 0 OR ABS(dlyprc) > 100000
```

This rule intentionally flags both zero/unusable prices and extremely high prices for manual inspection.

---

### Abnormal summary

| Variable | Total rows | Abnormal count | Abnormal percentage |
| -------- | ---------: | -------------: | ------------------: |
| `dlyprc` | 15,749,137 |          5,426 |             0.0345% |

The abnormal rows split into two very different patterns:

| Pattern                                             | Count | Percentage among abnormal `dlyprc` rows | Percentage among all rows |
| --------------------------------------------------- | ----: | --------------------------------------: | ------------------------: |
| Delisting / terminal rows with `dlyprc = 0`         | 3,666 |                                  67.56% |                   0.0233% |
| Active high-price stock rows with `dlyprc > 100000` | 1,760 |                                  32.44% |                   0.0112% |

Key interpretation:

* The `dlyprc = 0` rows are genuinely not valid normal trading prices.
* The `dlyprc > 100000` rows are **not necessarily errors**. They appear to correspond to valid high-priced stocks, especially Berkshire Hathaway Class A-style observations.

Therefore, the current abnormal rule is useful for diagnosis, but not all flagged rows should be removed.

---

### Abnormal patterns

#### Pattern 1: Delisting / terminal rows with zero price

Characteristics:

* `dlyprc = 0`
* `prc = 0`
* `dlyprcflg = DA`
* `tradingstatusflg = D`
* `primaryexch = X`
* `dlydelflg = Y`
* `securityactiveflg = N`
* daily trading variables such as volume, OHLC, bid, and ask are often missing
* distribution or delisting-related payment fields may be populated

Prevalence:

* 3,666 rows
* 67.56% of abnormal `dlyprc` rows
* 0.0233% of all rows

Interpretation:

These are not normal market-trading observations. The zero price should not be interpreted as a tradable stock price. These rows appear to be terminal or delisting-related observations where the economic outcome may be represented through delisting/distribution fields rather than a normal daily trading price.

---

#### Pattern 2: Active high-price stock rows

Characteristics:

* `dlyprc > 100000`
* `prc > 100000`
* `dlyprcflg = TR`
* `tradingstatusflg = A`
* `primaryexch = N`
* `securitytype = EQTY`
* `sharetype = NS`
* price, return, volume, market cap, bid, ask, and OHLC variables are available
* sample rows correspond to very high-priced equity observations, such as Berkshire Hathaway Class A-style data

Prevalence:

* 1,760 rows
* 32.44% of abnormal `dlyprc` rows
* 0.0112% of all rows

Interpretation:

These rows are likely **valid observations**, not input errors. The threshold `ABS(dlyprc) > 100000` is too conservative for U.S. equities because a small number of legitimate stocks can trade above $100,000 per share.

These rows should not be removed merely because price is high.

---

### Treatment plan

#### Pattern 1 treatment: zero-price delisting / terminal rows

Do not use these rows as normal trading candidates.

Recommended treatment:

* Keep them in `daily_core`
* Exclude them from the training / prediction universe
* Do not use `prc = 0` as a valid price
* Do not impute `dlyprc`
* Retain them for delisting / terminal return handling in backtest

Rationale:

* A stock with `dlyprc = 0` is not realistically tradable at a normal market price.
* These rows often represent terminal events rather than ordinary daily observations.
* Dropping them entirely could lose information needed for realized outcome calculation.

In the model panel, a simple filter will naturally remove them:

```text
prc > 0
tradingstatusflg = A
dlyvol is non-missing
```

---

#### Pattern 2 treatment: valid high-price active equity rows

Keep these rows if they pass normal universe filters.

Recommended treatment:

* Do not treat `dlyprc > 100000` as an automatic error
* Do not winsorize or cap the raw price in `daily_core`
* Keep raw `dlyprc` and `prc`
* Use scale-stable transformations in features, such as:

  * log price
  * log market cap
  * rank-normalized price features
  * cross-sectional ranks

Rationale:

* These observations appear to be valid active trading rows.
* Removing them would incorrectly exclude legitimate high-priced stocks.
* Raw price level is usually not a good alpha feature directly, so using log/rank transformations is safer.

The abnormal rule should be revised for future diagnostics:

```text
Old diagnostic rule:
  ABS(dlyprc) <= 0 OR ABS(dlyprc) > 100000

Better interpretation:
  dlyprc <= 0 is truly suspicious/unusable for trading
  dlyprc > 100000 should be flagged as "high price", not "bad price"
```

---

### Feature recommendation

For the first model version:

```text
Do not use raw dlyprc directly as a model feature.
Use transformed price-related features instead:
  log_prc = log(prc)
  size = log(dlycap)
  dollar_volume = prc * dlyvol
```

For abnormal handling:

```text
Keep high-price active rows.
Exclude zero-price rows from the tradable universe.
Preserve all rows in daily_core.
```

For future abnormal reports, separate price diagnostics into two categories:

```text
invalid_price_flag:
  dlyprc is null or prc <= 0

high_price_flag:
  prc > 100000
```

This is better than treating both as the same type of abnormality.

---

### Final decision

```text
daily_core:
  Keep all rows.
  Keep raw dlyprc unchanged.
  Keep prc = ABS(dlyprc).
  Preserve zero-price and high-price rows for auditability.

model_panel / universe:
  Exclude rows with prc <= 0.
  Exclude delisting / terminal rows as new trading candidates.
  Keep active high-price equities if they pass liquidity and tradability filters.

features:
  Do not use raw price level directly.
  Use log price, market cap, dollar volume, and cross-sectional ranks.
  Add invalid_price_flag if needed.
  Do not treat dlyprc > 100000 as invalid by itself.

backtest:
  Retain zero-price delisting rows for terminal outcome handling.
  Do not treat zero price as a normal executable trading price.
```

Overall conclusion:

```text
The abnormal dlyprc rows are not one homogeneous data-quality problem.
Most zero-price rows are delisting / terminal observations and should be excluded from the tradable universe but retained for backtest outcome handling.
High-price active rows are likely legitimate and should not be removed simply because they exceed 100,000.
```

## Variables investigated

We investigate:

* `disfacpr`: factor to adjust price

This is a **distribution / corporate-action adjustment variable**, not a normal daily trading variable. It is used to adjust prices after distributions so that prices before and after the event can be compared on an equivalent basis. CRSP documentation notes that `DisFacPr` can be set to `-1` for delisting distributions, and older CRSP documentation also states that reverse splits can produce values between `-1` and `0`. ([Center for Research in Security Prices][1])

---

### Abnormal summary

The abnormal rule used was:

```text
disfacpr < 0
```

| Variable   | Total rows | Non-missing count | Abnormal count | Abnormal percentage among all rows | Abnormal percentage among non-missing rows |
| ---------- | ---------: | ----------------: | -------------: | ---------------------------------: | -----------------------------------------: |
| `disfacpr` | 15,749,137 |           195,359 |          5,986 |                            0.0380% |                                     3.064% |

Key observations:

* `disfacpr` is mostly missing because it is an event-specific variable.
* Among non-missing `disfacpr` rows, about **3.06%** are negative.
* Negative `disfacpr` values are not necessarily data errors.
* The abnormal rows split into two main groups:

  * delisting / terminal distributions with `disfacpr = -1`
  * active reverse-split-style corporate actions with `-1 < disfacpr < 0`

Interpretation:

The initial rule `disfacpr < 0` is useful for diagnostics, but it should not automatically be interpreted as data corruption.

---

### Abnormal patterns

#### Pattern 1: Delisting / terminal distribution rows

Characteristics:

* `primaryexch = X`
* `tradingstatusflg = D`
* `dlydelflg = Y`
* `securityactiveflg = N`
* `dlyprc = 0`
* `prc = 0`
* `dlyvol`, `dlycap`, OHLC, bid, and ask are usually missing
* `disfacpr = -1`
* `disfacshr = -1`
* distribution / terminal payment fields may be populated

Prevalence:

| Pattern                   | Abnormal count | Percentage among abnormal `disfacpr` rows | Percentage among all rows |
| ------------------------- | -------------: | ----------------------------------------: | ------------------------: |
| Delisting / terminal rows |          3,583 |                                    59.85% |                   0.0228% |

Interpretation:

These rows are not normal trading observations. They likely represent terminal corporate-action outcomes where the security is delisted and the adjustment factor is set by CRSP convention.

These should not be treated as bad input records. They are economically meaningful terminal-event records.

---

#### Pattern 2: Active reverse-split-style corporate actions

Characteristics:

* `tradingstatusflg = A`
* `securityactiveflg = Y`
* `dlyprc`, `prc`, `dlyret`, `dlyvol`, and `dlycap` are available
* `distype` often indicates a split/reverse-split-style event
* `disfacpr` is negative but greater than `-1`, such as:

  * `-0.900000`
  * `-0.950000`
  * `-0.875000`
  * `-0.833333`
* `disfacpr` and `disfacshr` are often equal
* `dlyorddivamt = 0`
* `dlynonorddivamt = 0`

Prevalence:

| Pattern                      | Abnormal count | Percentage among abnormal `disfacpr` rows | Percentage among all rows |
| ---------------------------- | -------------: | ----------------------------------------: | ------------------------: |
| Active corporate-action rows |          2,403 |                                    40.15% |                   0.0153% |

Breakdown by primary exchange among active rows:

| Primary exchange | Active abnormal count |
| ---------------- | --------------------: |
| `Q`              |                 1,742 |
| `N`              |                   254 |
| `R`              |                   212 |
| `A`              |                   116 |
| `B`              |                    79 |

Interpretation:

These active rows are likely legitimate corporate-action observations, especially reverse-split-style events. A negative value in this context should not be treated as a simple invalid value.

---

### Treatment plan

#### Pattern 1 treatment: delisting / terminal distribution rows

Do not use these rows as new trading candidates.

Recommended treatment:

* Keep these rows in `daily_core`.
* Do not impute or overwrite `disfacpr`.
* Exclude these rows from the training / prediction universe.
* Retain them for backtest and realized-return handling.

Rationale:

* They are not normal tradable observations.
* They may encode the realized terminal outcome of a previously held position.
* Dropping them entirely could create survivorship or delisting bias.

---

#### Pattern 2 treatment: active reverse-split-style corporate actions

Keep these rows in `daily_core`.

For the model panel:

* Do not remove active rows solely because `disfacpr < 0`.
* Do not use raw `disfacpr` as a core baseline feature.
* Add corporate-action indicators if needed:

  * `has_distribution_event`
  * `has_split_or_reverse_split_event`
  * `negative_disfacpr_flag`
* Be careful when constructing price-based features around these event dates.

Rationale:

* These rows usually still have valid price, return, volume, and market-cap information.
* Negative `disfacpr` can be a valid CRSP convention for some corporate actions.
* Removing them would incorrectly delete real market observations.

---

### Feature recommendation

For the first baseline model:

```text
Do not include raw disfacpr as a core feature.
Do not require disfacpr to be non-missing.
Do not treat disfacpr < 0 as automatic data error.
```

For later event-aware models:

```text
has_distribution_event
negative_disfacpr_flag
split_or_reverse_split_event_flag
abs_disfacpr or transformed adjustment factor
```

Important timing warning:

```text
Only use distribution or corporate-action variables as predictive features if the event information is known by the prediction time.
Otherwise, these variables can introduce look-ahead bias.
```

For the first return-prediction project, it is safer to preserve `disfacpr` for diagnostics and backtest handling rather than use it directly as an alpha feature.

---

### Final decision

```text
daily_core:
  Keep disfacpr as-is.
  Do not overwrite negative values.
  Preserve both delisting-related and active corporate-action rows.

model_panel / universe:
  Do not exclude active rows solely because disfacpr < 0.
  Exclude delisting / terminal rows as new trading candidates.
  Use standard tradability filters based on status, price, volume, and liquidity.

features:
  Do not use raw disfacpr in the first baseline model.
  Optionally create corporate-action flags later.
  Be careful about timing to avoid leakage.

backtest:
  Retain delisting-related disfacpr rows.
  Use terminal and distribution information when handling realized outcomes.
```

Overall conclusion:

```text
Negative disfacpr values are not one homogeneous abnormal-data problem.
Most negative values are either delisting-distribution conventions or active split/reverse-split-style corporate-action records.
Therefore, disfacpr < 0 should be treated as an event/corporate-action flag, not as an automatic row-deletion rule.
```



## Variables investigated

We investigate:

* `dlyret`: daily total return
* `dlyretx`: daily price return excluding distributions
* `dlyreti`: daily income return component

The abnormal diagnostic rule was:

```text
dlyret < -1 OR dlyret > 20
```

There are only **two abnormal `dlyret` rows**, both with extremely large positive returns.

---

### Abnormal summary

| PERMNO | Ticker | Date       | `dlyret` | `dlyretx` | `dlyreti` | Price move    |
| -----: | ------ | ---------- | -------: | --------: | --------: | ------------- |
|  13883 | TPST   | 2023-10-11 |  39.7253 |   39.7253 |       0.0 | 0.2399 → 9.77 |
|  23127 | OCTO   | 2025-09-08 |  30.0897 |   30.0897 |       0.0 | 1.45 → 45.08  |

Key observations:

* Both rows are active trading rows: `tradingstatusflg = A`.
* Both have valid trade price flag: `dlyprcflg = TR`.
* Both have non-missing price, OHLC, bid/ask, volume, market cap, and return.
* Both have huge trading volume.
* `dlyret = dlyretx` and `dlyreti = 0`, so these are pure price moves, not income/distribution-driven returns.
* `dlyfacprc = 1.0` and `dlydistretflg = NO`, so these do not appear to be caused by price adjustment factors or distribution-return effects.

The return values are internally consistent:

```text
TPST:
  9.77 / 0.2399 - 1 = 39.7253

OCTO:
  45.08 / 1.45 - 1 = 30.0897
```

Therefore, these are probably **real extreme price moves**, not obvious data-entry mistakes.

---

### Abnormal patterns

#### Pattern 1: Extreme active microcap-style price jumps

Characteristics:

* `tradingstatusflg = A`
* `primaryexch = Q`
* `securitytype = EQTY`
* `sharetype = NS`
* `dlyprcflg = TR`
* `dlyretmissflg` is missing, meaning return is not flagged as missing
* `dlyret = dlyretx`
* `dlyreti = 0`
* no distribution event appears to explain the return
* volume is extremely high relative to usual small-cap activity

Prevalence:

| Pattern                    | Count | Percentage among abnormal `dlyret` rows | Percentage among all rows |
| -------------------------- | ----: | --------------------------------------: | ------------------------: |
| Extreme active price jumps |     2 |                                    100% |                ~0.000013% |

Interpretation:

These rows appear to be legitimate extreme market observations. They are not suspended rows, delisting rows, missing-price rows, or corporate-action adjustment rows.

The abnormal rule correctly flagged them for review, but after inspection, they should not be treated as obvious invalid data.

---

### Treatment plan

#### Pattern 1 treatment: extreme active price jumps

Keep these rows in `daily_core`.

Do not delete or overwrite the raw returns.

Recommended treatment:

* Preserve `dlyret`, `dlyretx`, and `dlyreti` as reported.
* Do not impute or cap values in `daily_core`.
* Add an optional diagnostic flag later, such as `extreme_return_flag`.
* For universe construction, keep these rows if they pass normal tradability and liquidity filters.
* For model training, handle them carefully because squared-error models can be dominated by extreme labels.

Rationale:

* The returns are internally consistent with the price change.
* These are active, valid trading observations.
* Removing them would erase real tail behavior, which matters for backtesting and risk analysis.

---

### Modeling recommendation

For LightGBM return prediction, these rows require careful handling.

Recommended approach:

```text
daily_core:
  Keep raw extreme returns.

model_panel:
  Keep rows if they pass tradability filters.

training label:
  Consider winsorized or clipped target for model fitting.

backtest/PnL:
  Use raw realized returns, not clipped returns.
```

This distinction is important.

For example:

```text
Training:
  target_1d_train = winsorized future return

Evaluation/backtest:
  realized_pnl = raw future return
```

Reason:

* If you train LGBM with squared loss on raw returns, a few 3000%–4000% observations can dominate the objective.
* But in a backtest, those tail events are economically real and should not be removed from realized PnL.

A good first-version plan is:

```text
Use raw dlyret for:
  realized return
  PnL
  backtest
  IC calculation sensitivity checks

Use winsorized target for:
  model training stability
```

---

### Final decision

```text
daily_core:
  Keep both abnormal dlyret rows unchanged.
  Do not delete them.
  Do not replace them with missing values.
  Treat them as legitimate extreme observations unless later evidence shows otherwise.

model_panel / universe:
  Keep these rows if they satisfy normal tradability filters.
  Do not exclude them solely because dlyret > 20.

features:
  Add optional extreme_return_flag.
  Avoid using raw extreme returns directly without robust transformations.

training:
  Consider winsorizing or clipping the target to reduce model instability.

backtest:
  Use raw realized returns.
  Do not clip realized PnL by default.
```

Overall conclusion:

```text
The two abnormal dlyret rows are not obvious data errors.
They are active, internally consistent, extreme price-jump observations.
They should be preserved in daily_core and backtest evaluation, but handled carefully during model training.
```


## Variables investigated

We investigate:

* `disdivamt`: dividend / distribution amount
* related distribution fields:

  * `disexdt`
  * `distype`
  * `dispaymenttype`
  * `disdetailtype`
  * `distaxtype`
  * `disfacpr`
  * `disfacshr`
  * `dlyfacprc`
  * `dlydistretflg`

The abnormal diagnostic rule was:

```text
disdivamt < 0
```

There is only **one abnormal row**.

---

### Abnormal summary

| PERMNO | Ticker | Date       | `disdivamt` | `disfacpr` | `disfacshr` | `dlyfacprc` | `dlydistretflg` |
| -----: | ------ | ---------- | ----------: | ---------: | ----------: | ----------: | --------------- |
|  22934 | EP     | 2024-03-06 |       -0.21 |  -0.035959 |         0.0 |    0.964041 | P1              |

Key observations:

* The row is an active equity observation:

  * `tradingstatusflg = A`
  * `securitytype = EQTY`
  * `sharetype = NS`
* Core trading fields are available:

  * `dlyprc = 5.84`
  * `dlyvol = 69,903`
  * `dlycap = 133,689.28`
  * OHLC and bid/ask fields are available
* The distribution event is recorded on the same date:

  * `dlycaldt = 2024-03-06`
  * `disexdt = 2024-03-06`
* `dlyret = dlyretx = -0.017452`
* `dlyreti = 0.0`
* `dlyorddivamt = 0.0`
* `dlynonorddivamt = 0.0`

Interpretation:

This does **not** look like a normal negative cash dividend. It is more likely a special corporate-action / distribution-adjustment record. The presence of `disfacpr = -0.035959`, `dlyfacprc = 0.964041`, and `dlydistretflg = P1` suggests that the row is related to a price adjustment rather than ordinary income return.

---

### Abnormal patterns

#### Pattern 1: Single active corporate-action adjustment row

Characteristics:

* `disdivamt < 0`
* `tradingstatusflg = A`
* `dlydelflg = N`
* `securityactiveflg = Y`
* `disordinaryflg = N`
* `distype = SP`
* `dispaymenttype = SS`
* `disdetailtype = SECRD`
* `distaxtype = N`
* `dlyorddivamt = 0`
* `dlynonorddivamt = 0`
* `dlyreti = 0`

Prevalence:

| Pattern                                | Count | Percentage among abnormal `disdivamt` rows | Percentage among all rows |
| -------------------------------------- | ----: | -----------------------------------------: | ------------------------: |
| Active corporate-action adjustment row |     1 |                                       100% |                ~0.000006% |

Interpretation:

This is not a broad data-quality issue. It is a single event-specific corporate-action row. The negative `disdivamt` should not be interpreted as a negative dividend yield signal.

The return behavior is also consistent with the adjustment factor. Although the raw price moved from `5.73` to `5.84`, the reported return is negative, which is consistent with the price adjustment factor `dlyfacprc = 0.964041`.

---

### Treatment plan

#### Pattern 1 treatment: single active corporate-action adjustment row

Keep the row in `daily_core`.

Do not overwrite or delete the negative `disdivamt`.

Recommended treatment:

* Preserve the raw value in `daily_core`
* Do not treat this as an ordinary dividend amount
* Do not use this row for ordinary dividend-yield feature construction
* Add a corporate-action flag if later using distribution variables
* Keep the row in the tradable universe if it passes standard price, volume, and status filters
* Use CRSP-provided return variables rather than manually reconstructing the return from raw price

Rationale:

* The row is active and has valid trading data
* The negative value appears tied to corporate-action adjustment logic
* Deleting the row would remove a valid market observation
* Treating `disdivamt = -0.21` as a normal negative dividend would be economically misleading

---

### Feature recommendation

For the first baseline model:

```text
Do not use raw disdivamt as a core feature.
Do not create dividend yield directly from disdivamt without filtering event types.
Do not treat negative disdivamt as ordinary income.
```

For later event-aware features, use safer indicators:

```text
has_distribution_event
has_nonordinary_distribution_event
negative_disdivamt_flag
price_adjustment_event_flag
```

If constructing dividend-related features later, separate ordinary cash dividends from corporate-action adjustments:

```text
ordinary_dividend_feature:
  use only ordinary distribution rows with positive cash-like amounts

corporate_action_feature:
  use split/security-distribution/adjustment flags separately
```

---

### Final decision

```text
daily_core:
  Keep the row unchanged.
  Preserve disdivamt = -0.21.
  Treat it as an event/corporate-action record, not a data-entry error.

model_panel / universe:
  Do not exclude this row solely because disdivamt < 0.
  Keep it if it passes normal tradability filters.

features:
  Do not use raw disdivamt in the first baseline model.
  Do not interpret this as a negative dividend.
  Add corporate-action/event flags only in later versions.

backtest:
  Use CRSP-provided dlyret / dlyretx for realized return.
  Do not reconstruct return naively from unadjusted price around this event.
```

Overall conclusion:

```text
The single abnormal disdivamt row is not evidence of a systematic data-quality problem.
It appears to be a valid active corporate-action adjustment record.
The correct treatment is to preserve it, avoid using it as an ordinary dividend amount, and rely on CRSP-provided return fields for realized-return calculations.
```


