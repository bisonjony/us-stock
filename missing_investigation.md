# OHLC missingness diagnosis summary
## Variables investigated

We focus on the four trade-based OHLC variables:

* `dlyopen` (daily open price)
* `dlyhigh` (daily high price)
* `dlylow` (daily low price)
* `dlyclose` (daily close price)

These variables represent actual trade-based intraday information and are distinct from `dlyprc`, which is the CRSP daily price that may be derived from either trades or bid/ask quotes.

---

### Missing summary

The four OHLC variables exhibit **almost identical missing patterns**, indicating that they are missing jointly rather than independently.

Key observations:

* Missing percentage is approximately **2.45% for all four variables**
* Missing counts are nearly identical across all four variables
* Logical inconsistency (e.g., `high < low`) is extremely rare and negligible
* Missingness is therefore the primary issue, not data inconsistency

Interpretation:

* OHLC values are missing as a block when **trade-based price information is unavailable**
* However, in many of these cases, `dlyprc` is still available, suggesting alternative price sources

---

### Missing patterns

#### Pattern 1: Active rows with missing OHLC but valid `dlyprc`

Characteristics:

* `tradingstatusflg = A` (active)
* All OHLC variables are missing
* `dlyprc` and `prc` are available
* `dlyprcflg` often indicates quote-based pricing (e.g., bid/ask average)

Prevalence:

* Dominant pattern
* Roughly **87% of all missing OHLC rows**

Interpretation:

* No trade-based OHLC data exists
* A daily price proxy is still available from quotes
* This is a **data structure feature**, not an error

---

#### Pattern 2: Non-tradable / suspended-style rows

Characteristics:

* `tradingstatusflg` indicates non-active status (e.g., suspended/inactive)
* OHLC variables missing
* `dlyprc` may also be missing or invalid
* Return flags may indicate missing price or return

Prevalence:

* Around **12% of missing OHLC rows**

Interpretation:

* These rows represent stocks that were **not tradable on that day**
* They are not valid candidates for a trading strategy

---

#### Pattern 3: Delisting-related rows

Characteristics:

* `tradingstatusflg = D` (delisted)
* `dlydelflg = Y`
* OHLC variables missing
* Distribution or delisting-related variables may be populated

Prevalence:

* Small fraction (~1% of missing OHLC rows)

Interpretation:

* These rows correspond to **terminal events such as delisting**
* They are not normal trading observations but carry important outcome information

---

### Treatment plan

#### Pattern 1 treatment

* **Keep these rows in the tradable universe** if they satisfy other filters (price, volume, etc.)
* Do not overwrite original OHLC variables
* Introduce indicator variables:

  * whether OHLC is missing
  * whether price comes from bid/ask proxy (`dlyprcflg`)
* For OHLC-based alpha:

  * Use **strict features** computed only when OHLC exists
  * Optionally create **imputed versions** using `prc`, but always include flags

Rationale:

* These rows still contain useful price information
* Missing OHLC reflects lack of trades, not invalid data

---

#### Pattern 2 treatment

* **Exclude these rows from the tradable universe**
* Do not remove from `daily_core`, only filter during model construction

Rationale:

* These stocks are not realistically tradable
* Including them would create unrealistic backtest assumptions

Bias consideration:

* This exclusion uses only contemporaneous information (status at day *t*)
* Therefore, it does **not introduce look-ahead bias**

---

#### Pattern 3 treatment

* **Exclude from training/prediction universe**
* **Retain for backtesting and label construction**

Rationale:

* These are not valid new trading opportunities
* However, they represent **realized outcomes of previously held positions**

Important implication:

* If a stock is held before delisting, its delisting return must be included
* Dropping these rows would lead to **overly optimistic backtest results**

---

### Final decision

```text
Pattern 1:
  Keep in universe
  Add indicator variables
  Do not overwrite raw OHLC
  Use strict OHLC features primarily

Pattern 2:
  Exclude from tradable universe

Pattern 3:
  Exclude from training/prediction universe
  Retain for backtest and realized return handling
```

Overall principle:

```text
daily_core:
  Preserve all raw information

model_panel:
  Apply tradability filters using only information available at day t

backtest:
  Ensure all realized outcomes, including delisting effects, are included
```

Yes. The `prc` missingness is much cleaner than the OHLC missingness.

Because `prc = ABS(dlyprc)`, missing `prc` simply means **`dlyprc` itself is missing**. So this is a more serious issue than missing OHLC: there is no usable CRSP daily price proxy for that row.

## Variables investigated

We investigate:

* `prc`: cleaned positive price magnitude, defined as `ABS(dlyprc)`
* `dlyprc`: raw CRSP daily price
* related flags: `dlyprcflg`, `tradingstatusflg`, `dlyretmissflg`, `dlyretdurflg`, `primaryexch`

---

### Missing summary

From the missingness report:

| Variable | Total rows | Missing count | Missing percentage |
| -------- | ---------: | ------------: | -----------------: |
| `prc`    | 15,749,137 |        47,018 |            0.2985% |
| `dlyprc` | 15,749,137 |        47,018 |            0.2985% |

Since `prc` is constructed directly from `dlyprc`, these two have exactly the same missingness.

---

### Missing patterns

#### Pattern 1: Inactive / non-trading rows

This is the dominant pattern.


Prevalence:

| Pattern                                                    | Missing count | Percentage among missing `prc` | Percentage among all rows |
| ---------------------------------------------------------- | ------------: | -----------------------------: | ------------------------: |
| Inactive / non-trading rows, mostly `tradingstatusflg = X` |        42,041 |                         89.41% |                   0.2669% |

Interpretation:

These rows are not normal tradable market observations. They have no price, no volume, no return, no ticker, and no OHLC. This looks like a structural non-trading status rather than a random data error.

---

#### Pattern 2: Suspended / halted rows

This is the second largest pattern.


Prevalence:

| Pattern                             | Missing count | Percentage among missing `prc` | Percentage among all rows |
| ----------------------------------- | ------------: | -----------------------------: | ------------------------: |
| Suspended / halted rows, `S` or `H` |         4,434 |                          9.43% |                   0.0282% |

Interpretation:

These are securities that exist in the database but are not normally tradable on that date. They should not be included as candidate stocks for a daily trading strategy.

---

#### Pattern 3: Active rows with missing `prc`

This is rare but worth inspecting.

Prevalence:

| Pattern                                                | Missing count | Percentage among missing `prc` | Percentage among all rows |
| ------------------------------------------------------ | ------------: | -----------------------------: | ------------------------: |
| Active rows, `tradingstatusflg = A`, but `prc` missing |           439 |                          0.93% |                   0.0028% |

Interpretation:

These rows are more concerning than the first two patterns because the stock is labeled active, but no daily price exists. 

This likely means “active security, but missing price for this date.” For universe construction, these rows still cannot be traded because no price exists.

---

#### Pattern 4: Delisting-related rows

This is very small.

Prevalence:

| Pattern                                       | Missing count | Percentage among missing `prc` | Percentage among all rows |
| --------------------------------------------- | ------------: | -----------------------------: | ------------------------: |
| Delisting status rows, `tradingstatusflg = D` |           104 |                          0.22% |                   0.0007% |

Interpretation:

These rows may be related to terminal/delisting events. They should not be used as new trading candidates, but they may be useful later for realized return / delisting return handling.

---

### Treatment plan

#### Pattern 1 treatment: inactive / non-trading rows

Exclude from the tradable universe.

Reason:

* no price
* no volume
* no return
* no ticker/trading symbol in many cases
* not a realistic tradable stock-date observation

These rows should remain in `daily_core`, but they should not enter `model_panel`.

---

#### Pattern 2 treatment: suspended / halted rows

Exclude from the tradable universe.

Reason:

* even if the security exists, it is not normally tradable on that day
* without `prc`, we cannot form a realistic position or compute price-based features
* excluding them uses contemporaneous information, so this is not look-ahead bias

---

#### Pattern 3 treatment: active rows with missing `prc`

Exclude from the training/prediction universe for that date.

Reason:

* even if `tradingstatusflg = A`, there is no usable daily price
* price filter, market cap, dollar volume, and return features cannot be reliably computed
* this row is not usable as a trading candidate

But keep a diagnostic flag for these rows, because they are the most interesting missing-price cases.

---

#### Pattern 4 treatment: delisting-related rows

Do not use as new trading candidates.

But retain for later backtest / realized return handling.

Reason:

* delisting event rows are not normal tradable observations
* but they can matter if a stock was held before delisting
* dropping delisting outcomes entirely would make the backtest too optimistic

---

### Final decision

For `prc` missingness:

```text
daily_core:
  Keep all rows.
  Do not impute prc.
  Missing prc should remain missing.

model_panel / training universe:
  Exclude rows where prc is missing.
  Exclude rows where dlyprc is missing.
  Exclude inactive, suspended, halted, and delisting rows as new candidates.

backtest:
  Keep delisting-related information for realized return handling.
```

The practical universe filter should include at least:

```text
prc IS NOT NULL
dlyprc IS NOT NULL
tradingstatusflg = 'A'
dlyvol IS NOT NULL
dlyvol > 0
```

## Variables investigated

We investigate the two quote variables:

* `dlybid`: daily bid price
* `dlyask`: daily ask price

These variables are mainly useful for constructing liquidity / transaction-cost-related features, especially bid-ask spread:

`(dlyask - dlybid) / midpoint`

They are different from `dlyprc` / `prc`, which are the main daily price variables.

---

### Missing summary

The missingness of `dlybid` and `dlyask` is almost identical.

| Variable | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| -------- | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `dlybid` | 15,749,137 |        51,625 |            0.3278% |          1,760 |             0.0112% |
| `dlyask` | 15,749,137 |        51,628 |            0.3278% |          1,760 |             0.0112% |

Interpretation:

* Bid and ask are usually missing together.
* The missing percentage is small.
* Most missing bid/ask rows are not normal active trading observations.
* For normal active stocks, bid/ask missingness is very rare.

The `dlyask` missing-pattern report gives the following breakdown:

| Trading status | Missing count | Percentage among missing `dlyask` | Percentage among all rows |
| -------------- | ------------: | --------------------------------: | ------------------------: |
| `X`            |        42,041 |                            81.43% |                   0.2669% |
| `D`            |         4,666 |                             9.04% |                   0.0296% |
| `S`            |         4,359 |                             8.44% |                   0.0277% |
| `H`            |            75 |                             0.15% |                   0.0005% |
| `A`            |           487 |                             0.94% |                   0.0031% |

So the main missingness is concentrated in inactive, delisted, suspended, or halted-style rows.

---

### Missing patterns

#### Pattern 1: Inactive / non-trading rows

Characteristics:

* `tradingstatusflg = X`
* `primaryexch = X`
* `dlybid` and `dlyask` are missing
* `dlyprc` / `prc` are often also missing
* `dlyvol`, `dlyret`, and OHLC variables are often missing
* ticker / trading symbol may also be missing

Prevalence:

* Around **81.4% of missing `dlyask` rows**
* Around **0.27% of all rows**

Interpretation:

These are not normal market observations. They likely represent inactive or non-trading security-date rows. Missing bid/ask is expected here and should not be treated as a random input error.

---

#### Pattern 2: Suspended / halted rows

Characteristics:

* `tradingstatusflg = S` or `H`
* `dlybid` and `dlyask` are missing
* price and return fields may also be missing or unusable
* the security exists in the database but is not normally tradable on that day

Prevalence:

* `S`: around **8.44% of missing `dlyask` rows**
* `H`: around **0.15% of missing `dlyask` rows**

Interpretation:

These rows represent suspended or halted securities. They are not valid trading candidates for a daily strategy.

---

#### Pattern 3: Delisting-related rows

Characteristics:

* `tradingstatusflg = D`
* usually associated with delisting or terminal security events
* `dlybid` and `dlyask` are missing
* price / volume / OHLC fields may also be missing or abnormal
* delisting or distribution variables may contain relevant terminal-event information

Prevalence:

* Around **9.04% of missing `dlyask` rows**
* Around **0.03% of all rows**

Interpretation:

These are not normal tradable observations, but they may contain important information for realized returns if a position was held before delisting.

---

#### Pattern 4: Active rows with missing bid/ask

Characteristics:

* `tradingstatusflg = A`
* `dlybid` and/or `dlyask` missing
* much rarer than the inactive/suspended/delisted patterns
* may still have valid `prc`, volume, and return fields

Prevalence:

* Around **0.94% of missing `dlyask` rows**
* Only about **0.0031% of all rows**

Interpretation:

These are the only bid/ask missing rows that may still be part of the normal tradable universe. They should be handled differently from inactive or suspended rows.

---

### Treatment plan

#### Pattern 1 treatment: inactive / non-trading rows

Exclude from the tradable universe.

Rationale:

* These rows are not realistic trading candidates.
* They usually lack core trading variables, not just bid/ask.
* Keeping them would create unrealistic backtest assumptions.

These rows should remain in `daily_core`, but should be filtered out in `model_panel`.

---

#### Pattern 2 treatment: suspended / halted rows

Exclude from the tradable universe.

Rationale:

* A stock that is suspended or halted is not a normal candidate for a daily trading strategy.
* Excluding these rows uses contemporaneous information, so it does not introduce look-ahead bias.

---

#### Pattern 3 treatment: delisting-related rows

Exclude from the training / prediction universe as new candidates.

However, retain them for backtest and realized return handling.

Rationale:

* These rows are not normal buyable observations.
* But if a stock was held before delisting, the delisting-related return must be included in PnL.
* Dropping delisting outcomes would make the backtest too optimistic.

---

#### Pattern 4 treatment: active rows with missing bid/ask

Keep these rows in the universe **if they pass all other tradability filters**.

Do not impute bid/ask aggressively.

Recommended treatment:

* Keep `dlybid` and `dlyask` as missing.
* Add a flag such as `bidask_missing_flag`.
* Let bid-ask-spread features be missing.
* Let LightGBM handle the missing spread feature.
* Use `prc`, `dlyvol`, and return-based variables for core features.

Rationale:

* Missing bid/ask alone does not necessarily mean the stock is untradable.
* But bid-ask-spread features cannot be computed reliably.
* Imputing bid/ask may create artificial liquidity signals.

---

### Final decision

```text
daily_core:
  Keep all rows.
  Do not impute dlybid or dlyask.
  Preserve raw missingness.

model_panel / universe:
  Exclude inactive, suspended, halted, and delisting rows as new candidates.
  Keep active rows with missing bid/ask if other filters pass.

features:
  Create bidask_missing_flag.
  Compute bid_ask_spread only when both bid and ask are valid.
  Leave spread-related features missing otherwise.

backtest:
  Use delisting-related rows only for realized return handling, not as new trading candidates.
```

Overall conclusion:

```text
dlybid/dlyask missingness is mostly not a serious data-quality problem.
It is mainly concentrated in non-tradable, suspended, halted, or delisting rows.
For active tradable stocks, bid/ask missingness is rare and can be handled with a missingness flag.
```


## Variables investigated

We investigate three liquidity / size variables:

* `dlyvol`: daily trading volume
* `dlycap`: daily capitalization / market capitalization
* `dlyprcvol`: daily price-volume measure

These variables are important for universe construction because they are directly related to tradability, liquidity, and position-capacity constraints.

---

### Missing summary

The three variables have almost identical missing rates.

| Variable    | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| ----------- | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `dlyvol`    | 15,749,137 |        51,577 |            0.3275% |              0 |             0.0000% |
| `dlycap`    | 15,749,137 |        51,580 |            0.3275% |              0 |             0.0000% |
| `dlyprcvol` | 15,749,137 |        51,581 |            0.3275% |              0 |             0.0000% |

Key observations:

* Missingness is very low overall, around **0.33%**
* Missing counts are nearly identical across the three variables
* There are no abnormal values under the current diagnostic rules
* Missingness appears to be a **block-level trading-data issue**, not an independent missingness issue for each variable

Interpretation:

* If `dlyvol` is missing, then `dlycap` and `dlyprcvol` are usually also missing
* These rows are mostly not normal tradable observations
* For active stocks, missingness is extremely rare

---

### Missing patterns

#### Pattern 1: Inactive / non-trading rows

Characteristics:

* `tradingstatusflg = X`
* `primaryexch = X`
* `dlyvol`, `dlycap`, and `dlyprcvol` are missing
* Price, return, OHLC, bid/ask, and ticker fields are often also missing
* These rows typically have non-trading flags such as `NT`

Prevalence based on `dlyvol` missingness:

| Pattern                     | Missing count | Percentage among missing `dlyvol` rows | Percentage among all rows |
| --------------------------- | ------------: | -------------------------------------: | ------------------------: |
| Inactive / non-trading rows |        42,041 |                                 81.51% |                   0.2670% |

Interpretation:

These rows are not valid market observations for a trading strategy. They represent securities that exist in the database but are not actively trading on that date.

---

#### Pattern 2: Suspended / halted rows

Characteristics:

* `tradingstatusflg = S` or `H`
* `dlyvol`, `dlycap`, and `dlyprcvol` are missing
* Price and return variables may also be missing or unusable
* These rows are not normal tradable observations

Prevalence based on `dlyvol` missingness:

| Pattern              | Missing count | Percentage among missing `dlyvol` rows | Percentage among all rows |
| -------------------- | ------------: | -------------------------------------: | ------------------------: |
| Suspended rows (`S`) |         4,358 |                                  8.45% |                   0.0277% |
| Halted rows (`H`)    |            75 |                                  0.15% |                   0.0005% |

Interpretation:

These rows should not enter the tradable universe because the stock was not normally tradable on that day.

---

#### Pattern 3: Delisting-related rows

Characteristics:

* `tradingstatusflg = D`
* Often associated with terminal / delisting status
* `dlyvol`, `dlycap`, and `dlyprcvol` are missing
* Delisting-related variables may contain useful terminal-event information

Prevalence based on `dlyvol` missingness:

| Pattern                | Missing count | Percentage among missing `dlyvol` rows | Percentage among all rows |
| ---------------------- | ------------: | -------------------------------------: | ------------------------: |
| Delisting-related rows |         4,666 |                                  9.05% |                   0.0296% |

Interpretation:

These are not valid new trading candidates, but they may still matter for realized-return and backtest handling if a stock was held before delisting.

---

#### Pattern 4: Active rows with missing volume / capitalization

Characteristics:

* `tradingstatusflg = A`
* `dlyvol`, `dlycap`, or `dlyprcvol` missing
* Very rare compared with other missing patterns
* May reflect isolated missing trading or capitalization information

Prevalence based on `dlyvol` missingness:

| Pattern                           | Missing count | Percentage among missing `dlyvol` rows | Percentage among all rows |
| --------------------------------- | ------------: | -------------------------------------: | ------------------------: |
| Active rows with missing `dlyvol` |           437 |                                  0.85% |                   0.0028% |

Interpretation:

These rows are rare but important. Even though the security is labeled active, missing volume or capitalization makes the row difficult to use for tradable-universe construction.

---

### Treatment plan

#### Pattern 1 treatment: inactive / non-trading rows

Exclude from the tradable universe.

Rationale:

* These rows lack core trading information
* They usually also lack valid price, return, and ticker information
* They are not realistic candidates for a daily trading strategy

They should remain in `daily_core`, but should not enter `model_panel`.

---

#### Pattern 2 treatment: suspended / halted rows

Exclude from the tradable universe.

Rationale:

* Suspended or halted securities are not normally tradable
* Missing volume and price information makes trading simulation unrealistic
* Exclusion is based on contemporaneous information, so it does not introduce look-ahead bias

---

#### Pattern 3 treatment: delisting-related rows

Exclude from the training / prediction universe as new candidates.

Retain for backtest and realized-return handling.

Rationale:

* These rows are not normal buyable observations
* However, delisting outcomes must be included if the stock was held before delisting
* Dropping these rows entirely could make backtest performance too optimistic

---

#### Pattern 4 treatment: active rows with missing volume / capitalization

Exclude from the tradable universe for that day.

Rationale:

* `dlyvol` is necessary for liquidity filtering
* `dlycap` is necessary for market-cap filtering
* `dlyprcvol` or a derived dollar-volume measure is necessary for capacity/liquidity constraints
* Even if the stock is active, missing these fields makes the row unreliable for a daily trading strategy

However, do not delete these rows from `daily_core`.

---

### Derived feature recommendation

For modeling and universe construction, it may be better to compute your own dollar-volume variable:

```text
dollar_volume = prc * dlyvol
```

rather than relying only on `dlyprcvol`.

Reason:

* `prc` is your cleaned positive price magnitude
* `dlyvol` is the direct volume variable
* `dollar_volume` is easier to interpret and audit
* It can be used for rolling liquidity filters such as ADV20

However, if either `prc` or `dlyvol` is missing, then `dollar_volume` should remain missing.

---

### Final decision

```text
daily_core:
  Keep all rows.
  Do not impute dlyvol, dlycap, or dlyprcvol.
  Preserve raw missingness.

model_panel / universe:
  Exclude rows where dlyvol is missing.
  Exclude rows where dlycap is missing.
  Exclude inactive, suspended, halted, and delisting rows as new candidates.
  Exclude rare active rows with missing volume/capitalization for that date.

features:
  Use dlyvol for liquidity features.
  Use dlycap for market-cap filters and size features.
  Prefer computing dollar_volume = prc * dlyvol for liquidity screening.
  Compute rolling ADV features only from valid volume and price observations.

backtest:
  Retain delisting-related rows for realized-return handling.
  Do not treat delisting rows as new tradable candidates.
```

Overall conclusion:

```text
dlyvol, dlycap, and dlyprcvol missingness is mostly not a serious data-quality problem.
It is concentrated in non-tradable, suspended, halted, and delisting rows.
For active tradable stocks, missingness is extremely rare.
Rows missing these variables should generally be excluded from the trading universe for that date.
```


## Variables investigated

We investigate the three daily return variables:

* `dlyret`: daily total return
* `dlyretx`: daily price return excluding distributions/income
* `dlyreti`: daily income return component

These variables are central for return-based alpha construction, rolling momentum/volatility features, target construction, and backtest PnL calculation.

---

### Missing summary

The three return variables have the same missing count, which suggests that return missingness occurs as a block.

| Variable  | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| --------- | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `dlyret`  | 15,749,137 |        54,680 |            0.3472% |              2 |           0.000013% |
| `dlyretx` | 15,749,137 |        54,680 |            0.3472% |              2 |           0.000013% |
| `dlyreti` | 15,749,137 |        54,680 |            0.3472% |              0 |           0.000000% |

Key observations:

* Missingness is low overall, around **0.35%**
* `dlyret`, `dlyretx`, and `dlyreti` are almost always missing together
* Abnormal return values are extremely rare
* Most missing return rows are not normal active tradable observations
* A smaller subset of active rows has valid price/volume but missing return

Interpretation:

* Return missingness is mostly driven by non-trading or inactive rows
* For active rows, missing returns may occur when a comparable previous return cannot be computed, such as first valid observations, return-duration issues, or special security/event cases

---

### Missing patterns

#### Pattern 1: Inactive / non-trading rows

Characteristics:

* `tradingstatusflg = X`
* `primaryexch = X`
* `conditionaltype = NT`
* `dlyprc`, `prc`, `dlyvol`, and OHLC variables are often missing
* `dlyret`, `dlyretx`, and `dlyreti` are missing
* `dlyretmissflg` often indicates non-trading / no valid return

Prevalence:

| Pattern                     | Missing count | Percentage among missing return rows | Percentage among all rows |
| --------------------------- | ------------: | -----------------------------------: | ------------------------: |
| Inactive / non-trading rows |        42,041 |                               76.89% |                   0.2669% |

Interpretation:

These are not valid market observations for daily trading. Return missingness is expected because there is no normal trading record from which to compute return.

---

#### Pattern 2: Suspended / halted rows

Characteristics:

* `tradingstatusflg = S` or `H`
* Return variables are missing
* Price, volume, and OHLC variables may also be missing or unusable
* These rows represent securities that were not normally tradable on that day

Prevalence:

| Pattern              | Missing count | Percentage among missing return rows | Percentage among all rows |
| -------------------- | ------------: | -----------------------------------: | ------------------------: |
| Suspended rows (`S`) |         4,359 |                                7.97% |                   0.0277% |
| Halted rows (`H`)    |            75 |                                0.14% |                   0.0005% |

Interpretation:

These rows should not enter the tradable universe. Missing returns are a consequence of the stock not having a normal tradable market observation.

---

#### Pattern 3: Active rows with missing returns

Characteristics:

* `tradingstatusflg = A`
* `dlyret`, `dlyretx`, and `dlyreti` are missing
* Some rows still have valid `dlyprc`, `prc`, `dlyvol`, `dlycap`, and bid/ask information
* `dlyretmissflg` and `dlyretdurflg` indicate that the return is not computable or not reported even though the row may be active

Prevalence:

| Pattern                          | Missing count | Percentage among missing return rows | Percentage among all rows |
| -------------------------------- | ------------: | -----------------------------------: | ------------------------: |
| Active rows with missing returns |         8,049 |                               14.72% |                   0.0511% |

Interpretation:

This is the most important pattern to inspect carefully. These rows may still be tradable, but the return field is missing. This often happens when the database cannot compute a valid daily return, for example because of missing previous comparable price, new listing behavior, stale-price issues, or special return-duration flags.

These rows are different from inactive rows: they may have valid price/volume information, but return-based features and labels cannot be directly computed from `dlyret`.

---

#### Pattern 4: Delisting-related rows

Characteristics:

* `tradingstatusflg = D`
* May correspond to terminal or delisting-related observations
* Return variables are missing in a small number of such rows
* Delisting-related variables may contain relevant terminal outcome information

Prevalence:

| Pattern                | Missing count | Percentage among missing return rows | Percentage among all rows |
| ---------------------- | ------------: | -----------------------------------: | ------------------------: |
| Delisting-related rows |           129 |                                0.24% |                   0.0008% |

Interpretation:

These are not normal new trading candidates, but they may matter for backtest outcome handling if a position was held before delisting.

---

### Treatment plan

#### Pattern 1 treatment: inactive / non-trading rows

Exclude from the tradable universe.

Rationale:

* These rows lack normal price, volume, and return information
* They are not realistic trading candidates
* Missing return is expected and should not be imputed

They should remain in `daily_core`, but should not enter `model_panel`.

---

#### Pattern 2 treatment: suspended / halted rows

Exclude from the tradable universe.

Rationale:

* Suspended or halted securities are not normally tradable
* Return missingness reflects lack of normal market activity
* Exclusion uses contemporaneous information and does not introduce look-ahead bias

---

#### Pattern 3 treatment: active rows with missing returns

Do not impute `dlyret`, `dlyretx`, or `dlyreti`.

Possible handling:

* Keep these rows in `daily_core`
* Add a return-missing indicator if needed
* Do not use them when a non-missing current return is required for feature construction
* Do not use them as realized-return labels
* For the first clean model panel, exclude them unless explicitly want LightGBM to handle missing return-based features

Rationale:

* Missing return is not the same as zero return
* Filling missing returns with zero would create artificial reversal/momentum signals
* Return-based rolling features should be computed only from valid historical return observations

For the first version of the project, a conservative rule is:

```text
Require dlyret to be non-missing for rows entering the model panel.
Require enough non-missing historical returns before using a stock in training or backtesting.
```

A later, more flexible version can keep active rows with missing current return as long as other tradability variables are valid, but all return-based features should remain missing or be computed from available historical observations only.

---

#### Pattern 4 treatment: delisting-related rows

Exclude from the training / prediction universe as new candidates.

Retain for backtest and realized-return handling.

Rationale:

* These rows are not normal buyable observations
* But if a stock was held before delisting, terminal/delisting outcomes must be incorporated
* Dropping delisting outcomes entirely would make the backtest too optimistic

---

### Derived feature recommendation

For return-based features, use `dlyret` carefully:

* Use `dlyret` for total-return momentum and realized PnL
* Use `dlyretx` for price-only momentum if you want to remove distribution effects
* Use `dlyreti` to identify income/distribution-driven returns if needed

Recommended first-version features:

```text
ret_1d = dlyret
ret_5d = rolling compounded or summed dlyret over past 5 valid trading days
ret_21d = rolling compounded or summed dlyret over past 21 valid trading days
vol_21d = rolling standard deviation of dlyret over past 21 valid trading days
```

Important:

```text
Do not fill missing return with 0.
Do not let future label availability define the tradable universe.
Compute rolling features using only information available up to day t.
```

---

### Final decision

```text
daily_core:
  Keep all rows.
  Do not impute dlyret, dlyretx, or dlyreti.
  Preserve return missingness and return-missing flags.

model_panel / universe:
  Exclude inactive, suspended, halted, and delisting rows as new candidates.
  For the first clean version, exclude rows with missing dlyret.
  Require enough non-missing historical return observations before using a stock.

features:
  Create return_missing_flag if useful.
  Compute return-based features only from valid historical returns.
  Do not replace missing returns with zero.

labels / backtest:
  Do not use missing future returns as realized labels.
  Retain delisting-related information for terminal realized-return handling.
```

Overall conclusion:

```text
dlyret, dlyretx, and dlyreti missingness is mostly not a serious random data-quality problem.
It is primarily concentrated in non-trading, suspended, halted, and inactive rows.
The main subtle case is active rows with missing returns; these should not be imputed, and they should be handled conservatively in the first model panel.
```

## Variables investigated

We investigate a group of security/share metadata variables that have exactly the same missing count:

* `cusip`: CUSIP identifier
* `cusip9`: 9-character CUSIP identifier
* `conditionaltype`: conditional type
* `securitytype`: security type
* `sharetype`: share type
* `delstatustype`: delisting completion status type
* `delreasontype`: delisting reason type
* `delpaymenttype`: delisting payment summary type
* `shrstartdt`: share information start date
* `shrenddt`: share information end date
* `shrout`: shares outstanding
* `shrsource`: share change source type
* `shradrflg`: share ADR flag

These variables are mostly identifier, security classification, delisting-status, and share-information variables. The variable dictionary defines `shrout` as shares outstanding, `shrstartdt`/`shrenddt` as share information date fields, and `shradrflg` as the share ADR flag. 

---

### Missing summary

All variables in this group have the same missing count and missing percentage:

| Variable group                                        | Total rows | Missing count | Missing percentage | Abnormal count |
| ----------------------------------------------------- | ---------: | ------------: | -----------------: | -------------: |
| CUSIP / security type / share type / share info group | 15,749,137 |         4,666 |            0.0296% |              0 |

Key observations:

* Missingness is extremely low: only **0.0296%** of all observations.
* The exact same 4,666 rows are missing across all variables in this group.
* This strongly suggests a **common structural missingness pattern**, not independent random missingness.
* The missing-pattern report for `shrout` shows that all 4,666 missing rows are in groups with:

  * `primaryexch = X`
  * `tradingstatusflg = D`
  * `securitytype` missing
  * `sharetype` missing
  * missing rate = 100% within those grouped rows
* These rows are distributed across years 2019–2025.

Interpretation:

This is not a normal missing-data issue for active securities. It is a small cluster of delisting/terminal observations where security/share metadata fields are unavailable or no longer applicable.

---

### Missing patterns

#### Pattern 1: Delisting / terminal event rows

Characteristics:

* `tradingstatusflg = D`
* `primaryexch = X`
* `dlydelflg = Y`
* Security/share metadata fields are missing:

  * `cusip`
  * `cusip9`
  * `securitytype`
  * `sharetype`
  * `shrout`
  * `shradrflg`
* The security name often appears as a “last known” security name
* Some daily return or delisting-related fields may still be available
* These rows are not normal active trading observations

Prevalence:

| Pattern                         | Missing count | Percentage among this missing group | Percentage among all rows |
| ------------------------------- | ------------: | ----------------------------------: | ------------------------: |
| Delisting / terminal event rows |         4,666 |                                100% |                   0.0296% |

Interpretation:

The missingness appears to come from delisting-related terminal rows. These are not rows where a normal active stock randomly lost `shrout` or `securitytype`. Instead, they are post-active or terminal observations where the daily file records delisting/outcome information but no longer has the usual share/security classification fields.

---

### Treatment plan

#### Pattern 1 treatment: delisting / terminal event rows

Do **not** impute these metadata fields in `daily_core`.

Recommended treatment:

* Keep all rows in `daily_core`
* Preserve missing values in:

  * `cusip`
  * `securitytype`
  * `sharetype`
  * `shrout`
  * `shradrflg`
* Do not forward-fill `shrout` or security classification fields at the `daily_core` stage
* Do not use these rows as new prediction/trading candidates
* Retain these rows for delisting and realized-return handling

Rationale:

* These rows are not normal stock-date observations for universe construction.
* Filling `shrout` or `securitytype` on terminal rows may create fake precision.
* These rows may still matter for backtest PnL if a position was held before delisting.

---

#### Universe construction treatment

Exclude these rows from the training/prediction universe.

Reason:

* `tradingstatusflg = D` means the stock is not a new tradable candidate.
* `primaryexch = X` suggests the row is outside the normal active exchange universe.
* Missing `shrout`, `securitytype`, and `sharetype` prevents reliable market-cap and common-stock filtering.
* These rows are terminal outcome rows, not investable day-*t* candidates.

For model-panel construction, this group will naturally be excluded by rules such as:

```text
tradingstatusflg = A
prc is not missing
dlyvol is not missing
shrout is not missing
securitytype / sharetype satisfy common-stock filters
```

---

#### Backtest / label treatment

Retain these rows for realized-return handling.

Reason:

* Delisting-related rows can contain terminal outcome information.
* If a stock was selected before delisting, the backtest must account for the realized outcome.
* Removing these rows entirely could create survivorship bias and make strategy performance look too optimistic.

Important distinction:

```text
Do not use these rows as input candidates.

But do preserve them as possible future outcomes of positions formed earlier.
```

---

### Final decision

```text
daily_core:
  Keep all rows.
  Do not impute CUSIP, security type, share type, share dates, or shares outstanding.
  Preserve missingness as part of the raw typed archive.

model_panel / universe:
  Exclude these 4,666 rows as new trading candidates.
  Require valid share/security metadata for normal universe construction.

features:
  Do not use missing metadata fields directly.
  Use valid shrout / dlycap / security type only for active tradable rows.

backtest:
  Retain these delisting / terminal rows for realized-return and delisting handling.
```

Overall conclusion:

```text
This missingness group is not a serious random data-quality issue.
It is a small, structurally coherent cluster of delisting or terminal observations.
The correct treatment is to exclude them from the tradable universe but retain them for backtest outcome handling.
```


## Variables investigated

We investigate three market microstructure / trading-activity variables:

* `dlynumtrd`: daily number of trades
* `dlymmcnt`: daily market maker count
* `exchangetier`: exchange tier

These variables are potentially useful for liquidity and microstructure analysis, but they are **not core variables** like price, return, volume, or market capitalization.

---

### Missing summary

The three variables have very high and very similar missing rates.

| Variable       | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| -------------- | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `dlynumtrd`    | 15,749,137 |     9,039,956 |             57.40% |              0 |               0.00% |
| `exchangetier` | 15,749,137 |     9,035,459 |             57.37% |              0 |               0.00% |
| `dlymmcnt`     | 15,749,137 |     9,035,459 |             57.37% |              0 |               0.00% |

Key observations:

* Missingness is very high: around **57%**
* `exchangetier` and `dlymmcnt` have exactly the same missing count
* `dlynumtrd` has slightly more missing rows, but the difference is small
* Missingness is **not mostly caused by bad rows**
* Many rows with missing `dlynumtrd`, `dlymmcnt`, and `exchangetier` still have valid:

  * `prc`
  * `dlyvol`
  * `dlycap`
  * `dlyret`
  * ticker / trading symbol

Interpretation:

This is different from missing `prc`, `dlyvol`, or `dlyret`. These variables are missing for many otherwise valid active trading observations, so they should be treated as **optional microstructure variables**, not mandatory universe variables.

---

### Missing patterns

#### Pattern 1: Active observations with valid core trading data but missing microstructure fields

Characteristics:

* `tradingstatusflg = A`
* Price, volume, market cap, and return are usually available
* `dlynumtrd`, `dlymmcnt`, and `exchangetier` are missing
* Common among:

  * `primaryexch = N`
  * `primaryexch = R`
  * `primaryexch = B`
  * `securitytype = FUND`
  * `securitytype = EQTY`

Prevalence based on the grouped `dlynumtrd` report:

| Pattern                                        | Missing count | Percentage among missing `dlynumtrd` rows | Percentage among all rows |
| ---------------------------------------------- | ------------: | ----------------------------------------: | ------------------------: |
| Active rows with missing microstructure fields |    ~8,975,809 |                                   ~99.29% |                   ~56.99% |

Interpretation:

This is the dominant pattern. Missingness here does **not** mean the stock is non-tradable. It means these particular microstructure fields are not broadly available for many otherwise normal observations.

This is the most important conclusion: **do not exclude active stocks from the universe just because these variables are missing.**

---

#### Pattern 2: Fund / ETF-heavy missingness

Characteristics:

* `securitytype = FUND` is a major source of missingness
* Many missing rows are associated with ETF/fund-like securities
* These rows can still have valid prices, returns, and volumes

Approximate prevalence from the grouped report:

| Security type | Missing count | Percentage among missing `dlynumtrd` rows | Percentage among all rows |
| ------------- | ------------: | ----------------------------------------: | ------------------------: |
| `FUND`        |    ~5,022,848 |                                   ~55.56% |                   ~31.89% |
| `EQTY`        |    ~3,995,163 |                                   ~44.19% |                   ~25.37% |

Interpretation:

A large portion of the missingness comes from funds/ETFs. For a **common-stock return-prediction project**, many of these securities may later be excluded by the common-stock universe filter anyway. But the same missingness also appears among equities, so it is not only an ETF/fund issue.

---

#### Pattern 3: Exchange/reporting-coverage pattern

Characteristics:

* Missingness is concentrated by exchange/source groups
* The largest grouped missing counts come from `primaryexch = N`, `R`, and `B`
* Missing rate is often 100% within some exchange/security-type/year groups

Approximate prevalence from the grouped report:

| Primary exchange | Missing count | Percentage among missing `dlynumtrd` rows | Percentage among all rows |
| ---------------- | ------------: | ----------------------------------------: | ------------------------: |
| `N`              |    ~4,308,283 |                                   ~47.66% |                   ~27.36% |
| `R`              |    ~3,188,591 |                                   ~35.27% |                   ~20.25% |
| `B`              |    ~1,022,471 |                                   ~11.31% |                    ~6.49% |
| `A`              |      ~456,464 |                                    ~5.05% |                    ~2.90% |
| `X`              |       ~43,721 |                                    ~0.48% |                    ~0.28% |
| `Q`              |        ~1,002 |                                    ~0.01% |                   ~0.006% |

Interpretation:

This looks like a **data-coverage issue by exchange/security-type/year**, not a random input mistake. These variables are not consistently populated across all parts of the CRSP daily universe.

---

#### Pattern 4: Non-tradable / suspended / delisting rows

Characteristics:

* `tradingstatusflg = X`, `S`, or `D`
* Some missing rows are inactive, suspended, or delisting-related
* These rows often also lack core trading variables

Approximate prevalence from the grouped `dlynumtrd` report:

| Status | Missing count | Percentage among missing `dlynumtrd` rows | Percentage among all rows |
| ------ | ------------: | ----------------------------------------: | ------------------------: |
| `X`    |       ~41,200 |                                    ~0.46% |                    ~0.26% |
| `D`    |        ~2,521 |                                    ~0.03% |                   ~0.016% |
| `S`    |        ~1,002 |                                    ~0.01% |                   ~0.006% |

Interpretation:

These rows follow the same logic as previous missingness diagnostics: they should not enter the tradable universe as new candidates. However, they are a very small part of the missingness for these variables.

---

### Treatment plan

#### Pattern 1 treatment: active observations with missing microstructure fields

Keep these rows in the tradable universe if they pass the usual core filters.

Do **not** require `dlynumtrd`, `dlymmcnt`, or `exchangetier` to be non-missing.

Rationale:

* These rows often have valid price, volume, market cap, and return
* Excluding them would remove a huge fraction of otherwise valid observations
* Missingness reflects limited coverage of these fields, not necessarily poor data quality

Recommended handling:

* Preserve missing values
* Add missingness indicators if these variables are used
* Let LightGBM handle missing values
* Do not impute with zero

Important:

Missing `dlynumtrd` does **not** mean zero trades. It means the number of trades is not reported.

---

#### Pattern 2 treatment: fund / ETF-heavy missingness

For a common-stock project, many fund/ETF rows will likely be excluded later using security-type and share-type filters.

However, do not use missing `dlynumtrd`, `dlymmcnt`, or `exchangetier` as the reason for exclusion.

Rationale:

* Fund/ETF exclusion should be based on `securitytype`, `securitysubtype`, `sharetype`, or similar classification fields
* Microstructure missingness should not define the stock universe

---

#### Pattern 3 treatment: exchange/reporting-coverage pattern

Treat these variables as optional features.

Recommended handling:

* Do not use `exchangetier` as a mandatory universe filter at first
* Do not require `dlymmcnt` or `dlynumtrd`
* If used as features, include missingness indicators
* Consider excluding these variables from the first baseline model to avoid coverage-driven artifacts

Rationale:

* Missingness appears strongly tied to reporting coverage
* These variables may accidentally encode exchange/security-type coverage rather than true alpha
* For the first clean baseline, core price/return/volume features are safer

---

#### Pattern 4 treatment: non-tradable / suspended / delisting rows

Exclude from the training / prediction universe as new candidates.

Retain delisting-related rows for backtest outcome handling.

Rationale:

* These rows are not normal buyable observations
* This treatment is consistent with previous diagnostics for missing `prc`, OHLC, bid/ask, volume, and return fields

---

### Feature recommendation

For the first version of the model, I would **not rely on these variables**.

Recommended baseline:

```text
Do not require:
  dlynumtrd
  dlymmcnt
  exchangetier

Optional features:
  log_num_trades = log1p(dlynumtrd)
  log_market_maker_count = log1p(dlymmcnt)
  num_trades_missing_flag
  market_maker_count_missing_flag
  exchange_tier_missing_flag
```

However, these should be treated as **secondary features**, not core features.

---

### Final decision

```text
daily_core:
  Keep all rows.
  Do not impute dlynumtrd, dlymmcnt, or exchangetier.
  Preserve raw missingness.

model_panel / universe:
  Do not require these variables to be non-missing.
  Do not exclude active rows only because these fields are missing.
  Exclude non-tradable, suspended, and delisting rows using standard status filters.

features:
  Treat these as optional microstructure features.
  Add missingness indicators if used.
  Prefer not to include them in the first baseline model.
  Never interpret missing dlynumtrd as zero trades.

backtest:
  These variables are not necessary for core PnL calculation.
  Continue retaining delisting-related rows for realized-return handling.
```

Overall conclusion:

```text
dlynumtrd, dlymmcnt, and exchangetier have high missingness, but this does not indicate broad data failure.
Their missingness mainly reflects limited field coverage across exchange/security-type groups.
They should not be used as mandatory universe filters.
For the first return-prediction project, treat them as optional secondary features or exclude them from the baseline model.
```


## Variables investigated

We investigate:

* `delactiontype`: delisting corporate action type

This variable is a **delisting-event metadata field**, not a core daily trading variable like price, volume, return, or market capitalization.

---

### Missing summary

`delactiontype` has high missingness:

| Variable        | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| --------------- | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `delactiontype` | 15,749,137 |    12,705,024 |             80.67% |              0 |               0.00% |

Key observations:

* Missingness is very high, around **80.7%**
* However, this is expected for an event-style variable
* Most stock-date rows are normal trading days with no delisting corporate action
* Random missing examples usually still have valid:

  * `prc`
  * `dlyret`
  * `dlyvol`
  * `ticker`
  * `tradingstatusflg = A`
  * `dlydelflg = N`

Interpretation:

High missingness in `delactiontype` does **not** indicate broad data failure. It mostly means that no delisting corporate action is applicable on that stock-date.

---

### Missing patterns

#### Pattern 1: Normal active rows with no delisting action

Characteristics:

* `tradingstatusflg = A`
* `dlydelflg = N`
* `securityactiveflg = Y`
* `prc`, `dlyret`, and `dlyvol` are usually available
* `delstatustype` often has values such as `UNAV`
* `delreasontype` often indicates no active delisting reason, e.g. `NACT`
* `delpaymenttype` often has values such as `UNAV`
* `delactiontype` is missing

Prevalence:

* This is the dominant missingness pattern
* The grouped report shows that the overwhelming majority of missing `delactiontype` rows are active rows
* Missingness is widespread across normal exchange/security groups, including both `EQTY` and `FUND`

Interpretation:

This pattern simply means that the row is a normal active trading observation with no delisting corporate action. It should not be treated as abnormal.

---

#### Pattern 2: Inactive / non-normal status rows

Characteristics:

* `tradingstatusflg = X` or other non-active status
* `primaryexch = X`
* `delactiontype` is missing
* Some core trading fields may also be missing or non-standard

Prevalence:

* Small relative to Pattern 1
* Not the main reason for `delactiontype` missingness

Interpretation:

These rows are not normal active trading observations. Missing `delactiontype` is expected, but the main reason to exclude these rows from the tradable universe is their non-active trading status, not the missing `delactiontype`.

---

#### Pattern 3: Delisting / terminal rows with missing `delactiontype`

Characteristics:

* `tradingstatusflg = D`
* `dlydelflg = Y`
* `securityactiveflg = N`
* `delactiontype` may still be missing
* Other delisting-related fields or distribution fields may contain terminal-event information

Prevalence:

* Rare compared with Pattern 1

Interpretation:

A missing `delactiontype` does not necessarily mean there is no delisting-related event. Some terminal rows may still have missing `delactiontype`, so delisting detection should not rely on this variable alone.

Use a broader set of fields:

```text
tradingstatusflg
dlydelflg
securityactiveflg
delstatustype
delreasontype
delpaymenttype
dlyret
distribution-related fields
```

---

### Treatment plan

#### Pattern 1 treatment: normal active rows with no delisting action

Keep these rows in the tradable universe if they pass other filters.

Do not exclude rows just because `delactiontype` is missing.

Rationale:

* These are normal active trading rows
* Missing `delactiontype` usually means no delisting action applies
* Excluding them would remove a large fraction of valid observations

Recommended handling:

* Keep `delactiontype` as missing in `daily_core`
* In `model_panel`, optionally create an event indicator:

```text
has_delactiontype = 1 if delactiontype is non-missing, else 0
```

For the first baseline model, `delactiontype` does not need to be used as a feature.

---

#### Pattern 2 treatment: inactive / non-normal status rows

Exclude from the tradable universe using status filters.

Rationale:

* These rows are not valid trading candidates
* The exclusion should be based on trading status and valid price/volume/return fields, not on `delactiontype`

Example logic:

```text
Exclude if tradingstatusflg is not active
Exclude if prc is missing
Exclude if dlyvol is missing
```

---

#### Pattern 3 treatment: delisting / terminal rows

Do not use these rows as new training or prediction candidates.

Retain them for backtest and realized-return handling.

Rationale:

* Terminal/delisting rows are not normal buyable observations
* However, if a stock was held before delisting, the terminal realized return must be included
* Missing `delactiontype` should not cause us to ignore a delisting event

Important implication:

```text
delactiontype missing != no delisting event
```

For delisting logic, rely on multiple fields rather than `delactiontype` alone.

---

### Feature recommendation

For the first version of the project:

```text
Do not use delactiontype as a core feature.
Do not require delactiontype to be non-missing.
Do not impute missing delactiontype in daily_core.
```

If used later, treat it as an event/categorical feature:

```text
has_delactiontype
delactiontype category if non-missing
delisting-related status indicators
```

But avoid making it a major alpha input unless the strategy explicitly models delisting/event risk.

---

### Final decision

```text
daily_core:
  Keep delactiontype as-is.
  Do not impute missing values.
  Preserve all delisting-related metadata.

model_panel / universe:
  Do not exclude active rows because delactiontype is missing.
  Exclude non-tradable rows using trading status, price, volume, and liquidity filters.
  Exclude delisting/terminal rows as new trading candidates.

features:
  Treat delactiontype as optional event metadata.
  Optionally create has_delactiontype.
  Do not use it in the first baseline model unless modeling event/delisting risk.

backtest:
  Retain delisting-related rows and fields.
  Do not rely only on delactiontype to identify terminal outcomes.
```

Overall conclusion:

```text
delactiontype missingness is expected and mostly reflects normal days without delisting corporate actions.
It is not a data-quality problem and should not be used as a universe exclusion rule.
The variable is useful mainly for event/backtest diagnostics, not for the first baseline alpha model.
```


## Variables investigated

We investigate:

* `shareclass`: share class

This variable is a security metadata field. It is different from core trading variables such as `prc`, `dlyret`, `dlyvol`, or `dlycap`.

---

### Missing summary

`shareclass` has very high missingness.

| Variable     | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| ------------ | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `shareclass` | 15,749,137 |    14,096,039 |             89.50% |              0 |               0.00% |

Key observations:

* Missingness is extremely high, about **89.5%**
* There are no abnormal values under the current diagnostic rule
* Random missing examples still have valid:

  * `prc`
  * `dlyret`
  * `dlyvol`
  * `dlycap`
  * `ticker`
  * `cusip`
  * `shrout`
  * `securitytype`
  * `sharetype`
* Many missing rows are normal active observations with `tradingstatusflg = A`

Interpretation:

High missingness in `shareclass` is **not a trading-data failure**. It is mostly an optional metadata field that is not populated for most securities.

---

### Missing patterns

#### Pattern 1: Normal active securities with missing `shareclass`

Characteristics:

* `tradingstatusflg = A`
* Core trading data are available
* `prc`, `dlyret`, `dlyvol`, and `dlycap` are usually valid
* `securitytype`, `securitysubtype`, `sharetype`, and `shrout` are usually available
* `shareclass` is missing

Prevalence:

* This is the dominant pattern
* The grouped report shows that the overwhelming majority of missing `shareclass` rows are active observations

Interpretation:

Missing `shareclass` does not imply that the row is invalid or non-tradable. It usually means that the security does not have a separately populated share-class label in this field.

---

#### Pattern 2: Equity and fund observations with missing `shareclass`

Characteristics:

* Missingness appears in both:

  * `securitytype = EQTY`
  * `securitytype = FUND`
* Many ETF/fund observations have missing `shareclass`
* Many common-equity observations also have missing `shareclass`

Approximate distribution from the grouped report:

| Security type | Missing count in grouped report | Interpretation                |
| ------------- | ------------------------------: | ----------------------------- |
| `EQTY`        |   large majority of equity rows | Common among regular equities |
| `FUND`        |   large number of fund/ETF rows | Common among ETFs/funds       |

Interpretation:

Missing `shareclass` is not specific to one security category. It appears broadly across both stocks and funds.

For a common-stock return-prediction project, funds/ETFs may be excluded later, but that exclusion should be based on `securitytype`, `securitysubtype`, or `sharetype`, not on missing `shareclass`.

---

#### Pattern 3: Exchange / coverage-driven missingness

Characteristics:

* Missingness is spread across major primary exchanges
* Large missing counts appear under exchanges such as:

  * `Q`
  * `N`
  * `R`
  * `B`
  * `A`
* Many exchange-year-security groups have high missing rates

Interpretation:

This looks like a coverage / metadata-population issue rather than an input error. `shareclass` is simply not consistently populated across the CRSP daily universe.

---

#### Pattern 4: Non-active / terminal observations

Characteristics:

* A small number of missing `shareclass` rows are associated with non-active status groups such as `primaryexch = X`
* These rows may overlap with inactive, delisting, or terminal observations

Interpretation:

These rows follow the same logic as previous diagnostics: they should not be used as new trading candidates. However, they are not the main reason `shareclass` is missing.

---

### Treatment plan

#### Pattern 1 treatment: normal active securities with missing `shareclass`

Keep these rows in the tradable universe if they satisfy other filters.

Do not require `shareclass` to be non-missing.

Rationale:

* These rows often have valid price, return, volume, market cap, and share information
* Excluding them would remove most of the dataset
* Missing `shareclass` is not evidence of bad trading data

---

#### Pattern 2 treatment: equity and fund observations

For common-stock universe construction, do not use `shareclass` as the main filter.

Use more reliable classification variables instead:

* `securitytype`
* `securitysubtype`
* `sharetype`
* `shradrflg`
* `usincflg`
* `primaryexch`

Rationale:

* `shareclass` is too sparse to define the universe
* Fund/ETF exclusion should be based on actual security classification, not missingness of `shareclass`

---

#### Pattern 3 treatment: exchange / coverage-driven missingness

Treat `shareclass` as optional metadata.

Recommended handling:

* Preserve missing values
* Do not impute missing `shareclass`
* Do not use it as a mandatory feature
* If used later, include a `shareclass_missing_flag` or `has_shareclass` indicator

Rationale:

* Missingness likely reflects metadata coverage, not economic information
* Using it naively may introduce coverage artifacts into the model

---

#### Pattern 4 treatment: non-active / terminal observations

Exclude these rows from the training / prediction universe using standard status filters.

Retain terminal or delisting rows for backtest outcome handling when relevant.

Rationale:

* The exclusion should be based on trading status and valid core trading variables
* Missing `shareclass` itself is not the reason for exclusion

---

### Feature recommendation

For the first version of the model:

```text
Do not use shareclass as a core feature.
Do not require shareclass to be non-missing.
Do not impute missing shareclass.
```

If used later, treat it as optional metadata:

```text
has_shareclass = 1 if shareclass is non-missing, else 0
shareclass_category = shareclass value when available
```

But for the baseline model, stronger and cleaner universe variables are:

```text
securitytype
securitysubtype
sharetype
shradrflg
usincflg
primaryexch
```

---

### Final decision

```text
daily_core:
  Keep shareclass as-is.
  Do not impute missing values.
  Preserve raw metadata missingness.

model_panel / universe:
  Do not exclude rows because shareclass is missing.
  Do not require shareclass for common-stock filtering.
  Use securitytype, securitysubtype, sharetype, shradrflg, usincflg, and primaryexch instead.

features:
  Do not include shareclass in the first baseline model.
  Optionally create has_shareclass later as a metadata indicator.

backtest:
  shareclass is not needed for core PnL calculation.
  Continue handling inactive/delisting rows through trading-status and delisting logic.
```

Overall conclusion:

```text
shareclass missingness is very high but not a serious data-quality problem.
It mostly reflects sparse metadata coverage.
The variable should be preserved in daily_core but should not be used as a mandatory universe filter or core baseline feature.
```



## Variables investigated

We investigate a group of distribution-event variables:

* `distype`: distribution type
* `dispaydt`: distribution payment date
* `disexdt`: ex-distribution date
* `disseqnbr`: distribution sequence number
* `disordinaryflg`: ordinary dividend flag
* `dispaymenttype`: distribution payment method type
* `disdetailtype`: distribution detail type
* `distaxtype`: distribution tax status type
* `disrecorddt`: distribution record date
* `dispermno`: PERMNO of security received
* `dispermco`: PERMCO of issuer providing payment

These variables are not core daily trading variables. They are **distribution-event metadata fields**, mainly populated when a dividend, split, spin-off, merger payment, stock distribution, or other corporate distribution event occurs.

---

### Missing summary

These variables have extremely high missingness, around **98.76%**.

| Variable         | Total rows | Missing count | Missing percentage |
| ---------------- | ---------: | ------------: | -----------------: |
| `distype`        | 15,749,137 |    15,553,797 |           98.7597% |
| `dispaydt`       | 15,749,137 |    15,553,779 |           98.7596% |
| `disexdt`        | 15,749,137 |    15,553,778 |           98.7596% |
| `disseqnbr`      | 15,749,137 |    15,553,778 |           98.7596% |
| `disordinaryflg` | 15,749,137 |    15,553,778 |           98.7596% |
| `dispaymenttype` | 15,749,137 |    15,553,778 |           98.7596% |
| `disdetailtype`  | 15,749,137 |    15,553,778 |           98.7596% |
| `distaxtype`     | 15,749,137 |    15,553,778 |           98.7596% |
| `disrecorddt`    | 15,749,137 |    15,553,778 |           98.7596% |
| `dispermno`      | 15,749,137 |    15,553,778 |           98.7596% |
| `dispermco`      | 15,749,137 |    15,553,778 |           98.7596% |

Key observations:

* Missingness is extremely high.
* Missing counts are almost identical across the whole group.
* This strongly suggests block-level event missingness.
* There are no abnormal values under the current diagnostic rules.
* The corresponding non-missing observations are only about **195k rows**, roughly **1.24%** of the full daily panel.

Interpretation:

This is expected. Most stock-date observations do **not** have a distribution event. Therefore, missing values in these variables usually mean:

```text
No distribution event is recorded for this stock on this date.
```

This is not a data-quality problem.

---

### Missing patterns

#### Pattern 1: Normal trading days with no distribution event

Characteristics:

* `tradingstatusflg = A`
* Core trading variables are usually valid:

  * `prc`
  * `dlyret`
  * `dlyvol`
  * `dlycap`
* Distribution-event variables are missing:

  * `disexdt`
  * `distype`
  * `dispaymenttype`
  * `disrecorddt`
  * `dispaydt`
  * etc.

Prevalence:

* This is the dominant pattern.
* It accounts for almost all missingness in this group.

Interpretation:

These are normal daily observations without dividends, splits, stock distributions, or other distribution events. The missing values should be interpreted structurally, not as bad data.

---

#### Pattern 2: Active rows with distribution-event variables populated

Characteristics:

* Core trading variables are still valid.
* Distribution-event fields become non-missing.
* These rows may correspond to:

  * ordinary cash dividends
  * special dividends
  * stock splits
  * stock distributions
  * merger or spin-off distributions
  * other corporate-action-related payments

Prevalence:

* Around **1.24%** of all rows have non-missing values in the main distribution-event block.

Interpretation:

These rows are economically meaningful. They may explain unusual return behavior because distributions directly affect total return and price adjustment.

Important distinction:

```text
dlyret  = total return, including distribution effects
dlyretx = price return, excluding distributions
dlyreti = income return component
```

So distribution fields are relevant when interpreting the difference between total return and price-only return.

---

#### Pattern 3: Distribution-related terminal / delisting observations

Characteristics:

* Some distribution fields may be populated near delisting, merger, liquidation, or other terminal events.
* Related variables may include:

  * `dlydelflg`
  * `delactiontype`
  * `delreasontype`
  * `dlynonorddivamt`
  * `disdivamt`
  * `disfacpr`
  * `disfacshr`

Interpretation:

These rows may not be normal tradable observations, but they can matter for realized-return and backtest handling.

For example, a security may stop trading and shareholders may receive a cash or stock distribution. Such outcomes should not be ignored if a strategy held the security before the event.

---

#### Pattern 4: Security received fields mostly missing by design

Variables such as:

* `dispermno`
* `dispermco`

are usually missing because most distributions do not involve receiving another CRSP-tracked security.

Interpretation:

Missing values here do not mean the distribution event is invalid. They usually mean the distribution does not involve a received security, or that the received security is not represented by a separate PERMNO/PERMCO in this field.

---

### Treatment plan

#### Pattern 1 treatment: normal days with no distribution event

Do not impute distribution variables.

Recommended treatment:

* Keep missing values as missing in `daily_core`.
* Do not treat missing distribution fields as abnormal.
* Do not exclude rows because these variables are missing.
* In later feature construction, create simple event indicators if needed.

Useful derived indicators:

```text
has_distribution_event = 1 if disexdt is non-missing, else 0
has_ordinary_distribution = 1 if disordinaryflg indicates ordinary distribution
```

Rationale:

* Missingness means “no event,” not “unknown value.”
* Imputation is unnecessary and could create misleading corporate-action signals.

---

#### Pattern 2 treatment: active rows with distribution events

Keep these rows in the data.

For the first baseline model:

* Do not use detailed distribution variables as core alpha features.
* Preserve them for diagnostics and later event-feature construction.
* Use `dlyret` for total-return modeling.
* Use `dlyretx` if constructing price-only momentum or price-only reversal features.

Potential later features:

```text
has_distribution_event
has_cash_distribution
has_stock_distribution
distribution_amount_scaled_by_price
distribution_factor_change
ordinary_distribution_flag
special_distribution_flag
```

Rationale:

* Distribution events can explain return jumps and price adjustments.
* But using them naively may introduce timing/leakage issues unless we are clear when the event information becomes known.

---

#### Pattern 3 treatment: terminal / delisting-related distribution rows

Do not use terminal distribution rows as new prediction candidates.

Retain them for backtest and realized-return handling.

Rationale:

* These rows may encode the realized outcome of a stock that was held before a terminal event.
* Dropping them can create survivorship or delisting bias.
* They are especially important for realistic PnL calculation.

---

#### Pattern 4 treatment: missing received-security identifiers

Do not impute `dispermno` or `dispermco`.

Rationale:

* These fields are only relevant for specific distribution types.
* Most distributions do not require a received-security identifier.
* Missingness is structurally expected.

---

### Feature recommendation

For the first model version:

```text
Do not include detailed distribution variables as core features.
Do not require them to be non-missing.
Do not impute them.
```

Use the return variables instead:

```text
dlyret  for total-return features and PnL
dlyretx for price-only return features
dlyreti for income/distribution component diagnostics
```

For a later enhanced model, distribution features can be added carefully:

```text
has_distribution_event
has_ordinary_distribution
has_special_distribution
distribution_amount / prc
split_or_adjustment_flag
```

But these should be added only after confirming timing assumptions to avoid look-ahead bias.

---

### Final decision

```text
daily_core:
  Keep all distribution-event variables as-is.
  Do not impute missing values.
  Interpret missingness as no recorded distribution event in most cases.

model_panel / universe:
  Do not exclude rows because distribution variables are missing.
  Do not require distribution variables for common-stock universe construction.
  Exclude non-tradable or terminal rows using trading-status and price/volume filters, not distribution missingness.

features:
  Do not use detailed distribution fields in the first baseline model.
  Optionally create event indicators later.
  Be careful about event timing to avoid leakage.

backtest:
  Retain distribution-related rows.
  Use total-return variables and delisting/distribution information when computing realized outcomes.
```

Overall conclusion:

```text
The high missingness of distribution variables is expected and structurally meaningful.
These fields are event-specific, so missingness usually means no distribution event occurred on that stock-date.
They should be preserved in daily_core, ignored as mandatory universe filters, and used later only for corporate-action diagnostics or carefully timed event features.
```


## Variables investigated

We investigate:

* `disdivamt`: dividend amount

This is a **distribution-event amount variable**, not a core daily trading variable. It is populated mainly when a dividend or distribution event is recorded.

---

### Missing summary

`disdivamt` has extremely high missingness.

| Variable    | Total rows | Missing count | Missing percentage | Abnormal count | Abnormal percentage |
| ----------- | ---------: | ------------: | -----------------: | -------------: | ------------------: |
| `disdivamt` | 15,749,137 |    15,556,843 |           98.7790% |              1 |           0.000006% |

Key observations:

* Missingness is about **98.78%**, which is expected for an event-specific variable.
* Only around **1.22%** of stock-date rows have a non-missing `disdivamt`.
* There is only **one abnormal negative value** under the current diagnostic rule.
* Missingness is therefore not a broad data-quality problem.

Interpretation:

For most stock-date observations, there is no dividend/distribution amount recorded. Thus, missing `disdivamt` usually means:

```text
No dividend/distribution amount is recorded for this stock on this date.
```

It should not be interpreted as a data failure.

---

### Missing patterns

#### Pattern 1: Normal trading days with no dividend/distribution amount

Characteristics:

* `tradingstatusflg = A`
* Core trading variables are usually valid:

  * `prc`
  * `dlyret`
  * `dlyvol`
  * `dlycap`
* `disdivamt` is missing
* Other distribution-event fields are often also missing

Prevalence:

* This is the dominant pattern.
* In the grouped report, active rows account for nearly all missing `disdivamt` observations.
* Missing rates are close to 100% within many active exchange/security groups.

Interpretation:

These are normal stock-date observations without a recorded dividend/distribution amount. Missing `disdivamt` is structurally expected.

---

#### Pattern 2: Active rows with dividend/distribution amount populated

Characteristics:

* `tradingstatusflg = A`
* `disdivamt` is non-missing
* The row likely corresponds to a dividend or distribution event
* Related fields may also be populated:

  * `disexdt`
  * `dispaydt`
  * `disrecorddt`
  * `distype`
  * `disordinaryflg`
  * `dlyorddivamt`
  * `dlynonorddivamt`

Prevalence:

* Around **1.22%** of all rows have non-missing `disdivamt`.

Interpretation:

These rows are economically meaningful. The distribution amount may explain the difference between total return and price-only return:

```text
dlyret  = total return
dlyretx = price return excluding distributions
dlyreti = income return component
```

So `disdivamt` is useful for diagnosing dividend-related return movements, but it should be used carefully in predictive features because of timing concerns.

---

#### Pattern 3: Fund / ETF distribution rows

Characteristics:

* Many non-missing distribution events may occur among `securitytype = FUND`
* Funds and ETFs often distribute income or capital gains
* Distribution amount fields may be more frequently populated for these securities than for common equities

Interpretation:

For a common-stock return-prediction project, many fund/ETF rows may later be excluded by the stock-universe filter. Therefore, `disdivamt` should not be used to define the universe. Fund/ETF exclusion should be handled separately using security classification variables.

---

#### Pattern 4: Terminal / delisting-related distribution rows

Characteristics:

* Some distribution amounts may be associated with special events, mergers, liquidation payments, or delisting outcomes
* Related variables may include:

  * `dlydelflg`
  * `delactiontype`
  * `delreasontype`
  * `dlynonorddivamt`
  * `disfacpr`
  * `disfacshr`

Interpretation:

These observations may matter for realized return and backtest handling. They should not be treated as normal daily trading signals without checking event timing.

---

#### Pattern 5: Rare abnormal negative `disdivamt`

Characteristics:

* There is only **one** row where `disdivamt < 0`
* This is extremely rare

Interpretation:

This single abnormal case should be inspected manually. It may be a data correction, special corporate action, or input anomaly.

Treatment should be conservative:

```text
Do not globally drop rows because of this one case.
Inspect the row manually.
Decide later whether to set it missing, keep it, or exclude the specific event from dividend-feature construction.
```

---

### Treatment plan

#### Pattern 1 treatment: normal days with no dividend/distribution amount

Do not treat missing `disdivamt` as abnormal.

Recommended treatment:

* Keep `disdivamt` missing in `daily_core`.
* Do not exclude rows because `disdivamt` is missing.
* Do not require `disdivamt` for universe construction.

Rationale:

* Most trading days do not have distribution events.
* Missingness is structurally expected.
* Excluding missing `disdivamt` rows would remove nearly the entire dataset.

---

#### Pattern 2 treatment: active rows with dividend/distribution amount populated

Keep these rows.

For the first baseline model:

* Do not use detailed dividend/distribution amount as a core feature.
* Preserve the variable for diagnostics.
* Use `dlyret` for total-return features and PnL.
* Use `dlyretx` if building price-only momentum/reversal features.

For later feature engineering, possible features include:

```text
has_distribution_amount = 1 if disdivamt is non-missing
distribution_amount_scaled = disdivamt / prc
```

But these features should be added only after confirming event timing to avoid look-ahead bias.

---

#### Pattern 3 treatment: fund / ETF distribution rows

Do not handle funds/ETFs based on `disdivamt`.

Recommended treatment:

* Exclude funds/ETFs later using security classification variables if the project focuses on common stocks.
* Do not use `disdivamt` missingness or non-missingness as a universe filter.

Rationale:

* Distribution behavior differs strongly between common stocks and funds/ETFs.
* The universe definition should be based on security type, not distribution-event fields.

---

#### Pattern 4 treatment: terminal / delisting-related distribution rows

Do not use terminal distribution rows as new prediction candidates.

Retain them for realized-return and backtest handling.

Rationale:

* These rows may encode actual cash/stock payments received by shareholders.
* If a strategy held the stock before a terminal event, the backtest should reflect the realized outcome.
* Dropping such rows may create survivorship or delisting bias.

---

#### Pattern 5 treatment: rare abnormal negative `disdivamt`

Inspect manually.

Recommended treatment:

* Keep the row in `daily_core`.
* Flag it in the abnormal-data report.
* Do not use it for dividend-feature construction until manually verified.

Rationale:

* The abnormal count is too small to affect the broad pipeline.
* Manual inspection is better than applying an aggressive rule globally.

---

### Feature recommendation

For the first baseline model:

```text
Do not include disdivamt as a core feature.
Do not require disdivamt to be non-missing.
Do not use disdivamt for universe construction.
```

For later versions:

```text
has_distribution_amount
distribution_amount_scaled_by_price
ordinary_distribution_indicator
special_distribution_indicator
income_return_component_features
```

Important timing caution:

```text
Only use dividend/distribution features if the event information is known by the prediction time.
Otherwise, using distribution variables can introduce look-ahead bias.
```

A safe first approach is to use `disdivamt` mostly for diagnostics and return interpretation, not for alpha prediction.

---

### Final decision

```text
daily_core:
  Keep disdivamt as-is.
  Do not impute missing values.
  Preserve the rare abnormal negative value for manual inspection.

model_panel / universe:
  Do not exclude rows because disdivamt is missing.
  Do not require disdivamt for common-stock universe construction.
  Exclude non-tradable or terminal rows using trading-status and price/volume filters, not disdivamt.

features:
  Do not include disdivamt in the first baseline model.
  Optionally create dividend/distribution event features later.
  Use strong timing discipline to avoid look-ahead bias.

backtest:
  Retain distribution-related rows.
  Use total-return and terminal-event information when computing realized outcomes.
```

Overall conclusion:

```text
disdivamt missingness is expected and structurally meaningful.
It mostly means that no dividend/distribution amount is recorded for that stock-date.
The variable should be preserved in daily_core, ignored as a mandatory universe filter, and used later only for event diagnostics or carefully timed distribution-related features.
```



