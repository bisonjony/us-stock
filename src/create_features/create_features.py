from pathlib import Path
import shutil
import duckdb


ROOT = Path("/home/xul9527/us-stock")

INPUT_GLOB = ROOT / "data/clean_parquet/daily_core_dup_removed/**/*.parquet"
OUT_FEATURES = ROOT / "data/clean_parquet/daily_features"

if OUT_FEATURES.exists():
    shutil.rmtree(OUT_FEATURES)

OUT_FEATURES.parent.mkdir(parents=True, exist_ok=True)

TMP_DIR = ROOT / "data/duckdb_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(ROOT / "data/us_stock.duckdb"))

# Conservative settings for WSL.
con.execute("PRAGMA threads=1")
con.execute("SET memory_limit='3GB'")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{TMP_DIR}'")
con.execute("SET max_temp_directory_size='150GB'")


# ============================================================
# Helper functions for generated SQL
# ============================================================

RETURN_WINDOWS = [1, 2, 5, 10, 20, 60, 120]
RISK_WINDOWS = [5, 10, 20, 60]
LIQ_WINDOWS = [5, 20, 60]


def make_window_def(name: str, rows_before: int) -> str:
    return (
        f"{name} AS ("
        f"PARTITION BY permno "
        f"ORDER BY dlycaldt "
        f"ROWS BETWEEN {rows_before} PRECEDING AND CURRENT ROW"
        f")"
    )


def rolling_cumret_expr(window_name: str, required_count: int, out_name: str) -> str:
    """
    Rolling cumulative return using returns observed through day t.

    If all required returns exist:
        prod(1 + dlyret) - 1

    If any return in the window is <= -1:
        set cumulative return to -1.0

    If the window is incomplete:
        NULL
    """

    count_expr = f"COUNT(dlyret) OVER {window_name}"
    loss_expr = f"SUM(CASE WHEN dlyret <= -1 THEN 1 ELSE 0 END) OVER {window_name}"
    sumlog_expr = (
        f"SUM(CASE WHEN dlyret > -1 THEN LN(1 + dlyret) ELSE 0 END) "
        f"OVER {window_name}"
    )

    return f"""
        CASE
            WHEN {count_expr} = {required_count}
            THEN
                CASE
                    WHEN {loss_expr} > 0 THEN -1.0
                    ELSE EXP({sumlog_expr}) - 1
                END
            ELSE NULL
        END AS {out_name}
    """


# ============================================================
# Feature SQL blocks
# ============================================================

# -----------------------------
# Block 1: return history
# -----------------------------

return_exprs = []
return_window_defs = []

for w in RETURN_WINDOWS:
    window_name = f"w_ret_{w}"
    return_exprs.append(rolling_cumret_expr(window_name, w, f"ret_{w}d"))
    return_window_defs.append(make_window_def(window_name, w - 1))

# Momentum skipping recent days:
# cumulative return from t-20 to t-5 inclusive.
return_exprs.append(
    rolling_cumret_expr("w_ret_20_5", 16, "ret_20_5")
)

return_window_defs.append(
    "w_ret_20_5 AS ("
    "PARTITION BY permno "
    "ORDER BY dlycaldt "
    "ROWS BETWEEN 20 PRECEDING AND 5 PRECEDING"
    ")"
)

return_expr_sql = ",\n".join(return_exprs)
return_window_sql = ",\n".join(return_window_defs)


# -----------------------------
# Block 2: volatility and downside risk
# -----------------------------

risk_exprs = []

for w in RISK_WINDOWS:
    window_name = f"w_risk_{w}"
    count_ret = f"COUNT(dlyret) OVER {window_name}"
    count_hl = f"COUNT(hl_range) OVER {window_name}"

    risk_exprs.extend([
        f"""
        CASE
            WHEN {count_ret} = {w}
            THEN STDDEV_SAMP(dlyret) OVER {window_name}
            ELSE NULL
        END AS vol_{w}d
        """,

        f"""
        CASE
            WHEN {count_ret} = {w}
            THEN SKEWNESS(dlyret) OVER {window_name}
            ELSE NULL
        END AS skew_{w}d
        """,

        f"""
        CASE
            WHEN {count_ret} = {w}
            THEN MAX(dlyret) OVER {window_name}
            ELSE NULL
        END AS max_ret_{w}d
        """,

        f"""
        CASE
            WHEN {count_ret} = {w}
            THEN MIN(dlyret) OVER {window_name}
            ELSE NULL
        END AS min_ret_{w}d
        """,

        f"""
        CASE
            WHEN {count_ret} = {w}
            THEN SQRT(
                AVG(
                    CASE
                        WHEN dlyret < 0 THEN dlyret * dlyret
                        ELSE 0.0
                    END
                ) OVER {window_name}
            )
            ELSE NULL
        END AS downside_vol_{w}d
        """,

        f"""
        CASE
            WHEN {count_hl} = {w}
            THEN AVG(hl_range) OVER {window_name}
            ELSE NULL
        END AS hl_range_avg_{w}d
        """,
    ])

risk_expr_sql = ",\n".join(risk_exprs)

risk_window_defs = [
    make_window_def(f"w_risk_{w}", w - 1)
    for w in RISK_WINDOWS
]

risk_window_sql = ",\n".join(risk_window_defs)


# -----------------------------
# Block 3: liquidity and volume
# -----------------------------

liq_exprs = []

for w in LIQ_WINDOWS:
    window_name = f"w_liq_{w}"

    liq_exprs.extend([
        f"""
        CASE
            WHEN COUNT(dollar_volume) OVER {window_name} = {w}
            THEN AVG(dollar_volume) OVER {window_name}
            ELSE NULL
        END AS adv{w}
        """,

        f"""
        CASE
            WHEN COUNT(dlyvol) OVER {window_name} = {w}
            THEN AVG(dlyvol) OVER {window_name}
            ELSE NULL
        END AS avg_volume_{w}d
        """,

        f"""
        CASE
            WHEN COUNT(turnover) OVER {window_name} = {w}
            THEN AVG(turnover) OVER {window_name}
            ELSE NULL
        END AS turnover_avg_{w}d
        """,

        f"""
        CASE
            WHEN COUNT(amihud_illiq) OVER {window_name} = {w}
            THEN AVG(amihud_illiq) OVER {window_name}
            ELSE NULL
        END AS amihud_illiq_avg_{w}d
        """,
    ])

liq_expr_sql = ",\n".join(liq_exprs)

liq_window_defs = [
    make_window_def(f"w_liq_{w}", w - 1)
    for w in LIQ_WINDOWS
]

liq_window_sql = ",\n".join(liq_window_defs)


# ============================================================
# Main SQL
# ============================================================

print("\nCreating compact engineered feature table...")

con.execute(f"""
COPY (
    WITH source AS (
        SELECT
            permno,
            dlycaldt,
            year,

            dlyret,
            dlyretx,
            dlyreti,

            prc,
            dlyprc,
            dlyprcflg,
            dlyvol,
            dlycap,
            dlyprcvol,

            dlyopen,
            dlyclose,
            dlyhigh,
            dlylow,
            dlybid,
            dlyask,

            dlynumtrd,
            dlymmcnt,

            shrout,

            tradingstatusflg,
            securityactiveflg,
            dlydelflg,

            disexdt,
            distype,
            disdivamt,
            disfacpr,
            disfacshr,
            disordinaryflg,

            shrfactype

        FROM read_parquet('{INPUT_GLOB}', hive_partitioning=true)
    ),

    base_features AS (
        SELECT
            *,

            -- ---------------------------------------------
            -- Basic flags
            -- ---------------------------------------------
            CASE
                WHEN tradingstatusflg = 'A'
                 AND securityactiveflg = 'Y'
                 AND dlydelflg = 'N'
                THEN 1 ELSE 0
            END AS active_non_delisting_flag,

            CASE
                WHEN dlyprcflg = 'BA' THEN 1 ELSE 0
            END AS price_from_bidask_flag,

            CASE
                WHEN dlyopen IS NULL
                  OR dlyclose IS NULL
                  OR dlyhigh IS NULL
                  OR dlylow IS NULL
                THEN 1 ELSE 0
            END AS ohlc_missing_flag,

            CASE
                WHEN dlyhigh IS NOT NULL
                 AND dlylow IS NOT NULL
                 AND dlyopen IS NOT NULL
                 AND dlyclose IS NOT NULL
                 AND dlyhigh >= dlylow
                 AND dlyopen >= dlylow
                 AND dlyopen <= dlyhigh
                 AND dlyclose >= dlylow
                 AND dlyclose <= dlyhigh
                THEN 1 ELSE 0
            END AS valid_ohlc_flag,

            CASE
                WHEN dlyhigh IS NOT NULL
                 AND dlylow IS NOT NULL
                 AND dlyopen IS NOT NULL
                 AND dlyclose IS NOT NULL
                 AND (
                        dlyhigh < dlylow
                     OR dlyopen > dlyhigh
                     OR dlyopen < dlylow
                     OR dlyclose > dlyhigh
                     OR dlyclose < dlylow
                 )
                THEN 1 ELSE 0
            END AS ohlc_inconsistent_flag,

            CASE
                WHEN dlybid IS NULL OR dlyask IS NULL THEN 1 ELSE 0
            END AS bidask_missing_flag,

            CASE
                WHEN dlybid IS NOT NULL
                 AND dlyask IS NOT NULL
                 AND dlybid > 0
                 AND dlyask > 0
                 AND dlyask >= dlybid
                THEN 1 ELSE 0
            END AS valid_bidask_flag,

            CASE
                WHEN dlybid IS NOT NULL
                 AND dlyask IS NOT NULL
                 AND dlybid > 0
                 AND dlyask > 0
                 AND dlyask < dlybid
                THEN 1 ELSE 0
            END AS crossed_quote_flag,

            CASE
                WHEN disexdt IS NOT NULL
                  OR distype IS NOT NULL
                  OR disdivamt IS NOT NULL
                  OR disfacpr IS NOT NULL
                  OR disfacshr IS NOT NULL
                THEN 1 ELSE 0
            END AS distribution_event_flag,

            CASE
                WHEN distype = 'CD' THEN 1 ELSE 0
            END AS cash_distribution_event_flag,

            CASE
                WHEN distype = 'FRS' THEN 1 ELSE 0
            END AS split_distribution_event_flag,

            CASE
                WHEN disordinaryflg = 'Y' THEN 1 ELSE 0
            END AS ordinary_distribution_event_flag,

            CASE
                WHEN shrfactype IS NOT NULL THEN 1 ELSE 0
            END AS share_factor_event_flag,


            -- ---------------------------------------------
            -- Basic price / size / volume features
            -- ---------------------------------------------
            CASE
                WHEN prc IS NOT NULL AND prc > 0 THEN LN(prc)
                ELSE NULL
            END AS log_prc,

            CASE
                WHEN dlycap IS NOT NULL AND dlycap > 0 THEN LN(dlycap)
                ELSE NULL
            END AS log_dlycap,

            CASE
                WHEN prc IS NOT NULL
                 AND prc > 0
                 AND dlyvol IS NOT NULL
                 AND dlyvol > 0
                THEN prc * dlyvol
                ELSE NULL
            END AS dollar_volume,

            CASE
                WHEN prc IS NOT NULL
                 AND prc > 0
                 AND dlyvol IS NOT NULL
                 AND dlyvol > 0
                THEN LN(prc * dlyvol)
                ELSE NULL
            END AS log_dollar_volume,

            CASE
                WHEN dlynumtrd IS NOT NULL AND dlynumtrd >= 0
                THEN LN(1 + dlynumtrd)
                ELSE NULL
            END AS log_num_trades,

            CASE
                WHEN dlymmcnt IS NOT NULL AND dlymmcnt >= 0
                THEN LN(1 + dlymmcnt)
                ELSE NULL
            END AS log_market_maker_count,

            CASE
                WHEN dlybid IS NOT NULL
                 AND dlyask IS NOT NULL
                 AND dlybid > 0
                 AND dlyask > 0
                 AND dlyask >= dlybid
                THEN (dlyask - dlybid) / ((dlyask + dlybid) / 2.0)
                ELSE NULL
            END AS bid_ask_spread,

            CASE
                WHEN dlyhigh IS NOT NULL
                 AND dlylow IS NOT NULL
                 AND dlyhigh > 0
                 AND dlylow > 0
                 AND dlyhigh >= dlylow
                THEN dlyhigh / dlylow - 1
                ELSE NULL
            END AS hl_range,

            CASE
                WHEN dlyopen IS NOT NULL
                 AND dlyclose IS NOT NULL
                 AND dlyopen > 0
                 AND dlyclose > 0
                THEN dlyclose / dlyopen - 1
                ELSE NULL
            END AS open_close_ret,

            CASE
                WHEN dlyvol IS NOT NULL
                 AND dlyvol >= 0
                 AND shrout IS NOT NULL
                 AND shrout > 0
                THEN dlyvol / (shrout * 1000.0)
                ELSE NULL
            END AS turnover,

            CASE
                WHEN dlyret IS NOT NULL
                 AND prc IS NOT NULL
                 AND prc > 0
                 AND dlyvol IS NOT NULL
                 AND dlyvol > 0
                THEN ABS(dlyret) / (prc * dlyvol)
                ELSE NULL
            END AS amihud_illiq

        FROM source
    ),

    return_features AS (
        SELECT
            *,
            {return_expr_sql}
        FROM base_features
        WINDOW
            {return_window_sql}
    ),

    risk_features AS (
        SELECT
            *,
            {risk_expr_sql}
        FROM return_features
        WINDOW
            {risk_window_sql}
    ),

    liquidity_features AS (
        SELECT
            *,
            {liq_expr_sql}
        FROM risk_features
        WINDOW
            {liq_window_sql}
    ),

    shock_features AS (
        SELECT
            *,

            CASE
                WHEN avg_volume_20d IS NOT NULL
                 AND avg_volume_20d > 0
                 AND dlyvol IS NOT NULL
                THEN dlyvol / avg_volume_20d
                ELSE NULL
            END AS volume_shock,

            CASE
                WHEN adv20 IS NOT NULL
                 AND adv20 > 0
                 AND dollar_volume IS NOT NULL
                THEN dollar_volume / adv20
                ELSE NULL
            END AS dollar_volume_shock

        FROM liquidity_features
    ),

    pressure_features AS (
        SELECT
            *,

            -- ---------------------------------------------
            -- Block 4: price pressure
            -- ---------------------------------------------
            CASE
                WHEN ret_1d IS NOT NULL AND volume_shock IS NOT NULL
                THEN ret_1d * volume_shock
                ELSE NULL
            END AS ret_1d_x_volume_shock,

            CASE
                WHEN ret_5d IS NOT NULL AND volume_shock IS NOT NULL
                THEN ret_5d * volume_shock
                ELSE NULL
            END AS ret_5d_x_volume_shock,

            CASE
                WHEN dlyret > 0
                 AND dlyvol IS NOT NULL
                 AND avg_volume_20d IS NOT NULL
                THEN dlyvol - avg_volume_20d
                WHEN dlyret < 0
                 AND dlyvol IS NOT NULL
                 AND avg_volume_20d IS NOT NULL
                THEN -(dlyvol - avg_volume_20d)
                WHEN dlyret = 0 THEN 0.0
                ELSE NULL
            END AS signed_abnormal_volume,

            CASE
                WHEN dlyret > 0 AND volume_shock IS NOT NULL
                THEN volume_shock - 1
                WHEN dlyret < 0 AND volume_shock IS NOT NULL
                THEN -(volume_shock - 1)
                WHEN dlyret = 0 THEN 0.0
                ELSE NULL
            END AS signed_abnormal_volume_ratio,

            CASE
                WHEN dlyret > 0 AND dollar_volume_shock IS NOT NULL
                THEN dollar_volume_shock - 1
                WHEN dlyret < 0 AND dollar_volume_shock IS NOT NULL
                THEN -(dollar_volume_shock - 1)
                WHEN dlyret = 0 THEN 0.0
                ELSE NULL
            END AS signed_dollar_volume_shock,

            CASE
                WHEN ret_1d IS NOT NULL
                 AND volume_shock IS NOT NULL
                 AND ret_1d >= 0.02
                 AND volume_shock >= 2.0
                THEN 1 ELSE 0
            END AS strong_up_high_volume_flag,

            CASE
                WHEN ret_1d IS NOT NULL
                 AND volume_shock IS NOT NULL
                 AND ret_1d <= -0.02
                 AND volume_shock >= 2.0
                THEN 1 ELSE 0
            END AS strong_down_high_volume_flag,

            CASE
                WHEN ret_1d IS NOT NULL
                 AND volume_shock IS NOT NULL
                 AND ret_1d > 0
                 AND volume_shock > 1
                THEN ret_1d * (volume_shock - 1)
                ELSE NULL
            END AS up_high_volume_pressure,

            CASE
                WHEN ret_1d IS NOT NULL
                 AND volume_shock IS NOT NULL
                 AND ret_1d < 0
                 AND volume_shock > 1
                THEN ret_1d * (volume_shock - 1)
                ELSE NULL
            END AS down_high_volume_pressure

        FROM shock_features
    )

    SELECT
        -- Join keys only
        permno,
        dlycaldt,
        year,

        -- Basic flags
        active_non_delisting_flag,
        price_from_bidask_flag,
        ohlc_missing_flag,
        valid_ohlc_flag,
        ohlc_inconsistent_flag,
        bidask_missing_flag,
        valid_bidask_flag,
        crossed_quote_flag,
        distribution_event_flag,
        cash_distribution_event_flag,
        split_distribution_event_flag,
        ordinary_distribution_event_flag,
        share_factor_event_flag,

        -- Basic engineered price / size / liquidity features
        log_prc,
        log_dlycap,
        dollar_volume,
        log_dollar_volume,
        log_num_trades,
        log_market_maker_count,
        bid_ask_spread,
        hl_range,
        open_close_ret,
        turnover,
        amihud_illiq,

        -- Feature block 1: return history
        ret_1d,
        ret_2d,
        ret_5d,
        ret_10d,
        ret_20d,
        ret_60d,
        ret_120d,
        ret_20_5,

        -- Feature block 2: volatility and downside risk
        vol_5d,
        vol_10d,
        vol_20d,
        vol_60d,
        skew_5d,
        skew_10d,
        skew_20d,
        skew_60d,
        max_ret_5d,
        max_ret_10d,
        max_ret_20d,
        max_ret_60d,
        min_ret_5d,
        min_ret_10d,
        min_ret_20d,
        min_ret_60d,
        downside_vol_5d,
        downside_vol_10d,
        downside_vol_20d,
        downside_vol_60d,
        hl_range_avg_5d,
        hl_range_avg_10d,
        hl_range_avg_20d,
        hl_range_avg_60d,

        -- Feature block 3: liquidity and volume
        adv5,
        adv20,
        adv60,
        avg_volume_5d,
        avg_volume_20d,
        avg_volume_60d,
        volume_shock,
        dollar_volume_shock,
        turnover_avg_5d,
        turnover_avg_20d,
        turnover_avg_60d,
        amihud_illiq_avg_5d,
        amihud_illiq_avg_20d,
        amihud_illiq_avg_60d,

        -- Feature block 4: price pressure
        ret_1d_x_volume_shock,
        ret_5d_x_volume_shock,
        signed_abnormal_volume,
        signed_abnormal_volume_ratio,
        signed_dollar_volume_shock,
        strong_up_high_volume_flag,
        strong_down_high_volume_flag,
        up_high_volume_pressure,
        down_high_volume_pressure

    FROM pressure_features
)
TO '{OUT_FEATURES}'
(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION SNAPPY);
""")

print(f"\nSaved compact engineered features to: {OUT_FEATURES}")
print("\nFinished creating features.")