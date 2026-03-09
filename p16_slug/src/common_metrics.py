# common_metrics.py
from __future__ import annotations

import math
from typing import Tuple, Dict

import polars as pl
import numpy as np
from statsmodels.stats.sandwich_covariance import cov_hac
from statsmodels.regression.linear_model import OLS


# -----------------------------
# Core per-contract computations
# -----------------------------

def compute_short_roll_returns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Short Roll per contract: R_SR_{t+1}(n) = (F_{t+1}^n - F_t^n) / F_t^n.

    Assumes df has: ["date", "commodity", "contract_rank", "futures_price"].
    Returns df with added "ret_sr".
    """
    df = df.sort(["commodity", "contract_rank", "date"])

    return (
        df
        .with_columns(
            pl.col("futures_price")
            .pct_change()
            .over(["commodity", "contract_rank"])
            .alias("ret_sr")
        )
        .drop_nulls(subset=["ret_sr"])
    )


def compute_excess_holding_spread(df_sr: pl.DataFrame) -> pl.DataFrame:
    """
    Excess Holding as requested:

    For n = x >= 2:
        ret_eh_t(x) = (F_t^x - F_t^1) / F_t^1,
    i.e., long contract x, short contract 1, normalized by the nearby price.

    For n = 1:
        ret_eh_t(1) = 0.

    Input: df_sr must have:
        "date", "commodity", "contract_rank", "futures_price", "ret_sr".
    Returns df_sr with new column "ret_eh".
    """
    df = df_sr

    # Nearby (n=1) price per commodity-date
    nearby = (
        df
        .filter(pl.col("contract_rank") == 1)
        .select(
            "date",
            "commodity",
            nearby_price=pl.col("futures_price"),
        )
    )

    # Join nearby_price into all ranks
    df = df.join(nearby, on=["commodity", "date"], how="left")

    # Excess Holding: (F_t^x - F_t^1) / F_t^1, 0 for n=1
    df = df.with_columns(
        pl.when(pl.col("contract_rank") == 1)
        .then(pl.lit(0.0))
        .otherwise(
            (pl.col("futures_price") - pl.col("nearby_price")) / pl.col("nearby_price")
        )
        .alias("ret_eh")
    )

    return df


def compute_holding_and_excess_returns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Combine Short Roll, Holding, and Excess Holding:

    - ret_sr: Short Roll = (F_{t+1}^n - F_t^n) / F_t^n.
    - ret_hold: equal to ret_sr (one-period hold of rank n).
    - ret_eh: Excess Holding spread:
        long n=x, short n=1: (F_t^x - F_t^1) / F_t^1.
    """
    df_sr = compute_short_roll_returns(df)

    # Holding = Short Roll at one-period horizon
    df_sr = df_sr.with_columns(
        pl.col("ret_sr").alias("ret_hold")
    )

    # Excess Holding spread
    df_sr = compute_excess_holding_spread(df_sr)

    return df_sr


def compute_basis(
    df: pl.DataFrame,
    basis_rank_for_spot: int = 1,
) -> pl.DataFrame:
    """
    Basis as in the paper, with S_t = F_t^{(basis_rank_for_spot)} (default n=1):

    y_t^n = (1/n) * ln(F_t^n / S_t).

    Adds column "basis" to df.
    """
    # Extract spot price per (commodity, date)
    spot = (
        df
        .filter(pl.col("contract_rank") == basis_rank_for_spot)
        .select(
            "date",
            "commodity",
            spot_price=pl.col("futures_price"),
        )
    )

    df = df.join(spot, on=["commodity", "date"], how="left")

    df = df.with_columns(
        (
            (pl.col("futures_price") / pl.col("spot_price")).log()
            / pl.col("contract_rank").cast(pl.Float64)
        ).alias("basis")
    )

    return df


# -----------------------------
# Time-series statistics
# -----------------------------

def newey_west_tstat(
    returns: np.ndarray,
    lags: int = 1,
) -> Tuple[float, float, float]:
    """
    Compute sample mean, Newey-West HAC standard error (for mean), and t-stat.

    Implementation: OLS of R_t on constant, HAC covariance with maxlags=lags.
    """
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]

    if r.size == 0:
        return np.nan, np.nan, np.nan

    X = np.ones((r.shape[0], 1))
    model = OLS(r, X)
    res = model.fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    alpha = res.params[0]
    cov = res.cov_params()
    se = math.sqrt(cov[0, 0]) if cov.size > 0 else np.nan
    t_stat = alpha / se if se not in (0.0, np.nan) else np.nan

    return float(alpha), float(se), float(t_stat)


def summarize_series_bimonthly(
    returns: np.ndarray,
    nw_lags: int = 1,
) -> Dict[str, float]:
    """
    For bimonthly returns, compute:

    - mean_ann = 6 * mean_per
    - std_ann = sqrt(6) * std_per
    - t_stat from Newey-West regression on the per-period mean
    """
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]

    if r.size == 0:
        return {"mean_ann": np.nan, "std_ann": np.nan, "t_stat": np.nan}

    mean_per = np.mean(r)
    std_per = np.std(r, ddof=1) if r.size > 1 else 0.0

    mean_ann = 6.0 * mean_per
    std_ann = std_per * math.sqrt(6.0)

    alpha, se, t_stat = newey_west_tstat(r, lags=nw_lags)

    return {
        "mean_ann": float(mean_ann),
        "std_ann": float(std_ann),
        "t_stat": float(t_stat),
    }


# -----------------------------
# Internal tests
# -----------------------------

def _test_short_roll():
    """
    If prices grow 10% per period, ret_sr should be 0.10 each period.
    """
    df_test = pl.DataFrame(
        {
            "date": [1, 2, 3, 4],
            "commodity": ["X"] * 4,
            "contract_rank": [1] * 4,
            "futures_price": [100.0, 110.0, 121.0, 133.1],
        }
    )

    out = compute_short_roll_returns(df_test)
    sr = out["ret_sr"].to_list()

    assert len(sr) == 3
    assert all(abs(x - 0.1) < 1e-8 for x in sr), f"Short Roll test failed: {sr}"


def _test_basis():
    """
    If F^1 = 100, F^2 = 110, basis n=2 = (1/2)*ln(110/100); basis n=1 = 0.
    """
    df_test = pl.DataFrame(
        {
            "date": [1, 1],
            "commodity": ["X", "X"],
            "contract_rank": [1, 2],
            "futures_price": [100.0, 110.0],
        }
    )

    out = compute_basis(df_test)
    basis_1 = out.filter(pl.col("contract_rank") == 1)["basis"][0]
    basis_2 = out.filter(pl.col("contract_rank") == 2)["basis"][0]

    assert abs(basis_1 - 0.0) < 1e-10
    expected = 0.5 * math.log(110.0 / 100.0)
    assert abs(basis_2 - expected) < 1e-10, f"Basis n=2 incorrect: {basis_2} vs {expected}"


def _test_excess_holding_spread():
    """
    If at a given date:
      F^1 = 100, F^2 = 110
    then ret_eh for n=2 should be 0.10, and 0 for n=1.
    """
    df_test = pl.DataFrame(
        {
            "date": [1, 1],
            "commodity": ["X", "X"],
            "contract_rank": [1, 2],
            "futures_price": [100.0, 110.0],
        }
    )

    df_sr = compute_short_roll_returns(
        df_test.sort(["commodity", "contract_rank", "date"])
    )
    # In this toy example there is only one date, so SR returns will be empty.
    # Instead, just add a dummy ret_sr:
    df_sr = df_test.with_columns(pl.lit(0.0).alias("ret_sr"))

    df_eh = compute_excess_holding_spread(df_sr)
    n1 = df_eh.filter(pl.col("contract_rank") == 1)["ret_eh"][0]
    n2 = df_eh.filter(pl.col("contract_rank") == 2)["ret_eh"][0]

    assert abs(n1 - 0.0) < 1e-10, f"EH n=1 must be 0, got {n1}"
    assert abs(n2 - 0.1) < 1e-10, f"EH n=2 must be 0.1, got {n2}"


def run_all_tests():
    _test_short_roll()
    _test_basis()
    _test_excess_holding_spread()
    print("All common_metrics tests passed.")


if __name__ == "__main__":
    run_all_tests()
