# table2.py
from __future__ import annotations

from typing import Literal

import polars as pl

from common_metrics import (
    compute_holding_and_excess_returns,
    compute_basis,
    summarize_series_bimonthly,
)


def assign_basis_portfolios(
    df_basis: pl.DataFrame,
    short_maturity_rank: int = 1,
) -> pl.DataFrame:
    """
    Sort commodities into 4 portfolios (P1..P4) by the basis of the chosen
    short maturity rank, at each date.
    """
    # Use chosen maturity rank's basis as sorting variable
    sort_df = (
        df_basis
        .filter(pl.col("contract_rank") == short_maturity_rank)
        .select("date", "commodity", basis_sort=pl.col("basis"))
    )

    # Rank within date
    sort_df = (
        sort_df
        .sort(["date", "basis_sort"])
        .with_columns(
            pl.arange(0, pl.count()).over("date").alias("rank_within_date")
        )
    )

    # Quartiles -> portfolios 1..4
    sort_df = sort_df.with_columns(
        (
            (4 * pl.col("rank_within_date") / (pl.count().over("date")))
            .floor()
            .cast(pl.Int32)
            + 1
        ).clip(1, 4).alias("basis_portfolio")
    ).drop(["rank_within_date"])

    # Merge back to full df_basis
    df_basis = df_basis.join(
        sort_df.select("date", "commodity", "basis_portfolio"),
        on=["date", "commodity"],
        how="left",
    )

    return df_basis


def compute_basis_sorted_portfolio_returns(
    df: pl.DataFrame,
    short_maturity_rank: int = 1,
) -> pl.DataFrame:
    """
    Compute equal-weight Short Roll and Excess Holding returns for basis-sorted
    portfolios, by contract_rank and date.
    """
    # SR, Holding, Excess (spread)
    df_he = compute_holding_and_excess_returns(df)

    # Basis
    df_b = compute_basis(df_he)

    # Portfolio assignment on chosen short maturity rank
    df_b = assign_basis_portfolios(df_b, short_maturity_rank=short_maturity_rank)

    # Equal-weight portfolio returns
    port_rets = (
        df_b
        .group_by(["date", "basis_portfolio", "contract_rank"])
        .agg(
            pl.col("ret_sr").mean().alias("ret_sr"),
            pl.col("ret_eh").mean().alias("ret_eh"),
        )
    )

    return port_rets


def summarize_table2(
    port_rets: pl.DataFrame,
    return_col: Literal["ret_sr", "ret_eh"],
    nw_lags: int = 1,
) -> pl.DataFrame:
    """
    Table 2-style summary:

    - For each basis_portfolio=1..4 and contract_rank: mean_ann, std_ann, t_stat.
    - High-minus-low spread (P4-P1) per contract_rank.
    """
    rows = []

    # Portfolio stats
    for p, n in port_rets.select("basis_portfolio", "contract_rank").unique().iter_rows():
        sub = port_rets.filter(
            (pl.col("basis_portfolio") == p) &
            (pl.col("contract_rank") == n)
        )
        r = sub[return_col].to_numpy()
        stats = summarize_series_bimonthly(r, nw_lags=nw_lags)

        rows.append(
            {
                "panel": "Portfolio",
                "basis_portfolio": int(p),
                "contract_rank": int(n),
                "mean_ann": stats["mean_ann"],
                "std_ann": stats["std_ann"],
                "t_stat": stats["t_stat"],
            }
        )

    # High-minus-low (P4-P1)
    for n in port_rets["contract_rank"].unique().to_list():
        p1 = port_rets.filter(
            (pl.col("basis_portfolio") == 1) &
            (pl.col("contract_rank") == n)
        ).sort("date")
        p4 = port_rets.filter(
            (pl.col("basis_portfolio") == 4) &
            (pl.col("contract_rank") == n)
        ).sort("date")

        merged = p1.join(
            p4.select("date", return_col).rename({return_col: "ret_p4"}),
            on="date",
            how="inner",
        )

        spread = merged["ret_p4"] - merged[return_col]
        stats = summarize_series_bimonthly(spread.to_numpy(), nw_lags=nw_lags)

        rows.append(
            {
                "panel": "P4-P1",
                "basis_portfolio": -1,
                "contract_rank": int(n),
                "mean_ann": stats["mean_ann"],
                "std_ann": stats["std_ann"],
                "t_stat": stats["t_stat"],
            }
        )

    return pl.DataFrame(rows)


def build_table2(
    raw_df: pl.DataFrame,
    short_maturity_rank: int = 1,
    nw_lags: int = 1,
) -> pl.DataFrame:
    """
    Build Table 2-style summary for both Short Roll and Excess Holding panels.

    Returns:
    ["panel_type", "panel", "basis_portfolio", "contract_rank",
     "mean_ann", "std_ann", "t_stat"]
    where panel_type in {"Short Roll", "Excess Holding"}.
    """
    port_rets = compute_basis_sorted_portfolio_returns(
        raw_df,
        short_maturity_rank=short_maturity_rank,
    )

    # Short Roll summary
    sr_summ = summarize_table2(port_rets, "ret_sr", nw_lags=nw_lags)
    sr_summ = sr_summ.with_columns(pl.lit("Short Roll").alias("panel_type"))

    # Excess Holding summary
    eh_summ = summarize_table2(port_rets, "ret_eh", nw_lags=nw_lags)
    eh_summ = eh_summ.with_columns(pl.lit("Excess Holding").alias("panel_type"))

    return pl.concat([sr_summ, eh_summ])


# -----------------------------
# Internal tests
# -----------------------------

def _mock_data_for_table2() -> pl.DataFrame:
    """
    Simple 2-commodity, 2-rank, 4-date dataset where B has higher basis than A.
    """
    dates = [1, 2, 3, 4]

    data = {
        "date": dates * 4,
        "commodity": (["A"] * 8) + (["B"] * 8),
        "contract_rank": [1, 1, 1, 1, 2, 2, 2, 2] * 2,
        # A: low basis, B: high basis (n=2 further above n=1)
        "futures_price": [
            100, 110, 121, 133.1,        # A, n=1
            105, 115.5, 127.05, 139.755, # A, n=2
            100, 110, 121, 133.1,        # B, n=1
            110, 121, 133.1, 146.41      # B, n=2
        ],
    }

    return pl.DataFrame(data)


def _test_basis_sorting_basic():
    """
    A should get lower basis_portfolio than B when sorting on n=2 basis.
    """
    df = _mock_data_for_table2()
    df_he = compute_holding_and_excess_returns(df)
    df_b = compute_basis(df_he)
    df_b = assign_basis_portfolios(df_b, short_maturity_rank=2)

    one_date = df_b.filter(pl.col("date") == 1).select("commodity", "basis_portfolio")
    mapping = dict(zip(one_date["commodity"], one_date["basis_portfolio"]))

    assert mapping["A"] <= mapping["B"], f"A basis_portfolio {mapping['A']} > B {mapping['B']}"
    assert set(mapping.values()).issubset({1, 2, 3, 4})


def run_all_tests():
    _test_basis_sorting_basic()
    print("All table2 tests passed.")


if __name__ == "__main__":
    run_all_tests()
