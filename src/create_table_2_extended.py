"""
create_table_2_extended.py
--------------------------
Extended-sample version of Table 2.

Same construction as create_table_2.py, but using returns_panel_extended.parquet.
"""

import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd

from settings import config
from process_futures import build_bimonthly_panel
from create_table_1 import newey_west_stats
from create_table_2 import (  # reuse helpers
    sort_into_portfolios,
    portfolio_stats,
    is_monotone,
    _fixed_sort_stats,
    _print_portfolio_panel,
    _print_spread_only,
    _save_table2_latex_core,
)

DATA_DIR   = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")


def build_table_2(returns_df=None, data_dir=DATA_DIR):
    """
    Build all three panels of Table 2 for the extended sample.
    """
    data_dir = Path(data_dir)

    if returns_df is None:
        panel_path = data_dir / "returns_panel_extended.parquet"
        if panel_path.exists():
            returns_df = pd.read_parquet(panel_path)
        else:
            returns_df = build_bimonthly_panel(data_dir=data_dir)

    df = returns_df.copy()

    mean_basis = df.groupby("product_code")["basis"].mean().rename("basis_mean")
    df = df.merge(mean_basis, on="product_code", how="left")
    df["basis_demean"] = df["basis"] - df["basis_mean"]

    print("Panel A (extended): sorting on current basis...")
    port_a      = sort_into_portfolios(df, sort_variable="basis")
    panel_a_sr  = portfolio_stats(port_a, "sr", [1, 2, 3, 4], {1:1, 2:1, 3:1, 4:1})
    panel_a_eh  = portfolio_stats(port_a, "eh", [2, 3, 4],   {2:2, 3:3, 4:4})

    print("Panel B.1 (extended): sorting on mean basis...")
    df_b1 = df.copy()
    unique_comms = df_b1[["product_code", "basis_mean"]].drop_duplicates()
    unique_comms["port_b1"] = pd.qcut(
        unique_comms["basis_mean"], q=4,
        labels=["P1", "P2", "P3", "P4"],
        duplicates="drop"
    )
    df_b1   = df_b1.merge(unique_comms[["product_code", "port_b1"]],
                          on="product_code", how="left")
    port_b1 = _fixed_sort_stats(df_b1, "port_b1")
    panel_b1_sr = portfolio_stats(port_b1, "sr", [1, 2, 3, 4], {1:1, 2:1, 3:1, 4:1})
    panel_b1_eh = portfolio_stats(port_b1, "eh", [2, 3, 4],   {2:2, 3:3, 4:4})

    print("Panel B.2 (extended): sorting on de-meaned basis...")
    port_b2     = sort_into_portfolios(df, sort_variable="basis_demean")
    panel_b2_sr = portfolio_stats(port_b2, "sr", [1, 2, 3, 4], {1:1, 2:1, 3:1, 4:1})
    panel_b2_eh = portfolio_stats(port_b2, "eh", [2, 3, 4],   {2:2, 3:3, 4:4})

    return {
        "panel_a_sr"  : panel_a_sr,
        "panel_a_eh"  : panel_a_eh,
        "panel_b1_sr" : panel_b1_sr,
        "panel_b1_eh" : panel_b1_eh,
        "panel_b2_sr" : panel_b2_sr,
        "panel_b2_eh" : panel_b2_eh,
    }


def format_table_2(table_dict, output_dir=OUTPUT_DIR):
    """Print to console and save CSV + extended-sample LaTeX."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 80)
    print("TABLE 2 (Extended): Sorts Based on the Basis")
    print("=" * 80)

    print("\nPANEL A: Sort on Current Basis")
    print("\nShort Roll Returns (annualized)")
    _print_portfolio_panel(table_dict["panel_a_sr"], [1, 2, 3, 4])
    print("\nExcess Holding Returns (annualized)")
    _print_portfolio_panel(table_dict["panel_a_eh"], [2, 3, 4])

    print("\nPANEL B.1: Sort on Mean Basis (non-investable)")
    print("Short Roll:")
    _print_spread_only(table_dict["panel_b1_sr"], [1, 2, 3, 4], check_mono=True)
    print("Excess Holding:")
    _print_spread_only(table_dict["panel_b1_eh"], [2, 3, 4], check_mono=True)

    print("\nPANEL B.2: Sort on De-Meaned Basis")
    print("Short Roll:")
    _print_spread_only(table_dict["panel_b2_sr"], [1, 2, 3, 4], check_mono=True)
    print("Excess Holding:")
    _print_spread_only(table_dict["panel_b2_eh"], [2, 3, 4], check_mono=True)

    for panel_name, panel_df in table_dict.items():
        panel_df.to_csv(output_dir / f"table2_{panel_name}_extended.csv")

    print(f"\nCSVs saved → {output_dir}")

    _save_table2_latex_core(
        table_dict,
        caption="Sorts Based on the Basis (Extended Sample)",
        label="tab:table2_extended",
        tex_name="table2_extended.tex",
        output_dir=output_dir,
    )


if __name__ == "__main__":
    print("Building Table 2 (extended)...")
    table = build_table_2()
    format_table_2(table)
