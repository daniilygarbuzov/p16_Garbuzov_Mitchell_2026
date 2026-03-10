"""
create_table_2.py
-----------------
Replicates Table 2 of Szymanowska et al. (2014):

    "Sorts Based on the Basis: Mean returns and standard deviations
     for Short Roll and Excess Holding returns when commodity futures
     are sorted into quartile portfolios by their log basis."

Panels:
    A   — Sort on current basis level
    B.1 — Sort on full-sample mean basis (non-investable benchmark)
    B.2 — Sort on de-meaned basis (time-series variation only)

Usage
-----
    ipython ./src/create_table_2.py
    from create_table_2 import build_table_2
"""

import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd

from settings import config
from process_futures import build_bimonthly_panel
from create_table_1 import newey_west_stats

DATA_DIR     = config("DATA_DIR")
OUTPUT_DIR   = config("OUTPUT_DIR")
N_PORTFOLIOS = 4


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Portfolio sort engine
# ─────────────────────────────────────────────────────────────────────────────

def sort_into_portfolios(returns_df, sort_variable, n_portfolios=4,
                          min_per_portfolio=3):
    """
    At each bimonthly date, rank commodities by sort_variable and
    assign to equally-weighted quartile portfolios.

    Returns
    -------
    dict
        Keys: 'P1'..'P4', 'P4_minus_P1'
        Values: pd.DataFrame indexed by obs_date
    """
    return_cols = ["sr_1", "sr_2", "sr_3", "sr_4",
                   "eh_2", "eh_3", "eh_4"]

    portfolio_returns = {f"P{i+1}": [] for i in range(n_portfolios)}
    portfolio_returns["P4_minus_P1"] = []

    for obs_dt, day_df in returns_df.groupby("obs_date"):
        valid = day_df.dropna(subset=[sort_variable])
        if len(valid) < n_portfolios * min_per_portfolio:
            continue

        valid = valid.copy()
        valid["portfolio"] = pd.qcut(
            valid[sort_variable],
            q=n_portfolios,
            labels=[f"P{i+1}" for i in range(n_portfolios)],
            duplicates="drop"
        )

        port_means = {}
        for pname in [f"P{i+1}" for i in range(n_portfolios)]:
            pdata = valid[valid["portfolio"] == pname]
            if len(pdata) < min_per_portfolio:
                port_means[pname] = None
                continue
            means = {col: pdata[col].mean() for col in return_cols}
            means["obs_date"]  = obs_dt
            means["n_members"] = len(pdata)
            port_means[pname]  = means

        if all(v is not None for v in port_means.values()):
            for pname, row in port_means.items():
                portfolio_returns[pname].append(row)

            spread = {"obs_date": obs_dt}
            for col in return_cols:
                spread[col] = port_means["P4"][col] - port_means["P1"][col]
            portfolio_returns["P4_minus_P1"].append(spread)

    result = {}
    for pname, records in portfolio_returns.items():
        if records:
            result[pname] = pd.DataFrame(records).set_index("obs_date")
        else:
            result[pname] = pd.DataFrame()

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Compute statistics for sorted portfolios
# ─────────────────────────────────────────────────────────────────────────────

def portfolio_stats(port_dict, return_type, maturities, n_periods_map):
    """
    Annualized mean, std dev, NW t-stat for each portfolio and spread.

    Parameters
    ----------
    return_type : str
        'sr' or 'eh'
    maturities : list[int]
    n_periods_map : dict
        {maturity: n_bimonthly_periods}

    Returns
    -------
    pd.DataFrame  (rows = portfolios, cols = mean_nX, std_nX, t_nX)
    """
    portfolio_names = ["P1", "P2", "P3", "P4", "P4_minus_P1"]
    rows = []

    for pname in portfolio_names:
        if pname not in port_dict or port_dict[pname].empty:
            row = {"portfolio": pname}
            for n in maturities:
                row[f"mean_n{n}"] = np.nan
                row[f"std_n{n}"]  = np.nan
                row[f"t_n{n}"]    = np.nan
            rows.append(row)
            continue

        df_p = port_dict[pname]
        row  = {"portfolio": pname}

        for n in maturities:
            col = f"{return_type}_{n}"
            if col not in df_p.columns:
                row[f"mean_n{n}"] = np.nan
                row[f"std_n{n}"]  = np.nan
                row[f"t_n{n}"]    = np.nan
                continue
            n_periods = n_periods_map.get(n, 1)
            stats = newey_west_stats(df_p[col], n_periods=n_periods)
            row[f"mean_n{n}"] = stats["mean_ann"]
            row[f"std_n{n}"]  = stats["std_ann"]
            row[f"t_n{n}"]    = stats["t_stat"]

        rows.append(row)

    return pd.DataFrame(rows).set_index("portfolio")


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Monotonicity check
# ─────────────────────────────────────────────────────────────────────────────

def is_monotone(stats_df, col, direction="either"):
    vals = [stats_df.loc[f"P{i+1}", col]
            for i in range(4) if f"P{i+1}" in stats_df.index]
    vals = [v for v in vals if not np.isnan(v)]
    if len(vals) < 2:
        return False
    inc = all(vals[i] <= vals[i+1] for i in range(len(vals)-1))
    dec = all(vals[i] >= vals[i+1] for i in range(len(vals)-1))
    if direction == "increasing": return inc
    if direction == "decreasing": return dec
    return inc or dec


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Fixed-sort helper (Panel B.1)
# ─────────────────────────────────────────────────────────────────────────────

def _fixed_sort_stats(df_with_port, port_col):
    """
    For Panel B.1: commodities have a fixed full-sample portfolio assignment.
    Build portfolio time-series by averaging over dates.
    """
    return_cols = ["sr_1", "sr_2", "sr_3", "sr_4",
                   "eh_2", "eh_3", "eh_4"]
    result = {}

    for pname in ["P1", "P2", "P3", "P4"]:
        sub = df_with_port[df_with_port[port_col] == pname]
        if sub.empty:
            result[pname] = pd.DataFrame()
            continue
        result[pname] = sub.groupby("obs_date")[return_cols].mean()

    if "P4" in result and "P1" in result and (
            not result["P4"].empty and not result["P1"].empty):
        common = result["P4"].index.intersection(result["P1"].index)
        result["P4_minus_P1"] = result["P4"].loc[common] - result["P1"].loc[common]
    else:
        result["P4_minus_P1"] = pd.DataFrame()

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Main Table 2 builder
# ─────────────────────────────────────────────────────────────────────────────

def build_table_2(returns_df=None, data_dir=DATA_DIR):
    """
    Build all three panels of Table 2.

    Returns
    -------
    dict
        panel_a_sr, panel_a_eh,
        panel_b1_sr, panel_b1_eh,
        panel_b2_sr, panel_b2_eh
    """
    data_dir = Path(data_dir)

    if returns_df is None:
        panel_path = data_dir / "returns_panel.parquet"
        if panel_path.exists():
            returns_df = pd.read_parquet(panel_path)
        else:
            returns_df = build_bimonthly_panel(data_dir=data_dir)

    df = returns_df.copy()

    # Mean basis per commodity (for Panels B)
    mean_basis = df.groupby("product_code")["basis"].mean().rename("basis_mean")
    df = df.merge(mean_basis, on="product_code", how="left")
    df["basis_demean"] = df["basis"] - df["basis_mean"]

    # ── Panel A: sort on current basis ──
    print("Panel A: sorting on current basis...")
    port_a      = sort_into_portfolios(df, sort_variable="basis")
    panel_a_sr  = portfolio_stats(port_a, "sr", [1,2,3,4], {1:1,2:1,3:1,4:1})
    panel_a_eh  = portfolio_stats(port_a, "eh", [2,3,4],   {2:2,3:3,4:4})

    # ── Panel B.1: sort on mean basis (fixed, non-investable) ──
    print("Panel B.1: sorting on mean basis...")
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
    panel_b1_sr = portfolio_stats(port_b1, "sr", [1,2,3,4], {1:1,2:1,3:1,4:1})
    panel_b1_eh = portfolio_stats(port_b1, "eh", [2,3,4],   {2:2,3:3,4:4})

    # ── Panel B.2: sort on de-meaned basis ──
    print("Panel B.2: sorting on de-meaned basis...")
    port_b2     = sort_into_portfolios(df, sort_variable="basis_demean")
    panel_b2_sr = portfolio_stats(port_b2, "sr", [1,2,3,4], {1:1,2:1,3:1,4:1})
    panel_b2_eh = portfolio_stats(port_b2, "eh", [2,3,4],   {2:2,3:3,4:4})

    return {
        "panel_a_sr"  : panel_a_sr,
        "panel_a_eh"  : panel_a_eh,
        "panel_b1_sr" : panel_b1_sr,
        "panel_b1_eh" : panel_b1_eh,
        "panel_b2_sr" : panel_b2_sr,
        "panel_b2_eh" : panel_b2_eh,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Formatting and output
# ─────────────────────────────────────────────────────────────────────────────

def _print_portfolio_panel(stats_df, maturities):
    labels = {"P1": "Low", "P2": "P2", "P3": "P3",
              "P4": "High", "P4_minus_P1": "P4-P1"}
    header = f"{'Portfolio':<12}" + "".join(
        f"  {'n='+str(n):^22}" for n in maturities
    )
    print(header)
    for pkey, plabel in labels.items():
        if pkey not in stats_df.index:
            continue
        row   = stats_df.loc[pkey]
        parts = []
        for n in maturities:
            mean  = row.get(f"mean_n{n}", np.nan)
            std   = row.get(f"std_n{n}",  np.nan)
            tstat = row.get(f"t_n{n}",    np.nan)
            ms = f"{mean:>7.2%}"    if not np.isnan(mean)  else "      —"
            ss = f"{std:>7.2%}"     if not np.isnan(std)   else "      —"
            ts = f"({tstat:>5.2f})" if not np.isnan(tstat) else "  (  —)"
            parts.append(f"{ms} {ss} {ts}")
        print(f"{plabel:<12}" + "  ".join(parts))


def _print_spread_only(stats_df, maturities, check_mono=False):
    if "P4_minus_P1" not in stats_df.index:
        print("  (no spread data)")
        return
    if check_mono:
        mono = ["y" if is_monotone(stats_df, f"mean_n{n}") else "n"
                for n in maturities]
        print("  Monotone: " + "  ".join(f"n={n}: {m}"
                                          for n, m in zip(maturities, mono)))
    spread = stats_df.loc["P4_minus_P1"]
    parts  = []
    for n in maturities:
        mean  = spread.get(f"mean_n{n}", np.nan)
        tstat = spread.get(f"t_n{n}",    np.nan)
        ms = f"{mean:>8.2%}"    if not np.isnan(mean)  else "       —"
        ts = f"({tstat:>5.2f})" if not np.isnan(tstat) else "  (  —)"
        parts.append(f"{ms} {ts}")
    print("  P4-P1  " + "  ".join(parts))


def format_table_2(table_dict, output_dir=OUTPUT_DIR):
    """Print to console and save CSV + LaTeX."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 80)
    print("TABLE 2: Sorts Based on the Basis")
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
        panel_df.to_csv(output_dir / f"table2_{panel_name}.csv")
    print(f"\nCSVs saved → {output_dir}")

    _save_table2_latex(table_dict, output_dir)


def _save_table2_latex(table_dict, output_dir):
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Sorts Based on the Basis}",
        r"\label{tab:table2}",
        r"\footnotesize",
        r"\textbf{Panel A: Short Roll Returns} \\",
        r"\begin{tabular}{l" + "rr" * 4 + "}",
        r"\toprule",
        (r" & \multicolumn{2}{c}{$n=1$} & \multicolumn{2}{c}{$n=2$} "
         r"& \multicolumn{2}{c}{$n=3$} & \multicolumn{2}{c}{$n=4$} \\"),
        r" & Mean & Std & Mean & Std & Mean & Std & Mean & Std \\",
        r"\midrule",
    ]

    labels = {"P1": "Low", "P2": "P2", "P3": "P3",
              "P4": "High", "P4_minus_P1": r"$P_4 - P_1$"}
    df = table_dict["panel_a_sr"]

    for pkey, plabel in labels.items():
        if pkey not in df.index:
            continue
        row  = df.loc[pkey]
        vals = []
        for n in [1, 2, 3, 4]:
            mean = row.get(f"mean_n{n}", np.nan)
            std  = row.get(f"std_n{n}",  np.nan)
            vals.append(f"{mean:.2%}" if not np.isnan(mean) else "—")
            vals.append(f"{std:.2%}"  if not np.isnan(std)  else "—")
        lines.append(plabel + " & " + " & ".join(vals) + r" \\")

    if "P4_minus_P1" in df.index:
        spread = df.loc["P4_minus_P1"]
        tstats = [f"({spread.get(f't_n{n}', np.nan):.2f})"
                  if not np.isnan(spread.get(f"t_n{n}", np.nan)) else "—"
                  for n in [1, 2, 3, 4]]
        interleaved = []
        for t in tstats:
            interleaved.extend([t, ""])
        lines.append(r"$t(P_4-P_1)$ & " +
                     " & ".join(interleaved).rstrip(" & ") + r" \\")

    lines += [r"\bottomrule", r"\end{tabular}", ""]

    latex_path = output_dir / "table2.tex"
    latex_path.write_text("\n".join(lines))
    print(f"LaTeX saved → {latex_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Validation targets from paper
# ─────────────────────────────────────────────────────────────────────────────

PAPER_TABLE2_TARGETS = {
    # Panel A Short Roll P4-P1 spreads
    ("panel_a_sr", "P4_minus_P1", 1): -0.0829,
    ("panel_a_sr", "P4_minus_P1", 2): -0.1135,
    ("panel_a_sr", "P4_minus_P1", 3): -0.1351,
    ("panel_a_sr", "P4_minus_P1", 4): -0.1453,
    # Panel A Excess Holding P4-P1 spreads
    ("panel_a_eh", "P4_minus_P1", 2):  0.0061,
    ("panel_a_eh", "P4_minus_P1", 3):  0.0144,
    ("panel_a_eh", "P4_minus_P1", 4):  0.0184,
    # Panel A individual portfolios
    ("panel_a_sr", "P1", 1):  0.0482,
    ("panel_a_sr", "P4", 1): -0.0347,
}


def validate_table_2(table_dict):
    """Compare computed Table 2 values to paper targets."""
    print("\n" + "=" * 70)
    print("VALIDATION: Comparing to Paper's Table 2")
    print("=" * 70)
    print(f"{'Key':<40} {'Paper':>9} {'Computed':>9} {'Diff':>9}")
    print("-" * 70)

    for (panel, portfolio, n), paper_val in PAPER_TABLE2_TARGETS.items():
        df = table_dict.get(panel, pd.DataFrame())
        if df.empty or portfolio not in df.index:
            computed = np.nan
        else:
            computed = df.loc[portfolio, f"mean_n{n}"]

        diff    = computed - paper_val if not np.isnan(computed) else np.nan
        key_str = f"{panel} | {portfolio} | n={n}"
        print(f"{key_str:<40} {paper_val:>9.2%} "
              f"{computed:>9.2%} {diff:>+9.2%}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Building Table 2...")
    table = build_table_2()
    format_table_2(table)
    validate_table_2(table)