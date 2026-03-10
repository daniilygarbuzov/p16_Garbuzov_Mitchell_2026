"""
create_table_1.py
-----------------
Replicates Table 1 of Szymanowska et al. (2014):

    "Summary Statistics: Annualized mean returns, standard deviations,
     and t-statistics for Short Roll and Excess Holding returns across
     seven commodity sectors and an equally-weighted index."

Usage
-----
    ipython ./src/create_table_1.py
    # or:
    from create_table_1 import build_table_1
    table = build_table_1()
"""

import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd
import statsmodels.api as sm

from settings import config
from process_futures import (
    build_bimonthly_panel,
    SECTORS,
    CODE_TO_SECTOR,
    CODE_TO_NAME,
)

DATA_DIR   = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")


# ─────────────────────────────────────────────────────────────────────────────
# Statistical helpers
# ─────────────────────────────────────────────────────────────────────────────

def newey_west_stats(series, n_periods=1, n_lags=None):
    """
    Annualized mean, std dev, and Newey-West t-stat for a bimonthly
    return series.

    Annualization:
      - 6 bimonthly periods per year.
      - SR (n=1): multiply mean by 6, std by sqrt(6).
      - EH at maturity n: multiply mean by 6/n, std by sqrt(6/n).

    Parameters
    ----------
    series : pd.Series
        Raw (not annualized) bimonthly returns.
    n_periods : int
        Holding period in bimonthly units (1 for SR, 2/3/4 for EH).
    n_lags : int, optional
        NW lag truncation. Default: max(n_periods-1, floor(T^(1/3))).

    Returns
    -------
    dict: mean_ann, std_ann, t_stat, n_obs
    """
    clean = series.dropna()
    T     = len(clean)

    if T < 5:
        return {"mean_ann": np.nan, "std_ann": np.nan,
                "t_stat": np.nan,  "n_obs": T}

    ann_factor = 6.0 / n_periods
    mean_raw   = clean.mean()
    std_raw    = clean.std(ddof=1)
    mean_ann   = mean_raw * ann_factor
    std_ann    = std_raw  * np.sqrt(ann_factor)

    if n_lags is None:
        overlap_lags  = max(n_periods - 1, 1)
        rule_of_thumb = max(int(np.floor(T ** (1 / 3))), 1)
        n_lags        = max(overlap_lags, rule_of_thumb)

    X = np.ones((T, 1))
    y = clean.values
    try:
        model  = sm.OLS(y, X).fit(cov_type="HAC",
                                   cov_kwds={"maxlags": n_lags})
        se_raw = model.bse[0]
        t_stat = mean_raw / se_raw if se_raw > 0 else np.nan
    except Exception:
        t_stat = np.nan

    return {"mean_ann": mean_ann, "std_ann": std_ann,
            "t_stat": t_stat, "n_obs": T}


# ─────────────────────────────────────────────────────────────────────────────
# Sector index construction
# ─────────────────────────────────────────────────────────────────────────────

def build_sector_ew_returns(returns_df):
    """
    Equally weighted sector-level return indices.

    At each bimonthly date, average log returns across all commodities
    within a sector (equal weight).

    Returns
    -------
    dict
        Keys: sector names + "EW_All"
        Values: pd.DataFrames with obs_date and return columns
    """
    return_cols = ["sr_1", "sr_2", "sr_3", "sr_4",
                   "eh_2", "eh_3", "eh_4"]
    sector_returns = {}

    for sector in SECTORS:
        sub = returns_df[returns_df["sector"] == sector].copy()
        if sub.empty:
            print(f"  WARNING: no data for sector {sector}")
            continue
        agg = (sub.groupby("obs_date")[return_cols]
                  .mean()
                  .reset_index())
        sector_returns[sector] = agg

    agg_all = (returns_df.groupby("obs_date")[return_cols]
                         .mean()
                         .reset_index())
    sector_returns["EW_All"] = agg_all

    return sector_returns


# ─────────────────────────────────────────────────────────────────────────────
# Build Table 1
# ─────────────────────────────────────────────────────────────────────────────

def build_table_1(returns_df=None, data_dir=DATA_DIR):
    """
    Compute Table 1 statistics.

    Returns
    -------
    dict
        "short_roll"     : pd.DataFrame  (Panel A)
        "excess_holding" : pd.DataFrame  (Panel B)
    """
    data_dir = Path(data_dir)

    if returns_df is None:
        panel_path = data_dir / "returns_panel.parquet"
        if panel_path.exists():
            print("Loading pre-built returns panel...")
            returns_df = pd.read_parquet(panel_path)
        else:
            print("Building returns panel from scratch...")
            returns_df = build_bimonthly_panel(data_dir=data_dir)

    print("Building sector EW return indices...")
    sector_returns = build_sector_ew_returns(returns_df)

    # ── Panel A: Short Roll ──
    sr_rows = []
    for sector_label, sector_df in sector_returns.items():
        row = {"sector": sector_label}
        for n in [1, 2, 3, 4]:
            col = f"sr_{n}"
            if col not in sector_df.columns:
                for key in ["mean_ann", "std_ann", "t_stat"]:
                    row[f"{key}_n{n}"] = np.nan
                continue
            stats = newey_west_stats(sector_df[col], n_periods=1)
            row[f"mean_ann_n{n}"] = stats["mean_ann"]
            row[f"std_ann_n{n}"]  = stats["std_ann"]
            row[f"t_stat_n{n}"]   = stats["t_stat"]
        sr_rows.append(row)

    df_sr = pd.DataFrame(sr_rows).set_index("sector")

    # ── Panel B: Excess Holding ──
    eh_rows = []
    for sector_label, sector_df in sector_returns.items():
        row = {"sector": sector_label}
        for n in [2, 3, 4]:
            col = f"eh_{n}"
            if col not in sector_df.columns:
                for key in ["mean_ann", "std_ann", "t_stat"]:
                    row[f"{key}_n{n}"] = np.nan
                continue
            stats = newey_west_stats(sector_df[col], n_periods=n)
            row[f"mean_ann_n{n}"] = stats["mean_ann"]
            row[f"std_ann_n{n}"]  = stats["std_ann"]
            row[f"t_stat_n{n}"]   = stats["t_stat"]
        eh_rows.append(row)

    df_eh = pd.DataFrame(eh_rows).set_index("sector")

    return {"short_roll": df_sr, "excess_holding": df_eh}


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_DISPLAY_NAMES = {
    "Energy"        : "Energy",
    "Meats"         : "Meats",
    "Metals"        : "Metals",
    "Grains"        : "Grains",
    "Oilseeds"      : "Oilseeds",
    "Softs"         : "Softs",
    "Ind_Materials" : "Ind materials",
    "EW_All"        : "EW",
}

SECTOR_ORDER = list(SECTOR_DISPLAY_NAMES.keys())


def _print_panel(df, maturities):
    """Print one panel to console."""
    mat_str = "  ".join(f"  n={n}  " for n in maturities)
    print(f"{'Sector':<18}  {mat_str}")
    print(f"{'':18}  " + "  ".join(["Mean   Std    t    "] * len(maturities)))
    print("-" * 80)

    for sector_key in SECTOR_ORDER:
        if sector_key not in df.index:
            continue
        display = SECTOR_DISPLAY_NAMES.get(sector_key, sector_key)
        row     = df.loc[sector_key]
        parts   = []
        for n in maturities:
            mean  = row.get(f"mean_ann_n{n}", np.nan)
            std   = row.get(f"std_ann_n{n}",  np.nan)
            tstat = row.get(f"t_stat_n{n}",   np.nan)
            ms = f"{mean:>7.2%}"  if not np.isnan(mean)  else "      — "
            ss = f"{std:>7.2%}"   if not np.isnan(std)   else "      — "
            ts = f"({tstat:>5.2f})" if not np.isnan(tstat) else "  (  — )"
            parts.append(f"{ms} {ss} {ts}")
        print(f"{display:<18}  {'  '.join(parts)}")


def _save_latex(df_sr, df_eh, output_dir):
    """Save Table 1 as LaTeX."""
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Summary Statistics}",
        r"\label{tab:table1}",
        r"\footnotesize",
        r"\begin{tabular}{l" + "r" * 12 + "}",
        r"\toprule",
        (r" & \multicolumn{4}{c}{Mean} & \multicolumn{4}{c}{Std Dev} "
         r"& \multicolumn{4}{c}{$t$-stat} \\"),
        (r" & $n=1$ & $n=2$ & $n=3$ & $n=4$ "
         r"& $n=1$ & $n=2$ & $n=3$ & $n=4$ "
         r"& $n=1$ & $n=2$ & $n=3$ & $n=4$ \\"),
        r"\midrule",
        r"\textit{Short Roll} \\",
    ]

    for sector_key in SECTOR_ORDER:
        if sector_key not in df_sr.index:
            continue
        display = SECTOR_DISPLAY_NAMES.get(sector_key, sector_key)
        row     = df_sr.loc[sector_key]
        vals    = []
        for stat in ["mean_ann", "std_ann", "t_stat"]:
            for n in [1, 2, 3, 4]:
                v = row.get(f"{stat}_n{n}", np.nan)
                if np.isnan(v):
                    vals.append("—")
                elif stat == "t_stat":
                    vals.append(f"({v:.2f})")
                else:
                    vals.append(f"{v:.2%}")
        lines.append(f"{display} & " + " & ".join(vals) + r" \\")

    lines += [r"\addlinespace", r"\textit{Excess Holding} \\"]

    for sector_key in SECTOR_ORDER:
        if sector_key not in df_eh.index:
            continue
        display    = SECTOR_DISPLAY_NAMES.get(sector_key, sector_key)
        row        = df_eh.loc[sector_key]
        vals_mean, vals_std, vals_t = [], [], []
        for n in [2, 3, 4]:
            for stat, lst in [("mean_ann", vals_mean),
                               ("std_ann",  vals_std),
                               ("t_stat",   vals_t)]:
                v = row.get(f"{stat}_n{n}", np.nan)
                if np.isnan(v):
                    lst.append("—")
                elif stat == "t_stat":
                    lst.append(f"({v:.2f})")
                else:
                    lst.append(f"{v:.2%}")
        # n=1 blank for EH
        vals = (["—"] + vals_mean + ["—"] + vals_std + ["—"] + vals_t)
        lines.append(f"{display} & " + " & ".join(vals) + r" \\")

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}"]

    latex_path = output_dir / "table1.tex"
    latex_path.write_text("\n".join(lines))
    print(f"LaTeX saved → {latex_path}")


def format_table_1(table_dict, output_dir=OUTPUT_DIR):
    """Print to console and save CSV + LaTeX."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df_sr = table_dict["short_roll"]
    df_eh = table_dict["excess_holding"]

    print("\n" + "=" * 80)
    print("TABLE 1: Summary Statistics")
    print("Annualized Mean Returns, Standard Deviations, and t-Statistics")
    print("Sample: March 1986 – December 2010 (bimonthly)")
    print("=" * 80)

    print("\nPanel A: Short Roll Returns (n=1,2,3,4)")
    print("-" * 80)
    _print_panel(df_sr, maturities=[1, 2, 3, 4])

    print("\nPanel B: Excess Holding Returns (n=2,3,4)")
    print("-" * 80)
    _print_panel(df_eh, maturities=[2, 3, 4])

    df_sr.to_csv(output_dir / "table1_short_roll.csv")
    df_eh.to_csv(output_dir / "table1_excess_holding.csv")
    print(f"\nCSVs saved → {output_dir}")

    _save_latex(df_sr, df_eh, output_dir)


# ─────────────────────────────────────────────────────────────────────────────
# Validation targets from paper
# ─────────────────────────────────────────────────────────────────────────────

PAPER_TABLE1_TARGETS = {
    # (sector, return_type, n): (mean_ann, t_stat)
    ("Energy",        "sr", 1): ( 0.1083,  1.64),
    ("Meats",         "sr", 1): ( 0.0420,  1.60),
    ("Metals",        "sr", 1): ( 0.0542,  1.57),
    ("Grains",        "sr", 1): (-0.0610, -1.60),
    ("Oilseeds",      "sr", 1): ( 0.0186,  0.44),
    ("Softs",         "sr", 1): (-0.0658, -1.77),
    ("Ind_Materials", "sr", 1): (-0.0482, -1.22),
    ("EW_All",        "sr", 1): ( 0.0065,  0.27),
    ("EW_All",        "eh", 2): ( 0.0073,  3.01),
    ("EW_All",        "eh", 3): ( 0.0108,  2.58),
    ("EW_All",        "eh", 4): ( 0.0277,  3.21),
}


def validate_table_1(table_dict):
    """Compare computed values to paper's Table 1."""
    print("\n" + "=" * 70)
    print("VALIDATION: Comparing to Paper's Table 1")
    print("=" * 70)
    print(f"{'Key':<35} {'Paper':>10} {'Computed':>10} {'Diff':>10}")
    print("-" * 70)

    df_sr = table_dict["short_roll"]
    df_eh = table_dict["excess_holding"]

    for (sector, rtype, n), (paper_mean, paper_t) in PAPER_TABLE1_TARGETS.items():
        df = df_sr if rtype == "sr" else df_eh
        if sector not in df.index:
            computed_mean = np.nan
        else:
            computed_mean = df.loc[sector, f"mean_ann_n{n}"]

        diff    = computed_mean - paper_mean if not np.isnan(computed_mean) else np.nan
        key_str = f"{sector} | {rtype}_{n}"
        print(f"{key_str:<35} {paper_mean:>10.2%} "
              f"{computed_mean:>10.2%} {diff:>+10.2%}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Building Table 1...")
    table = build_table_1()
    format_table_1(table)
    validate_table_1(table)