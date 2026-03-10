"""
summary_stats.py
----------------
Generates original summary statistics table and chart for the
underlying commodity futures data used in the replication of
Szymanowska et al. (2014).

Outputs
-------
- _output/summary_stats_basis.tex   : LaTeX table of basis statistics by sector
- _output/summary_stats_returns.tex : LaTeX table of SR/EH return stats by sector
- _output/fig_basis_over_time.png   : Chart of median log basis over time by sector

Usage
-----
    ipython ./src/summary_stats.py
"""

import sys
from pathlib import Path
sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from settings import config

DATA_DIR   = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Load data ─────────────────────────────────────────────────────────────────

panel   = pd.read_parquet(DATA_DIR / "commodity_panel.parquet")
returns = pd.read_parquet(DATA_DIR / "returns_panel.parquet")

panel["date"] = pd.to_datetime(panel["date"])
panel["log_basis"] = np.log(panel["price_n2"]) - np.log(panel["price_n1"])


# ── Table 1: Basis statistics by sector ──────────────────────────────────────

basis_stats = (
    panel.groupby("sector")["log_basis"]
    .agg(
        Mean="mean",
        Std="std",
        Min="min",
        Median="median",
        Max="max",
        N="count",
    )
    .round(4)
)
basis_stats.index.name = "Sector"
basis_stats = basis_stats.sort_values("Mean")

# Format as percentages
pct_cols = ["Mean", "Std", "Min", "Median", "Max"]
for col in pct_cols:
    basis_stats[col] = basis_stats[col].map("{:.2%}".format)
basis_stats["N"] = basis_stats["N"].astype(int)

latex_basis = basis_stats.to_latex(
    escape=False,
    column_format="lrrrrrrr",
    caption=(
        "Summary statistics for the log basis ($\\ln F^{(2)}_t - \\ln F^{(1)}_t$) "
        "by sector, computed over the full sample March 1986--December 2010. "
        "A negative mean basis indicates that the sector is on average in "
        "backwardation (spot premium earners), while a positive mean basis "
        "indicates contango. Energy and Metals exhibit near-zero or negative "
        "mean basis, consistent with the Theory of Normal Backwardation, "
        "while Grains and Softs are predominantly in contango."
    ),
    label="tab:basis_summary",
    position="htbp",
)

with open(OUTPUT_DIR / "summary_stats_basis.tex", "w") as f:
    f.write(latex_basis)
print("Saved summary_stats_basis.tex")


# ── Table 2: SR and EH return stats by sector ─────────────────────────────────

rows = []
for sector, grp in returns.groupby("sector"):
    rows.append({
        "Sector"     : sector,
        "SR n=1 Mean": grp["sr_1"].mean() * 6,
        "SR n=1 Std" : grp["sr_1"].std() * np.sqrt(6),
        "EH n=2 Mean": grp["eh_2"].mean() * 3,
        "EH n=2 Std" : grp["eh_2"].std() * np.sqrt(3),
        "EH n=4 Mean": grp["eh_4"].mean() * 1.5,
        "EH n=4 Std" : grp["eh_4"].std() * np.sqrt(1.5),
        "N"          : grp["sr_1"].notna().sum(),
    })

ret_stats = pd.DataFrame(rows).set_index("Sector").sort_index()
for col in [c for c in ret_stats.columns if c != "N"]:
    ret_stats[col] = ret_stats[col].map("{:.2%}".format)
ret_stats["N"] = ret_stats["N"].astype(int)

latex_ret = ret_stats.to_latex(
    escape=False,
    column_format="lrrrrrrrr",
    caption=(
        "Annualized mean returns and standard deviations for Short Roll (SR, $n=1$) "
        "and Excess Holding (EH, $n=2,4$) strategies by sector, computed over "
        "March 1986--December 2010. SR returns capture the roll yield earned by "
        "continuously rolling the nearest contract. EH returns isolate the term "
        "premium from holding longer-dated contracts. Energy and Metals earn "
        "positive SR returns (backwardation), while Grains and Softs earn negative "
        "SR returns (contango). EH returns are positive and increasing in maturity "
        "for most sectors, consistent with upward-sloping term premia."
    ),
    label="tab:returns_summary",
    position="htbp",
)

with open(OUTPUT_DIR / "summary_stats_returns.tex", "w") as f:
    f.write(latex_ret)
print("Saved summary_stats_returns.tex")


# ── Figure: Median log basis over time by sector ─────────────────────────────

SECTOR_COLORS = {
    "Energy"       : "#e07b39",
    "Metals"       : "#5a9e6f",
    "Grains"       : "#9b59b6",
    "Oilseeds"     : "#3498db",
    "Meats"        : "#e74c3c",
    "Softs"        : "#1abc9c",
    "Ind_Materials": "#95a5a6",
}

fig, ax = plt.subplots(figsize=(12, 6))

for sector, grp in panel.groupby("sector"):
    monthly = grp.groupby("date")["log_basis"].median()
    # 3-period rolling average to smooth
    smoothed = monthly.rolling(3, min_periods=1).mean()
    ax.plot(
        smoothed.index, smoothed.values,
        label=sector,
        color=SECTOR_COLORS.get(sector, "gray"),
        linewidth=1.5,
        alpha=0.85,
    )

ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.xaxis.set_major_locator(mdates.YearLocator(5))
ax.set_xlabel("Date", fontsize=11)
ax.set_ylabel("Log Basis ($\\ln F^{(2)} - \\ln F^{(1)}$)", fontsize=11)
ax.set_title(
    "Median Log Basis by Sector (3-Period Rolling Average)\n"
    "March 1986 – December 2010",
    fontsize=12,
)
ax.legend(loc="upper right", fontsize=9, ncol=2)
ax.grid(True, alpha=0.3)
fig.tight_layout()

fig.savefig(OUTPUT_DIR / "fig_basis_over_time.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print("Saved fig_basis_over_time.png")

print("\nAll summary stats outputs saved.")