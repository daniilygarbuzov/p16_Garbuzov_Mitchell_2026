'''
This script creates a bar chart of the annualized mean returns for the Short Roll strategy across different commodity sectors and maturities, 
based on the data in "table1_short_roll.csv". The resulting chart is saved as "fig_sr_sector_bar.png" in the output directory.
'''
from pathlib import Path
import re

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ------------------------------------------------------------
# 1. Load Table 1 Short Roll CSV
# ------------------------------------------------------------
OUTPUT_DIR = Path("_output")  # adjust if needed
sr_path = OUTPUT_DIR / "table1_short_roll.csv"

sr = pd.read_csv(sr_path, index_col=0)

# Optional: enforce sector order to match your LaTeX table
sector_order = [
    "Energy",
    "Meats",
    "Metals",
    "Grains",
    "Oilseeds",
    "Softs",
    "Ind_Materials",  # or "Ind materials" depending on CSV index
    "EW_All",         # or "EW"
]

# Keep only sectors that actually exist in the CSV (avoid KeyErrors)
sector_order = [s for s in sector_order if s in sr.index]

# ------------------------------------------------------------
# 2. Extract mean_ann_n1..n4 for each sector into a tidy frame
# ------------------------------------------------------------
ns = [1, 2, 3, 4]

data = []
labels_for_plot = []
for sect in sector_order:
    row = sr.loc[sect]
    means = []
    for n in ns:
        col = f"mean_ann_n{n}"
        means.append(row.get(col, float("nan")))
    data.append(means)
    # Pretty label (match Table 1 display)
    if sect == "Ind_Materials":
        labels_for_plot.append("Ind materials")
    elif sect in ("EW_All", "EW"):
        labels_for_plot.append("EW")
    else:
        labels_for_plot.append(sect)

data = np.array(data)  # shape: (n_sectors, 4)

# ------------------------------------------------------------
# 3. Plot grouped bar chart
# ------------------------------------------------------------
n_sectors = len(sector_order)
x = np.arange(n_sectors)  # sector positions on x-axis
width = 0.18              # bar width

fig, ax = plt.subplots(figsize=(10, 4.5))

for i, n in enumerate(ns):
    ax.bar(
        x + (i - 1.5) * width,
        data[:, i],
        width,
        label=f"n={n}",
    )

ax.set_xticks(x)
ax.set_xticklabels(labels_for_plot, rotation=45, ha="right")

ax.set_ylabel("Annualized SR mean return")
ax.set_title("Sector-Level Short Roll Mean Returns by Maturity")
ax.legend(title="Maturity")

ax.grid(axis="y", alpha=0.3)

fig.tight_layout()

png_path = OUTPUT_DIR / "fig_sr_sector_bar.png"
fig.savefig(png_path, dpi=300)
plt.close(fig)

print(f"Saved chart to: {png_path}")
