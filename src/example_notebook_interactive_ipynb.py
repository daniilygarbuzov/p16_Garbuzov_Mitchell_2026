# %% [markdown]
# # Szymanowska et al. (2014) – Table 1 and Table 2 Tour
#
# This notebook script:
#
# 1. Loads the bimonthly futures return panel used in the project.
# 2. Reconstructs **Table 1**:
#    - Panel A: Short Roll returns (SR, maturities n = 1,2,3,4)
#    - Panel B: Excess Holding returns (EH, maturities n = 2,3,4)
# 3. Aggregates and combines original and extended Table 1 outputs.
# 4. Combines original and extended **Table 2** outputs into a single view.
# 5. Explains the math of how SR and EH returns and their annualized
#    moments are computed.

# %%
import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd
from IPython.display import display

from settings import config
from create_table_1 import build_table_1, newey_west_stats

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

# %% [markdown]
# ## 1. Build Table 1 data
#
# We reuse the existing `build_table_1` function, which:
# - loads or builds the bimonthly futures panel (`returns_panel.parquet`),
# - constructs sector-level equal-weighted indices,
# - computes annualized mean, standard deviation, and Newey–West t-statistics
#   for SR and EH returns.

# %%
table1 = build_table_1()
df_sr = table1["short_roll"].copy()
df_eh = table1["excess_holding"].copy()

display(df_sr)
display(df_eh)

# %% [markdown]
# ### 1.1 Comments on Table 1 data
#
# We noticed a difference from the values in the table of approx. 1 to 2 %.
# This is significant, but given the vague nature of the paper we had to make many assumptions around approximating.
# Those assumptions include when to roll over, how often to sample, and which specific commodities contracts to use.

# %% [markdown]
# ## 2. Aggregate Panel A (Short Roll) and Panel B (Excess Holding)
#
# Here are the Short Roll and Excess Holding panels stacked on top of each other.

# %%
sr_long = (
    df_sr.reset_index()
    .assign(panel="Short Roll")
)

eh_long = (
    df_eh.reset_index()
    .assign(panel="Excess Holding")
)

table1_combined = pd.concat([sr_long, eh_long], ignore_index=True)

# Explicit panel order: Short Roll (0), Excess Holding (1)
panel_order = {"Short Roll": 0, "Excess Holding": 1}
table1_combined["panel_order"] = table1_combined["panel"].map(panel_order)

table1_combined = (
    table1_combined
    .sort_values(["panel_order", "sector"])
    .drop(columns="panel_order")
)

display(table1_combined)

# %% [markdown]
# ## 3. Table 1 – extended sample only
#
# Here we look at the Table 1 outputs using only the extended sample
# (e.g. 1986–2026). We keep the same panel labels as before and order
# Short Roll above Excess Holding.

# %%
# Load extended Table 1 CSVs
t1_sr_ext_only = pd.read_csv(OUTPUT_DIR / "table1_short_roll_extended.csv")
t1_eh_ext_only = pd.read_csv(OUTPUT_DIR / "table1_excess_holding_extended.csv")

# Label panels
t1_sr_ext_only["panel"] = "Short Roll"
t1_eh_ext_only["panel"] = "Excess Holding"

# Stack
t1_ext_only = pd.concat(
    [t1_sr_ext_only, t1_eh_ext_only],
    ignore_index=True,
)

# Explicit panel order: Short Roll (0), Excess Holding (1)
panel_order = {"Short Roll": 0, "Excess Holding": 1}
t1_ext_only["panel_order"] = t1_ext_only["panel"].map(panel_order)

# Sort: Short Roll rows above Excess Holding, optional sector grouping
sort_cols = [c for c in ["panel_order", "sector"] if c in t1_ext_only.columns]
t1_ext_only = (
    t1_ext_only
    .sort_values(sort_cols)
    .drop(columns="panel_order")
)

display(t1_ext_only)

# Save extended-sample-only Table 1
t1_ext_only_path = OUTPUT_DIR / "table1_extended_only_all_sectors.csv"
t1_ext_only.to_csv(t1_ext_only_path, index=False)
t1_ext_only_path

# %% [markdown]
# ### 3.1 Table 1 extended sample
#
# This is Table 1 extended to the present

# %% [markdown]
# ## 3. Table 2
#
# Here we collect our generated Table 2 outputs into a single table

# %%
# Original-sample Table 2 CSVs
t2_pa_sr_orig  = pd.read_csv(OUTPUT_DIR / "table2_panel_a_sr.csv")
t2_pa_eh_orig  = pd.read_csv(OUTPUT_DIR / "table2_panel_a_eh.csv")
t2_pb1_sr_orig = pd.read_csv(OUTPUT_DIR / "table2_panel_b1_sr.csv")
t2_pb1_eh_orig = pd.read_csv(OUTPUT_DIR / "table2_panel_b1_eh.csv")
t2_pb2_sr_orig = pd.read_csv(OUTPUT_DIR / "table2_panel_b2_sr.csv")
t2_pb2_eh_orig = pd.read_csv(OUTPUT_DIR / "table2_panel_b2_eh.csv")

def label_t2(df, panel, strat):
    df = df.copy()
    df["panel"] = panel       # "A", "B1", "B2"
    df["strategy"] = strat    # "SR" or "EH"
    return df

t2_orig_frames = [
    label_t2(t2_pa_sr_orig,  "A",  "SR"),
    label_t2(t2_pa_eh_orig,  "A",  "EH"),
    label_t2(t2_pb1_sr_orig, "B1", "SR"),
    label_t2(t2_pb1_eh_orig, "B1", "EH"),
    label_t2(t2_pb2_sr_orig, "B2", "SR"),
    label_t2(t2_pb2_eh_orig, "B2", "EH"),
]

t2_orig = pd.concat(t2_orig_frames, ignore_index=True)

# Strategy order: SR (0), EH (1)
strategy_order = {"SR": 0, "EH": 1}
t2_orig["strategy_order"] = t2_orig["strategy"].map(strategy_order)
t2_orig = (
    t2_orig
    .sort_values(["strategy_order", "panel"])  # all SR panels first, then all EH panels
    .drop(columns="strategy_order")
)

display(t2_orig)

# Save original-sample combined Table 2
t2_orig_path = OUTPUT_DIR / "table2_original_all_panels.csv"
t2_orig.to_csv(t2_orig_path, index=False)
t2_orig_path

# %% [markdown]
# ### 3.1 Table 2 Notes
#
# When running unit tests on Table 2, we noticed significant differences compared to the original table.
# These include differences in the amplitudes, and signs of some of the values.
# While we attempted many changes to address this issue, ultimately we were not able to resolve it.
# We suspect that the issue may be due to differences in the underlying data between ourselves and the research team.


# %% [markdown]
# ## 4. Combined Table 2 (extended sample only)
#
# Now we build an analogous combined table using only the extended-sample
# Table 2 CSVs.

# %%
# Extended-sample Table 2 CSVs
t2_pa_sr_ext  = pd.read_csv(OUTPUT_DIR / "table2_panel_a_sr_extended.csv")
t2_pa_eh_ext  = pd.read_csv(OUTPUT_DIR / "table2_panel_a_eh_extended.csv")
t2_pb1_sr_ext = pd.read_csv(OUTPUT_DIR / "table2_panel_b1_sr_extended.csv")
t2_pb1_eh_ext = pd.read_csv(OUTPUT_DIR / "table2_panel_b1_eh_extended.csv")
t2_pb2_sr_ext = pd.read_csv(OUTPUT_DIR / "table2_panel_b2_sr_extended.csv")
t2_pb2_eh_ext = pd.read_csv(OUTPUT_DIR / "table2_panel_b2_eh_extended.csv")

t2_ext_frames = [
    label_t2(t2_pa_sr_ext,  "A",  "SR"),
    label_t2(t2_pa_eh_ext,  "A",  "EH"),
    label_t2(t2_pb1_sr_ext, "B1", "SR"),
    label_t2(t2_pb1_eh_ext, "B1", "EH"),
    label_t2(t2_pb2_sr_ext, "B2", "SR"),
    label_t2(t2_pb2_eh_ext, "B2", "EH"),
]

t2_ext = pd.concat(t2_ext_frames, ignore_index=True)

t2_ext["strategy_order"] = t2_ext["strategy"].map(strategy_order)
t2_ext = (
    t2_ext
    .sort_values(["strategy_order", "panel"])  # all SR panels first, then all EH panels
    .drop(columns="strategy_order")
)

display(t2_ext)

# Save extended-sample combined Table 2
t2_ext_path = OUTPUT_DIR / "table2_extended_all_panels.csv"
t2_ext.to_csv(t2_ext_path, index=False)
t2_ext_path
