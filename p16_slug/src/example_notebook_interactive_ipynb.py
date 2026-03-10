# %% [markdown]
# # Szymanowska et al. (2014) – Table 1 Summary
#
# This notebook-style script:
#
# 1. Loads the bimonthly futures return panel used in the project.
# 2. Reconstructs **Table 1**:
#    - Panel A: Short Roll returns (SR, maturities n = 1,2,3,4)
#    - Panel B: Excess Holding returns (EH, maturities n = 2,3,4)
# 3. Aggregates the two panels into a single table for quick inspection.
# 4. Explains the **math** of how SR and EH returns and their
#    annualized moments are computed.

# %%
import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd

from settings import config
from create_table_1 import build_table_1, newey_west_stats

DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

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
# ## 2. Aggregate Panel A (Short Roll) and Panel B (Excess Holding)
#
# We want a single view with Short Roll on top and Excess Holding underneath.
# The underlying data are:
#
# - **Short Roll (SR)**: series `sr_n` for n = 1,2,3,4.
# - **Excess Holding (EH)**: series `eh_n` for n = 2,3,4.
#
# In both cases the values in `df_sr` and `df_eh` are already **annualized**
# using bimonthly data (6 bimonthly periods per year).

# %%
# Add a panel label so we can stack and still identify the type
sr_long = (
    df_sr.reset_index()
    .assign(panel="Short Roll")
)

eh_long = (
    df_eh.reset_index()
    .assign(panel="Excess Holding")
)

# For a compact “Panel A then Panel B” view, we just append:
table1_combined = pd.concat([sr_long, eh_long], ignore_index=True)

# Sort by panel and sector to match paper-style ordering
table1_combined = table1_combined.sort_values(["panel", "sector"])

table1_combined

# %% [markdown]
# ### 2.1 Save aggregated table
#
# We save a CSV that contains both panels, with:
# - `panel` ∈ {Short Roll, Excess Holding}
# - `sector` (Energy, Meats, Metals, Grains, Oilseeds, Softs, Ind_Materials, EW_All)
# - for each maturity n, the columns:
#   - `mean_ann_n{n}`
#   - `std_ann_n{n}`
#   - `t_stat_n{n}`

# %%
OUTPUT_DIR = Path(OUTPUT_DIR)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

agg_path = OUTPUT_DIR / "table1_aggregated_sr_eh.csv"
table1_combined.to_csv(agg_path, index=False)
agg_path

# %% [markdown]
# ## 3. How the returns are computed – intuition
#
# Let:
# - \( S_t \) be the spot price at time \( t \),
# - \( F_t^{(n)} \) be the **n-maturity** futures price at time \( t \),
# - bimonthly dates \( t = 0, 1, 2, \dots \) (6 per year).
#
# The paper decomposes futures returns into:
#
# - **Spot premia** via a rolling strategy in nearby contracts (Short Roll),
# - **Term premia** via holding contracts to maturity vs rolling (Excess Holding). 

# %% [markdown]
# ### 3.1 Short Roll returns (SR)
#
# For a given maturity horizon \( n \) (in months, mapped to bimonthly units in the code),
# define a strategy that **rolls** short-maturity futures contracts instead of
# holding one contract to maturity.
#
# At a high level, the (log) Short Roll return from date t to t+Δt is:
#
# \[
# r^{\text{SR},n}_{t \to t+\Delta}
#   = \ln F_{t+\Delta}^{(n)} - \ln F_t^{(n)}
# \]
#
# when you roll the contract forward at each observation, always staying at the
# same relative maturity \( n \).[file:41]
#
# In the implementation:
# - `sr_1, sr_2, sr_3, sr_4` are such log returns for different horizons.
# - They are **bimonthly** returns (Δ = 2 months), stored in `returns_df`.

# %% [markdown]
# ### 3.2 Excess Holding returns (EH)
#
# Excess Holding returns compare **holding** a contract to maturity to the
# Short Roll strategy. Intuitively, you:
#
# - Go long a contract and hold it until maturity,
# - Compare that payoff to the sequence of reinvested Short Roll positions.
#
# The EH return for maturity \( n \) (in bimonthly units) can be written as:
#
# \[
# r^{\text{EH},n}_{t \to t+n}
#   = r^{\text{Hold},n}_{t \to t+n} - r^{\text{SR},n}_{t \to t+n}
# \]
#
# where \( r^{\text{Hold},n} \) is the log return from buying the \( n \)-maturity
# contract at \( t \) and holding to \( t+n \).[file:41]
#
# In the data:
# - `eh_2, eh_3, eh_4` capture these excess holding returns for horizons
#   corresponding to 4, 6, and 8 months (2, 3, 4 bimonthly periods).

# %% [markdown]
# ## 4. Annualization and Newey–West adjustment
#
# The helper `newey_west_stats(series, n_periods)` computes:
#
# - `mean_ann`: annualized mean of the bimonthly returns,
# - `std_ann`: annualized standard deviation,
# - `t_stat`: Newey–West t-statistic for the **unannualized** mean.
#
# With bimonthly data, there are 6 periods per year. For a holding period
# of `n_periods` bimonthly intervals:
#
# - Annualization factor for the **mean**:
#
# \[
# \text{ann\_factor} = \frac{6}{n_{\text{periods}}}
# \quad\Rightarrow\quad
# \mu_{\text{ann}} = \mu_{\text{raw}} \times \text{ann\_factor}
# \]
#
# - Annualization factor for the **standard deviation** (square-root rule):
#
# \[
# \sigma_{\text{ann}}
#   = \sigma_{\text{raw}} \times \sqrt{\text{ann\_factor}}
# \]
#
# For example:
# - Short Roll (SR) uses `n_periods = 1`, so `ann_factor = 6`.
# - Excess Holding with maturity n uses `n_periods = n`, so
#   `ann_factor = 6 / n`.[file:41]

# %% [markdown]
# ### 4.1 Newey–West t-stat for serial correlation
#
# The Newey–West t-statistic corrects for autocorrelation and heteroskedasticity
# in the bimonthly return series.
#
# Given a return series \( r_t \) for \( t = 1,\dots,T \), the code:
#
# 1. Runs an OLS regression:
#
# \[
# r_t = \alpha + \varepsilon_t
# \]
#
# 2. Uses a HAC (Newey–West) covariance estimator with a lag length
#    chosen as:
#    - at least `n_periods - 1` (to account for overlapping EH returns),
#    - or a rule-of-thumb \( \lfloor T^{1/3} \rfloor \), whichever is larger.
#
# 3. Forms:
#
# \[
# t_{\text{NW}} = \frac{\hat{\alpha}}{\text{SE}_{\text{NW}}(\hat{\alpha})}
# \]
#
# This t-statistic is reported in the `t_stat` columns for both SR and EH.

# %% [markdown]
# ## 5. Quick sanity check on one sector
#
# As a simple check, we can recompute the stats for one sector manually
# using `newey_west_stats` and compare to the stored values.

# %%
sector = "EW_All"  # equally-weighted across all commodities

# Rebuild stats for EW_All Short Roll n=1
sr_series = df_sr.loc[sector]
sr_n1_mean = sr_series["mean_ann_n1"]
sr_n1_std = sr_series["std_ann_n1"]
sr_n1_t = sr_series["t_stat_n1"]

sr_n1_mean, sr_n1_std, sr_n1_t

# %%
# The actual bimonthly series can be accessed from the returns panel if needed:
panel_path = Path(DATA_DIR) / "returns_panel.parquet"
if panel_path.exists():
    returns_df = pd.read_parquet(panel_path)
    ew_all_sr1 = (
        returns_df.groupby("obs_date")["sr_1"]
        .mean()
        .dropna()
    )
    stats_manual = newey_west_stats(ew_all_sr1, n_periods=1)
    stats_manual
else:
    print("returns_panel.parquet not found; cannot run manual check here.")
