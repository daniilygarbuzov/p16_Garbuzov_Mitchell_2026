"""
process_futures.py
------------------
Core data processing for commodity futures replication.

Reads the cleaned bimonthly panel produced by build_clean_data.py
(commodity_panel.parquet) and computes Short Roll and Excess Holding
returns needed for Tables 1 and 2.

Usage
-----
    ipython ./src/process_futures.py
    from process_futures import build_bimonthly_panel
"""

import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd

from settings import config

DATA_DIR   = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Commodity → sector mapping  (verified via WRDS JupyterHub query)
# ─────────────────────────────────────────────────────────────────────────────

COMMODITY_META = {
    # product_code : (name, sector)
    1986: ("crude_oil",     "Energy"),
    2091: ("gasoline",      "Energy"),
    2029: ("heating_oil",   "Energy"),
    2020: ("gold",          "Metals"),
    2026: ("copper",        "Metals"),
    2108: ("silver",        "Metals"),
    3247: ("corn",          "Grains"),
    430:  ("wheat",         "Grains"),
    385:  ("oats",          "Grains"),
    379:  ("rough_rice",    "Grains"),
    3256: ("soybean_meal",  "Oilseeds"),
    282:  ("soybean_oil",   "Oilseeds"),
    396:  ("soybeans",      "Oilseeds"),
    3250: ("feeder_cattle", "Meats"),
    2675: ("live_cattle",   "Meats"),
    2676: ("lean_hogs",     "Meats"),
    1980: ("cocoa",         "Softs"),
    2038: ("coffee",        "Softs"),
    2036: ("orange_juice",  "Softs"),
    1992: ("cotton",        "Ind_Materials"),
    361:  ("lumber",        "Ind_Materials"),
}

CODE_TO_NAME   = {code: name   for code, (name, sector) in COMMODITY_META.items()}
CODE_TO_SECTOR = {code: sector for code, (name, sector) in COMMODITY_META.items()}
NAME_TO_CODE   = {name: code   for code, (name, sector) in COMMODITY_META.items()}
NAME_TO_SECTOR = {name: sector for code, (name, sector) in COMMODITY_META.items()}

SECTORS = ["Energy", "Meats", "Metals", "Grains",
           "Oilseeds", "Softs", "Ind_Materials"]


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Compute Short Roll and Excess Holding returns
# ─────────────────────────────────────────────────────────────────────────────

def compute_returns(wide):
    """
    Compute bimonthly Short Roll and Excess Holding returns.

    Input: wide DataFrame with columns
        obs_date, product_code, lp_1, lp_2, lp_3, lp_4, basis

    Short Roll (period t):
        r_SR[t] = lp_1[t+1] - lp_2[t]
        (buy rank-2 contract at t; rolls to rank-1 by t+1)

    Holding (n periods):
        r_H[t,n] = lp_1[t+n] - lp_{n+1}[t]

    Excess Holding (n periods):
        r_EH[t,n] = r_H[t,n] - sum(r_SR[t+j] for j=1..n)

    Returns
    -------
    pd.DataFrame
        Columns: obs_date, product_code, commodity, sector,
                 sr_1..sr_4, eh_2..eh_4, basis
    """
    wide = wide.copy().sort_values(["product_code", "obs_date"])
    result_records = []

    for prod, grp in wide.groupby("product_code"):
        grp = grp.sort_values("obs_date").reset_index(drop=True)
        T   = len(grp)

        for i in range(T - 4):
            t_row  = grp.iloc[i]
            t1_row = grp.iloc[i + 1]
            obs_t  = t_row["obs_date"]

            # ── Short Roll at maturity n ──
            # sr_n[t] = lp_n[t+1] - lp_{n+1}[t]
            # Buy rank-(n+1) contract at t, sell as rank-n one period later.
            # sr_1: buy rank-2, sell as rank-1  (nearest-contract roll)
            # sr_2: buy rank-3, sell as rank-2
            # sr_3: buy rank-4, sell as rank-3
            # sr_4: buy rank-5, sell as rank-4  (needs lp_5 → NaN if unavailable)
            t1_row = grp.iloc[i + 1]
            sr = {}
            for n in [1, 2, 3, 4]:
                lp_n_next = t1_row.get(f"lp_{n}",    np.nan)
                lp_n1_now = t_row.get(f"lp_{n + 1}", np.nan)
                sr[n] = (lp_n_next - lp_n1_now
                         if not (pd.isna(lp_n_next) or pd.isna(lp_n1_now))
                         else np.nan)

            # ── Holding return: r_H[t,n] = lp_1[t+n] - lp_{n+1}[t] ──
            # Buy rank-(n+1) contract at t, hold n periods to spot.
            holding = {}
            for n in [2, 3, 4]:
                lp1_future = grp.iloc[i + n].get("lp_1",    np.nan)
                lpn1_now   = t_row.get(f"lp_{n + 1}",       np.nan)
                holding[n] = (lp1_future - lpn1_now
                              if not (pd.isna(lp1_future) or pd.isna(lpn1_now))
                              else np.nan)

            # ── Excess Holding: r_EH[t,n] = r_H[t,n] - sum of SR legs ──
            # EH(n) = H(n) - [sr_1[t] + sr_1[t+1] + ... + sr_1[t+n-1]]
            # i.e. subtract the n sequential nearest-contract rolls that
            # replicate the holding strategy via the term structure.
            # Each leg j uses sr_1 at time t+j-1: lp_1[t+j] - lp_2[t+j-1]
            def _eh(n):
                h = holding[n]
                if pd.isna(h):
                    return np.nan
                sr1_legs = []
                for j in range(n):          # j = 0..n-1  →  period t+j
                    row_j    = grp.iloc[i + j]
                    row_j1   = grp.iloc[i + j + 1]
                    lp1_next = row_j1.get("lp_1", np.nan)
                    lp2_now  = row_j.get("lp_2",  np.nan)
                    if pd.isna(lp1_next) or pd.isna(lp2_now):
                        return np.nan
                    sr1_legs.append(lp1_next - lp2_now)
                return h - sum(sr1_legs)

            result_records.append({
                "obs_date"    : obs_t,
                "product_code": prod,
                "sr_1"        : sr[1],
                "sr_2"        : sr[2],
                "sr_3"        : sr[3],
                "sr_4"        : sr[4],
                "eh_2"        : _eh(2),
                "eh_3"        : _eh(3),
                "eh_4"        : _eh(4),
                "basis"       : t_row.get("basis", np.nan),
            })

    returns_df = pd.DataFrame(result_records)
    if returns_df.empty:
        return returns_df
    returns_df["commodity"] = returns_df["product_code"].map(CODE_TO_NAME)
    returns_df["sector"]    = returns_df["product_code"].map(CODE_TO_SECTOR)
    return returns_df


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def build_bimonthly_panel(data_dir=DATA_DIR, output_dir=OUTPUT_DIR,
                           save=True):
    """
    Read commodity_panel.parquet (from build_clean_data.py) and compute
    returns panel.

    commodity_panel has columns:
        date, commodity, sector, price_n1, price_n2, price_n3, price_n4

    Saves returns_panel.parquet to data_dir.
    """
    data_dir   = Path(data_dir)
    output_dir = Path(output_dir)

    # commodity_panel.parquet may be in _data/ or _output/
    for candidate in [data_dir   / "commodity_panel.parquet",
                      output_dir / "commodity_panel.parquet"]:
        if candidate.exists():
            panel_path = candidate
            break
    else:
        raise FileNotFoundError(
            "commodity_panel.parquet not found in _data/ or _output/. "
            "Run build_clean_data.py first."
        )

    print(f"Loading cleaned panel from {panel_path} ...")
    df = pd.read_parquet(panel_path)
    print(f"  Shape    : {df.shape}")
    print(f"  Columns  : {df.columns.tolist()}")
    print(f"  Dates    : {df['date'].min()} to {df['date'].max()}")
    print(f"  Comms    : {sorted(df['commodity'].unique())}")

    df["date"] = pd.to_datetime(df["date"])

    # Map commodity name → product_code
    df["product_code"] = df["commodity"].map(NAME_TO_CODE)
    unmapped = df[df["product_code"].isna()]["commodity"].unique()
    if len(unmapped):
        print(f"  WARNING: unmapped commodities (will be dropped): {unmapped}")
    df = df.dropna(subset=["product_code"])
    df["product_code"] = df["product_code"].astype(int)

    # Log prices from price_n1..price_n5
    for n in [1, 2, 3, 4, 5]:
        col = f"price_n{n}"
        if col in df.columns:
            df[f"lp_{n}"] = np.log(df[col].clip(lower=1e-6))
        else:
            df[f"lp_{n}"] = np.nan

    # basis = lp_2 - lp_1  (positive = contango, negative = backwardation)
    df["basis"] = df["lp_2"] - df["lp_1"]

    # Rename date -> obs_date for compute_returns
    wide = df.rename(columns={"date": "obs_date"})

    print(f"  wide columns : {wide.columns.tolist()}")
    print(f"  lp_5 non-null: {wide['lp_5'].notna().sum() if 'lp_5' in wide.columns else 'MISSING'}")
    # Spot-check one commodity
    sample = wide[wide['product_code'] == 3247][['obs_date','lp_4','lp_5']].head(5)
    print(f"  corn lp_4/lp_5 sample:\n{sample.to_string()}")
    print("Computing returns...")
    returns = compute_returns(wide)

    if returns.empty:
        print("  ERROR: returns DataFrame is empty.")
        print("  Check that commodity names in commodity_panel match NAME_TO_CODE.")
        print(f"  NAME_TO_CODE keys: {sorted(NAME_TO_CODE.keys())}")
        return returns

    print(f"  Returns shape : {returns.shape}")
    print(f"  Date range    : {returns['obs_date'].min().date()} to {returns['obs_date'].max().date()}")
    print(f"  Commodities   : {returns['commodity'].nunique()}")
    print(f"  Missing sr_1  : {returns['sr_1'].isna().sum()}")

    if save:
        out_path = data_dir / "returns_panel.parquet"
        returns.to_parquet(out_path, index=False)
        print(f"  Saved -> {out_path}")

    return returns


if __name__ == "__main__":
    panel = build_bimonthly_panel()
    if panel.empty:
        print("Empty panel — check warnings above.")
    else:
        print("\nSample output:")
        print(panel.head(10).to_string())
        print("\nRaw bimonthly return stats:")
        for col in ["sr_1", "sr_2", "sr_3", "sr_4", "eh_2", "eh_3", "eh_4"]:
            print(f"  {col}: mean={panel[col].mean():.4f}, "
                  f"std={panel[col].std():.4f}, "
                  f"n={panel[col].notna().sum()}")