"""
build_clean_data_extended.py
-------------------
Produces a clean flat file with columns:
    date, commodity, sector, price_n1, price_n2, price_n3, price_n4, price_n5

One row per (bimonthly date, commodity).
"""

import sys
from pathlib import Path
sys.path.insert(0, "./src/")

import pandas as pd
import numpy as np
from settings import config

DATA_DIR   = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

# Map product_code -> (commodity_name, sector)
# Based on date ranges and known commodities in the paper
# Need to plug in correct codes

PRODUCT_MAP = {
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


    
def parse_contrdate(s):
    """
    Convert MMYY string like '0983' -> datetime(1983, 9, 1).
    """
    try:
        s = str(s).zfill(4)
        mm = int(s[:2])
        yy = int(s[2:])
        yyyy = 1900 + yy if yy >= 50 else 2000 + yy
        return pd.Timestamp(year=yyyy, month=mm, day=1)
    except Exception:
        return pd.NaT

def make_bimonthly_dates(start="1986-03-01", end="2025-12-31"):
    """Last business day of Jan, Mar, May, Jul, Sep, Nov."""
    all_month_ends = pd.date_range(start=start, end=end, freq="BME")
    mask = all_month_ends.month.isin([1, 3, 5, 7, 9, 11])
    return all_month_ends[mask]

def build_clean_panel():
    print("Loading raw futures data...")
    df = pd.read_parquet(Path(DATA_DIR) / "wrds_futures.parquet")
    
    # Keep only our target commodities
    df = df[df["product_code"].isin(PRODUCT_MAP.keys())].copy()
    
    # Parse contrdate
    df["contrdate_parsed"] = df["contrdate"].apply(parse_contrdate)
    
    # Filter to sample period with 1-month buffer on each side
    df = df[
        (df["date"] >= "1986-01-01") &
        (df["date"] <= "2026-03-01")
    ]
    
    print(f"Rows after filtering: {len(df)}")
    
    # Get bimonthly observation dates
    obs_dates = make_bimonthly_dates()
    print(f"Observation dates: {len(obs_dates)} "
          f"({obs_dates[0].date()} to {obs_dates[-1].date()})")
    
    records = []
    
    for obs_dt in obs_dates:
        # Get prices on or near this date (within 5 business days back)
        window = pd.date_range(
            obs_dt - pd.offsets.BDay(5), obs_dt, freq="B"
        )
        day_df = df[df["date"].isin(window)].copy()
        if day_df.empty:
            continue
        # Use most recent available date
        most_recent = day_df["date"].max()
        day_df = day_df[day_df["date"] == most_recent]
        
        for prod_code, (comm_name, sector) in PRODUCT_MAP.items():
            sub = day_df[day_df["product_code"] == prod_code].copy()
            if sub.empty:
                continue
            
            # Drop contracts expiring within 30 days (roll buffer)
            cutoff = obs_dt + pd.Timedelta(days=30)
            sub = sub[sub["contrdate_parsed"] > cutoff]
            if sub.empty:
                continue
            
            # Sort by expiry, take nearest 4
            sub = sub.sort_values("contrdate_parsed").reset_index(drop=True)
            
            row = {
                "date"     : obs_dt,
                "commodity": comm_name,
                "sector"   : sector,
                "price_n1" : sub.iloc[0]["settlement"] if len(sub) > 0 else np.nan,
                "price_n2" : sub.iloc[1]["settlement"] if len(sub) > 1 else np.nan,
                "price_n3" : sub.iloc[2]["settlement"] if len(sub) > 2 else np.nan,
                "price_n4" : sub.iloc[3]["settlement"] if len(sub) > 3 else np.nan,
                "price_n5" : sub.iloc[4]["settlement"] if len(sub) > 4 else np.nan,
            }
            records.append(row)
    
    panel = pd.DataFrame(records)
    panel = panel.sort_values(["date", "sector", "commodity"]).reset_index(drop=True)
    
    print(f"\nFinal panel shape: {panel.shape}")
    print(f"Commodities: {panel['commodity'].nunique()}")
    print(f"Date range: {panel['date'].min().date()} to {panel['date'].max().date()}")
    print(f"\nSample:\n{panel.head(10).to_string()}")
    
    # Save
    out_path = Path(OUTPUT_DIR) / "commodity_panel_expanded.csv"
    panel.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")
    
    out_path2 = Path(DATA_DIR) / "commodity_panel_extended.parquet"
    panel.to_parquet(out_path2, index=False)
    print(f"Saved to {out_path2}")
    
    return panel

if __name__ == "__main__":
    panel = build_clean_panel()