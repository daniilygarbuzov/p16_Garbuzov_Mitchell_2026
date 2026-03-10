"""
exploratory_charts.py
---------------------
Generates exploratory charts of commodity futures settlement prices
by product code. Reads from wrds_futures.parquet and saves a PNG
to OUTPUT_DIR.

Usage
-----
    ipython ./src/exploratory_charts.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import matplotlib.pyplot as plt

from settings import config

OUTPUT_DIR = config("OUTPUT_DIR")
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def load_futures_data():
    import pandas as pd
    data_dir = Path(config("DATA_DIR"))
    return pd.read_parquet(data_dir / "wrds_futures.parquet")


try:
    df_futures = load_futures_data()
    fig, ax = plt.subplots(figsize=(12, 6))
    for code, group in df_futures.groupby("product_code"):
        ax.plot(group["date"], group["settlement"], alpha=0.3, linewidth=0.5)
    ax.set_title("Futures Settlement Prices")
    ax.set_xlabel("Date")
    ax.set_ylabel("Settlement Price")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(Path(OUTPUT_DIR) / "futures_settlements.png", dpi=150)
    plt.close(fig)
    print("Futures settlements chart saved.")
except FileNotFoundError:
    print("Skipping futures chart — wrds_futures.parquet not found. Run pull_wrds_clean first.")

print("Exploratory charts done.")