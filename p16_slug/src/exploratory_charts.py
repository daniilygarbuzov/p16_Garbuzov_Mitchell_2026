import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import matplotlib.pyplot as plt
import pandas as pd

from settings import config
from pull_FRED import load_fred
from pull_WRDS import load_combined_futures_data

OUTPUT_DIR = config("OUTPUT_DIR")
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# --- Treasury Yields ---
try:
    df_fred = load_fred()
    fig, ax = plt.subplots(figsize=(12, 6))
    for col in df_fred.columns:
        ax.plot(df_fred.index, df_fred[col], label=col)
    ax.set_title("Treasury Yields Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Yield (%)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(Path(OUTPUT_DIR) / "treasury_yields.png", dpi=150)
    plt.close(fig)
    print("Treasury yields chart saved.")
except FileNotFoundError:
    print("Skipping treasury yields chart — fred.parquet not found. Run pull_FRED first.")

# --- Futures Settlements ---
try:
    df_futures = load_combined_futures_data()
    fig, ax = plt.subplots(figsize=(12, 6))
    for code, group in df_futures.groupby("product_code"):
        ax.plot(group["date"], group["settlement"], alpha=0.5, label=f"Product {code}")
    ax.set_title("Futures Settlement Prices")
    ax.set_xlabel("Date")
    ax.set_ylabel("Settlement Price")
    ax.grid(True, alpha=0.3)
    if df_futures["product_code"].nunique() <= 10:
        ax.legend()
    fig.tight_layout()
    fig.savefig(Path(OUTPUT_DIR) / "futures_settlements.png", dpi=150)
    plt.close(fig)
    print("Futures settlements chart saved.")
except FileNotFoundError:
    print("Skipping futures chart — wrds_futures.parquet not found. Run pull_WRDS first.")

print("Exploratory charts done.")