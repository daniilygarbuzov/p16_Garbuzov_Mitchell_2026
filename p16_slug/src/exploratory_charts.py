import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import matplotlib.pyplot as plt
import pandas as pd
<<<<<<< HEAD
<<<<<<< HEAD
=======
import plotly.tools as tls
import plotly.graph_objects as go
>>>>>>> 78ddf03ec1bd176dd908f4be26647081829fc04e

from settings import config
from pull_FRED import load_fred
=======

from settings import config
>>>>>>> f1e88fce90b65662266708504274cac43c79b921
from pull_WRDS import load_combined_futures_data

OUTPUT_DIR = config("OUTPUT_DIR")
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

<<<<<<< HEAD
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
<<<<<<< HEAD
    fig.savefig(Path(OUTPUT_DIR) / "treasury_yields.png", dpi=150)
=======

    # Convert to Plotly and save HTML
    plotly_fig = tls.mpl_to_plotly(fig)
    plotly_fig.write_html(Path(OUTPUT_DIR) / "treasury_yields.html")
>>>>>>> 78ddf03ec1bd176dd908f4be26647081829fc04e
    plt.close(fig)
    print("Treasury yields chart saved.")
except FileNotFoundError:
    print("Skipping treasury yields chart — fred.parquet not found. Run pull_FRED first.")

<<<<<<< HEAD
=======
>>>>>>> f1e88fce90b65662266708504274cac43c79b921
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
<<<<<<< HEAD
=======
# --- Futures Settlements (Compressed) ---
try:
    df_futures = load_combined_futures_data()
    
    # Downsample to every 5th trading day to reduce data points
    df_futures = df_futures.iloc[::5, :].reset_index(drop=True)
    
    # Limit to top 10 commodities by trading volume
    top_commodities = df_futures.groupby("product_code").size().nlargest(10).index
    df_futures = df_futures[df_futures["product_code"].isin(top_commodities)]
    
    # Use Plotly directly (faster, smaller output than matplotlib conversion)
    fig = go.Figure()
    for code in df_futures["product_code"].unique():
        group = df_futures[df_futures["product_code"] == code]
        fig.add_trace(go.Scattergl(
            x=group["date"],
            y=group["settlement"],
            mode='lines',
            name=f"Product {code}",
            hoverinfo='skip'  # Remove hover data to reduce size
        ))
    
    fig.update_layout(
        title="Futures Settlement Prices",
        xaxis_title="Date",
        yaxis_title="Settlement Price",
        hovermode=False,  # Disable hover to save space
        template="plotly_white"
    )
    
    # Save HTML with compression
    fig.write_html(
        Path(OUTPUT_DIR) / "futures_settlements.html",
        config={"responsive": True}
    )
    
    # Save PNG separately
    fig.write_image(Path(OUTPUT_DIR) / "futures_settlements.png", width=1200, height=600)
    print("Futures settlements chart saved (compressed).")
>>>>>>> 78ddf03ec1bd176dd908f4be26647081829fc04e
=======
>>>>>>> f1e88fce90b65662266708504274cac43c79b921
except FileNotFoundError:
    print("Skipping futures chart — wrds_futures.parquet not found. Run pull_WRDS first.")

print("Exploratory charts done.")