import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import pandas as pd
from settings import config


DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))


def save_latex(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_latex(path, index=False, escape=True, bold_rows=False)


def make_summary_stats_latex():
    """Sector-level summary stats for sr_1 -> summary_stats.tex."""
    panel_path = DATA_DIR / "returns_panel.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(f"returns_panel not found at {panel_path}")

    df = pd.read_parquet(panel_path)

    required = {"sector", "sr_1"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Missing columns in returns_panel: {missing}")

    grouped = (
        df[["sector", "sr_1"]]
        .groupby("sector")["sr_1"]
        .agg(
            Mean="mean",
            Std="std",
            Min="min",
            Max="max",
            Skew=lambda x: x.skew(),
            Kurtosis=lambda x: x.kurt(),
        )
        .reset_index()
        .rename(columns={"sector": "Sector"})
    )

    out_path = OUTPUT_DIR / "summary_stats.tex"
    save_latex(grouped, out_path)
    print(f"summary_stats.tex written to {out_path}")


def make_panel_head():
    """Head of commodity_panel -> commodity_panel_head.tex."""
    panel_path = DATA_DIR / "commodity_panel.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(panel_path)

    df = pd.read_parquet(panel_path)

    # Use the actual columns: date, commodity, sector, price_n1-5
    cols = [c for c in ["date", "commodity", "sector",
                        "price_n1", "price_n2", "price_n3", "price_n4", "price_n5"]
            if c in df.columns]
    head = df[cols].head(10)

    out_path = OUTPUT_DIR / "commodity_panel_head.tex"
    save_latex(head, out_path)
    print(f"commodity_panel_head.tex written to {out_path}")


def make_sectors_list():
    """Distinct sectors -> sectors_list.tex."""
    panel_path = DATA_DIR / "commodity_panel.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(panel_path)

    df = pd.read_parquet(panel_path)
    if "sector" not in df.columns:
        raise ValueError("column 'sector' not found in commodity_panel")

    sectors = pd.DataFrame(sorted(df["sector"].dropna().unique()), columns=["Sector"])

    out_path = OUTPUT_DIR / "sectors_list.tex"
    save_latex(sectors, out_path)
    print(f"sectors_list.tex written to {out_path}")


def make_commodities_list():
    """Commodity names and sectors -> commodities_list.tex."""
    panel_path = DATA_DIR / "commodity_panel.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(panel_path)

    df = pd.read_parquet(panel_path)

    required = {"commodity", "sector"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Missing columns in commodity_panel: {missing}")

    comm = (
        df[["commodity", "sector"]]
        .drop_duplicates()
        .sort_values(["sector", "commodity"])
        .rename(columns={"commodity": "Commodity", "sector": "Sector"})
    )

    out_path = OUTPUT_DIR / "commodities_list.tex"
    save_latex(comm, out_path)
    print(f"commodities_list.tex written to {out_path}")


if __name__ == "__main__":
    make_summary_stats_latex()
    make_panel_head()
    make_sectors_list()
    make_commodities_list()
