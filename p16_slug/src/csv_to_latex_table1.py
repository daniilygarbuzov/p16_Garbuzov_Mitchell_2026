import sys
from pathlib import Path

sys.path.insert(0, "./src/")

import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

def main():
    sr_path = OUTPUT_DIR / "table1_short_roll.csv"
    eh_path = OUTPUT_DIR / "table1_excess_holding.csv"

    df_sr = pd.read_csv(sr_path, index_col=0)
    df_eh = pd.read_csv(eh_path, index_col=0)

    # Stack into one DataFrame with a "panel" column
    sr_long = df_sr.reset_index().assign(panel="Short Roll")
    eh_long = df_eh.reset_index().assign(panel="Excess Holding")
    df_all = pd.concat([sr_long, eh_long], ignore_index=True)

    # Simple LaTeX table with caption and label for referencing
    latex_table = df_all.to_latex(
        index=False,
        float_format="%.4f",
        caption="Summary statistics for Short Roll and Excess Holding returns",
        label="tab:table1_sr_eh",
    )

    # Saved as a .tex file in reports so LaTeX can \input it
    reports_dir = Path("./reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    tex_path = reports_dir / "table1_sr_eh.tex"
    tex_path.write_text(latex_table)
    print(f"Saved LaTeX table to {tex_path}")

if __name__ == "__main__":
    main()
