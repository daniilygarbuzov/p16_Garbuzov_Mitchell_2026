
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


import pandas as pd
import pandas_datareader.data as web
import databento as db

from settings import config

"Credit to jmbejara and the ftsfr paper for the FRED pull code which has been adapted here"

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
Databento_API = config("Databento_API")

# Establish connection and authenticate
client = db.Historical(Databento_API)

def pull_databento(start_date=START_DATE, end_date=END_DATE, ffill=True):
    """
    Lookup series code, e.g., like this:
    
    """

    df = client.timeseries.get_range(
        dataset="IFUS.IMPACT",
        schema="statistics",
        symbols=["ALL_SYMBOLS"],
        #Just proof of concept here that we can pull data, will need to do more with this in a bit
        start="2018-12-24",
        end="2018-12-25",
        limit = 100
    )

    return df

def load_databento(data_dir=DATA_DIR):
    """
    Must first run this module as main to pull and save data.
    """
    file_path = Path(data_dir) / "databento.parquet"
    df = pd.read_parquet(file_path)
    # df = pd.read_csv(file_path, parse_dates=["DATE"])
    # df = df.set_index("DATE")
    return df


if __name__ == "__main__":
    df = pull_databento(START_DATE, END_DATE)
    filedir = Path(DATA_DIR)
    filedir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filedir / "databento.parquet")