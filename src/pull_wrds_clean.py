"""
Pull futures data from WRDS.
"""

import sys
from pathlib import Path
from settings import config

sys.path.insert(1, "./src/")

import pandas as pd
import polars as pl
import wrds
#import chartbook

DATA_DIR = config("DATA_DIR")
WRDS_USERNAME = config("WRDS_USERNAME")

# Verified product codes from tr_ds_fut.wrds_contract_info
# contrcode -> (commodity_name, sector)
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
    396:  ("soybeans",      "Oilseeds"),
    3256: ("soybean_meal",  "Oilseeds"),
    282:  ("soybean_oil",   "Oilseeds"),
    3250: ("feeder_cattle", "Meats"),
    2675: ("live_cattle",   "Meats"),
    2676: ("lean_hogs",     "Meats"),
    1980: ("cocoa",         "Softs"),
    2038: ("coffee",        "Softs"),
    2036: ("orange_juice",  "Softs"),
    1992: ("cotton",        "Ind_Materials"),
    361:  ("lumber",        "Ind_Materials"),
}

TR_DS_COMDS_SERIES = {
    "heating_oil": 7796,
    "gas": 1609,
    "crude_oil": 15909,
    "feeder_cattle": 11646,
    "live_cattle": 10829,
    "live_hogs": 11292,
    "gold": 11288,
    "copper": 4155,
    "silver": 7193,
    "corn": 8629,
    "oats": 7113,
    "wheat": 13100,
    "rough_rice": 14985,
    "soybean_oil": 10731,
    "soybeans": 13060,
    "soybean_meal": 6718,
    "coffee": 10806,
    "orange_juice": 2519,
    "cocoa": 684,
    "cotton": 792,
    "lumber": 16751,
}

def fetch_wrds_contract_info(product_contract_code):
    """
    Fetch rows from wrds_contract_info.

    Parameters
    ----------
    product_contract_code : int
        The commodity's integer contract code (e.g., 1986).

    Returns
    -------
    pandas.DataFrame
        Columns include: futcode, contrcode, contrname, contrdate, startdate, lasttrddate.
    """
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    query = f"""
    SELECT futcode, contrcode, contrname, contrdate, startdate, lasttrddate
    FROM tr_ds_fut.wrds_contract_info
    WHERE contrcode = {product_contract_code}
    """
    df = db.raw_sql(query)
    db.close()
    return df


def fetch_wrds_fut_contract(futcodes_contrdates):
    """
    Fetch daily settlement prices from wrds_fut_contract.

    Parameters
    ----------
    futcodes_contrdates : dict
        Keys are futcode values, and values are corresponding contract date strings.

    Returns
    -------
    pandas.DataFrame
        Columns include: futcode, date_, settlement, and a 'contrdate' column mapped from futcodes_contrdates.
    """
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    #this section is included to prevent an issue with np.float ints being saved as floats in the SQL query
    futcodes = [int(x) for x in futcodes_contrdates.keys()]
    if len(futcodes) == 1:
        in_clause = f"({futcodes[0]})"
    else:
        in_clause = str(tuple(futcodes))

    query = f"""
    SELECT futcode, date_, settlement
    FROM tr_ds_fut.wrds_fut_contract
    WHERE futcode IN {in_clause}
    """
    df = db.raw_sql(query)
    df["date_"] = pd.to_datetime(df["date_"])
    df["contrdate"] = df["futcode"].map(futcodes_contrdates)
    db.close()
    return df


def pull_all_futures_data():
    """
    Pull raw data from WRDS for all 21 commodities in PRODUCT_MAP,
    then concatenate into one DataFrame.

    Returns
    -------
    pandas.DataFrame
        Combined daily settlements for all relevant product codes.
    """
    product_list = list(PRODUCT_MAP.keys())
    all_frames = []
    for code in product_list:
        print(f"Pulling {PRODUCT_MAP[code][0]} (contrcode={code})...")
        info_df = fetch_wrds_contract_info(code)
        if info_df.empty:
            print(f"  No contract info found for {code}")
            continue
        futcodes_contrdates = info_df.set_index("futcode")["contrdate"].to_dict()
        data_contracts = fetch_wrds_fut_contract(futcodes_contrdates)
        if not data_contracts.empty:
            data_contracts["product_code"] = code
            all_frames.append(data_contracts)
            print(f"  Got {len(data_contracts)} rows")
        else:
            print(f"  No price data found for {code}")
    final_df = pd.concat(all_frames, ignore_index=True)
    final_df = final_df.rename(columns={"date_": "date"})
    return final_df


def load_combined_futures_data(data_dir=DATA_DIR, format="pandas"):
    """
    Load combined futures dataset from local parquet file.

    Returns
    -------
    pandas.DataFrame or polars.DataFrame
        A DataFrame of daily settlement data.
    """
    data_dir = Path(data_dir)
    path = data_dir / "wrds_futures.parquet"
    if format == "pandas":
        df = pd.read_parquet(path)
    elif format == "polars":
        df = pl.read_parquet(path)
    else:
        raise ValueError(f"Invalid format: {format}")
    return df


def pull_wrds_tables(data_dir=DATA_DIR):
    """
    Pull selected tables from WRDS.
    """
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    df = db.get_table(library="tr_ds_fut", table="wrds_cseries_info")
    df.to_csv(data_dir / "wrds_cseries_info.csv")
    df.to_parquet(data_dir / "wrds_cseries_info.parquet")

    df = db.get_table(library="tr_ds_fut", table="dsfutcalcserval")
    df.to_csv(data_dir / "dsfutcalcserval.csv")
    df.to_parquet(data_dir / "dsfutcalcserval.parquet")

    db.close()


def pull_all_spot_series(data_dir=DATA_DIR):
    """
    Pull continuous/spot commodity series from TR_DS_COMDS (wrds_cmdy_data)
    and save to parquet.
    """
    db = wrds.Connection(wrds_username=WRDS_USERNAME)

    table_name = "wrds_cmdy_data"
    comcode_col = "comcode"
    date_col = "date_"
    value_col = "close_"

    comcodes = tuple(TR_DS_COMDS_SERIES.values())

    query = f"""
    SELECT {comcode_col} AS comcode,
           {date_col} AS date_,
           {value_col} AS value
    FROM tr_ds_comds.{table_name}
    WHERE {comcode_col} IN {comcodes}
    """

    df = db.raw_sql(query)

    info = db.get_table(library="tr_ds_comds", table="wrds_cmdy_info")
    db.close()

    info_small = info[["comcode", "dsmnemonic", "name", "comdesc"]].drop_duplicates()
    df = df.merge(info_small, on="comcode", how="left")

    df["date"] = pd.to_datetime(df["date_"])
    df = df.drop(columns=["date_"])

    inv_map = {v: k for k, v in TR_DS_COMDS_SERIES.items()}
    df["series_name"] = df["comcode"].map(inv_map)

    data_dir = Path(data_dir)
    path = data_dir / "wrds_spot.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)

    return df


if __name__ == "__main__":
    df = pull_all_futures_data()
    path = Path(DATA_DIR) / "wrds_futures.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    print(f"\nSaved {len(df)} rows to {path}")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"Product codes: {df['product_code'].unique()}")