import polars as pl
from table1 import build_table1
from table2 import build_table2

df = pl.read_parquet("your_long_form_futures.parquet")  # columns: date, commodity, contract_rank, futures_price
commodity_to_sector = {
    "CrudeOil": "Energy",
    "HeatingOil": "Energy",
    # ...
}

table1 = build_table1(df, commodity_to_sector, nw_lags=1)
print(table1)

table2 = build_table2(df, short_maturity_rank=2, nw_lags=1)
print(table2)

