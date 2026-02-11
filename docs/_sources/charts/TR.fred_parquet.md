---
date: 2026-02-10 23:18:28
tags: I, n, t, e, r, e, s, t,  , R, a, t, e, s, :, 
, ", D, G, S, 1, M, O, ", :,  , ", 1, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 3, M, O, ", :,  , ", 3, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 6, M, O, ", :,  , ", 6, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 1, ", :,  , ", 1, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 2, ", :,  , ", 2, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 3, ", :,  , ", 3, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", 

category: 
---

# Chart: Interest rates over time
Plotting US interest rates over time from 1986 to 2010

## Chart
```{raw} html
<iframe src="../_static/TR/fred_parquet.html" height="500px" width="100%"></iframe>

<p style="text-align: center;">Sources: I, n, t, e, r, e, s, t,  , R, a, t, e, s, :, 
, ", D, G, S, 1, M, O, ", :,  , ", 1, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 3, M, O, ", :,  , ", 3, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 6, M, O, ", :,  , ", 6, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 1, ", :,  , ", 1, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 2, ", :,  , ", 2, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 3, ", :,  , ", 3, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", 
</p>
```
[Full Screen Chart](../download_chart/TR/fred_parquet.html)








## Chart Specs

| Chart Name             | Interest rates over time                                             |
|------------------------|------------------------------------------------------------|
| Chart ID               | fred_parquet                                               |
| Topic Tags             |                                 |
| Data Series Start Date |                                  |
| Data Frequency         |                                          |
| Observation Period     |                                      |
| Lag in Data Release    |                                     |
| Data Release Timing    |                                     |
| Seasonal Adjustment    |                                     |
| Units                  |                                                   |
| HTML Chart             | [HTML](../download_chart/TR/fred_parquet.html)    |


## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [fred_parquet](../dataframes/TR/fred_parquet.md)                                       |
| Data Sources                   | I, n, t, e, r, e, s, t,  , R, a, t, e, s, :, 
, ", D, G, S, 1, M, O, ", :,  , ", 1, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 3, M, O, ", :,  , ", 3, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 6, M, O, ", :,  , ", 6, -, M, o, n, t, h,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 1, ", :,  , ", 1, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 2, ", :,  , ", 2, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", ,, 
, ", D, G, S, 3, ", :,  , ", 3, -, Y, e, a, r,  , T, r, e, a, s, u, r, y,  , Y, i, e, l, d, ", 
                                        |
| Data Providers                 | F, R, E, D,  , (, F, e, d, e, r, a, l,  , R, e, s, e, r, v, e,  , B, a, n, k,  , o, f,  , S, t, .,  , L, o, u, i, s, )                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | API Call: df = web.DataReader(list(series_to_pull.keys()), "fred", start_date, end_date)                                                    |
| Data available up to (min)     |                                                              |
| Data available up to (max)     |                                                              |
| Dataframe Path                 | /Users/daniilg/FinM/329/p16_Garbuzov_Mitchell_2026/p16_slug/_data/fred.parquet                                                   |


**Linked Charts:**


- [TR:fred_parquet](../../charts/TR.fred_parquet.md)



## Pipeline Manifest

| Pipeline Name                   | Commodities Paper Replication                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [TR](../index.md)              |
| Lead Pipeline Developer         | Garbuzov & Mitchell             |
| Contributors                    | Garbuzov & Mitchell           |
| Git Repo URL                    | p16_Garbuzov_Mitchell_2026                        |
| Pipeline Web Page               | <a href="file:///Users/daniilg/FinM/329/p16_Garbuzov_Mitchell_2026/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-10 23:18:28           |
| OS Compatibility                |  |
| Linked Dataframes               |  [TR:fred_parquet](../dataframes/TR/fred_parquet.md)<br>  [TR:wrds_futures_parquet](../dataframes/TR/wrds_futures_parquet.md)<br>  |

