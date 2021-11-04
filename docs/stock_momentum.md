# Stock Momentum



## Historical Data

```function=QUERY```

Arguments:

```table_name```:
The table to query historical data from. Valid tables are ```candlestick_5min```, ```candlestick_daily```, ```vix_5min```, and ```vix_1hour```. 

```symbol```: The symbol to query data for. This only applies to the candlestick tables. Not all symbols are supported and unsupported symbols will give an error.

```start_date```, ```end_date```: The beginning and end of the data to query. Due to API limitations candlestick data may not be completely updated. VIX data is available up till Oct 1 2021.


Example:
```
    $API_URL function=QUERY&table_name=candlestick_5min&symbol=IWM&start_date=2021-01-01&end_date=2021-10-01
```

## Backtesting
```funciton=BACKTEST```

Arguments:

