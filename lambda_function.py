from time import localtime
from backtester import query_table, backtest_data
import sm_util
from sm_util import ArgumentError, historical_database

sm_util.credentials_file = 'credentials/trader_bot0'

def lambda_handler(event, context = None):
    params = event['params']['querystring']
    function = params['function']
    
    con = historical_database()
    
    try:
        if function == 'QUERY':
            return query_table(con, params)
        elif function == 'BACKTEST':
            return backtest_data(con, params)
    except ArgumentError as arg_err:
        return str.encode(arg_err.get_msg())

print(lambda_handler({
  "params": {
    "querystring": {
      "function": "BACKTEST",
      "backtester": "sentiment_antivix",
      "start_date": "2010-08-01",
      "end_date": "2021-09-01",
      "buy_point": "12",
      "sell_point": "89",
      "symbol": "LASR",
      "observation_period": "10"
    }
  }
}))