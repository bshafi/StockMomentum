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
