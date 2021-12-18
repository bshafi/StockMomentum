from time import localtime
from backtester import query_table, backtest_data
import sm_util
from sm_util import ArgumentError, historical_database

sm_util.credentials_file = 'credentials/admin_credentials'
import alphavantage

alphavantage.alphavantage_key = open('credentials/alphavantage_key0').read().strip()

def lambda_handler(event, context = None):
    con = sm_util.historical_database()
    alphavantage.update_symbols_in_db(con, TIME_LIMIT_MINUTES=10)
    con.close()