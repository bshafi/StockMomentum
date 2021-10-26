from os import pipe
import psycopg2
from datetime import datetime, timezone, timedelta
from enum import Enum
from sm_util import ArgumentError, parse_date, check_args
from strategy import percent_return_daily, buying_enclosed_vix_daily, sentiment_trader_antivix, percent_gains, sentiment_trader_antivix_5min

def check_symbol(con, symbol):
    with con.cursor() as cursor:
        cursor.execute("SELECT DISTINCT ON (symbol) symbol FROM meta_updates_table WHERE symbol = %s", (symbol,))
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise ArgumentError(f"Symbol name {symbol} is not supported")
        return symbol

def check_num(text):
    f = None
    try:
        f = float(text)
    except ValueError:
        raise ArgumentError("Could not parse float")
    return f

def query_table(con, params):
    VALID_TABLE_NAMES = [
        'candlestick_5min', 
        'candlestick_daily',
        'vix_1hour',
        'vix_5min',
    ]
    def check_if_valid_table(name):
        if name in VALID_TABLE_NAMES:
            return name
        else:
            raise ArgumentError("Invalid table_name")

    parsed_args = check_args(params, {
        "start_date": parse_date,
        "end_date": parse_date,
        "symbol": lambda symbol: check_symbol(con, symbol),
        "table_name": check_if_valid_table
    })
    table_name = parsed_args['table_name']
    start_date = parsed_args['start_date']
    end_date = parsed_args['end_date']
    _symbol = parsed_args['symbol']
    order = 'DESC'
    
    cursor = con.cursor()

    data = None
    row_str = None
    if table_name in ['candlestick_5min', 'candlestick_daily']:
        cursor.execute(f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {table_name}
            WHERE symbol = %(symbol)s AND timestamp BETWEEN (%(start_date)s AT TIME ZONE 'America/New_York') AND (%(end_date)s AT TIME ZONE 'America/New_York')
            ORDER BY timestamp {order}
        """, {'start_date': start_date, 'end_date': end_date, 'symbol': _symbol})
        data = cursor.fetchall()
        row_str = "timestamp, open, high, low, close, volume"
    elif table_name in ['vix_1hour', 'vix_5min']:
        cursor.execute(f"""
            SELECT timestamp, open, high, low, close 
            FROM {table_name}
            WHERE timestamp BETWEEN (%(start_date)s AT TIME ZONE 'America/New_York') AND (%(end_date)s AT TIME ZONE 'America/New_York')
            ORDER BY timestamp {order}
        """, {'start_date': start_date, 'end_date': end_date})
        data = cursor.fetchall()
        row_str = "timestamp, open, high, low, close"
    
    row_str = row_str + "\n"
    for row in data:
        for i in range(len(row)):
            if not isinstance(row[i], datetime):
                row_str = row_str +  str(row[i])
            else:
                row_str = row_str + str(row[i])
            if i + 1 < len(row):
                row_str = row_str + ","
        row_str = row_str + '\n'

    return str.encode(row_str)

def backtest_data(con, params):
    VALID_BACKTESTERS = [
        "sentiment_antivix",
        "sentiment_antivix_5min"
    ]
    VALID_STATS = [
        None,
        'sum'
    ]
    def check_in_list(x, l):
        if x in l:
            return x
        else:
            raise ArgumentError("invalid value")

            
    parsed_args = check_args(params, {
        "symbol": lambda symbol: check_symbol(con, symbol),
        "start_date": parse_date,
        "end_date": parse_date,
        "observation_period": check_num,
        "buy_point": check_num,
        "backtester": lambda x: check_in_list(x, VALID_BACKTESTERS),
        "stats": lambda x: check_in_list(x, VALID_STATS)
    })

    actions = None
    if parsed_args['backtester'] == 'sentiment_antivix':
        actions = sentiment_trader_antivix(
            con, 
            parsed_args['buy_point'], 
            parsed_args['observation_period'], 
            parsed_args['symbol'], 
            parsed_args['start_date'], 
            parsed_args['end_date']
        )
        gains = percent_gains(actions)
        last_price = None
    elif parsed_args['backtester'] == 'sentiment_antivix_5min':
        actions = sentiment_trader_antivix_5min(
            con, 
            parsed_args['buy_point'], 
            parsed_args['observation_period'], 
            parsed_args['symbol'], 
            parsed_args['start_date'], 
            parsed_args['end_date']
        )
        gains = percent_gains(actions)
        last_price = None
    
    stats_name = parsed_args['stats']
    if stats_name == 'sum':
        return str.encode(str(gains * 100))
        
    obs_period = parsed_args['observation_period']
    
    ret_str = "timestamp, gain\n"
    for timestamp, action, price in actions:
        if last_price == None:
            last_price = price
        else:
            gain = round(((price - last_price) / last_price) * 100, 2)
            offset_date = (timestamp - timedelta(days=obs_period)).strftime('%Y-%m-%d %H:%M')
            ret_str = ret_str + f"'{offset_date}, {gain}\n"
            last_price = None
    

    return str.encode(ret_str)
