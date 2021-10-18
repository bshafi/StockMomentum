import psycopg2
from datetime import datetime, timezone
from enum import Enum
from sm_util import ArgumentError, parse_date
from strategy import percent_return_daily, buying_enclosed_vix_daily

def query_table(con, params):
    _start_date = params.get('start_date', None)
    _end_date = params.get('end_date', None)
    _metadata = params.get('metadata', None)
    _order = params.get('order', None)
    _symbol = params.get('symbol', None)
    table_name = params.get('table_name', None)
    VALID_TABLE_NAMES = [
        'candlestick_5min', 
        'candlestick_daily',
        'vix_1hour',
        'vix_5min',
    ]
    if table_name not in VALID_TABLE_NAMES:
        raise ArgumentError("Invalid argument 'table_name'")
    if table_name in ['candlestick_5min', 'candlestick_daily'] and _symbol == None:
        raise ArgumentError("Candlestick data requires parameter symbol")

    start_date = None
    end_date = None
    try:
        start_date_formatted = parse_date(_start_date)
        start_date = start_date_formatted.strftime('%Y-%m-%d %H:%M:%S')
        end_date_formatted = parse_date(_end_date)
        end_date = end_date_formatted.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ArgumentError("Date parameter was in an invalid format\nFormat the date in 2021-08-01 21:00")

    metadata = None
    if _metadata in ['t', 'true', 'True', 'TRUE']:
        metadata = True
    if _metadata in ['f', 'false', 'False', 'FALSE'] or _metadata == None:
        metadata = False
    if metadata == None:
        raise ArgumentError("Optional parameter metadata can only be true or false")
    
    order = None
    if _order in ['asc', 'ASC', 'ascending']:
        order = 'ASC'
    if _order in ['desc', 'DESC', 'descending'] or _order == None:
        order = 'DESC'
    if order == None:
        raise ArgumentError("Optional parameter order can only be ascending or descending")

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
    symbol = params.get('symbol', None)
    if symbol == None:
        raise ArgumentError('function BACKTEST requires parameter symbol')
    _start_date = params.get('start_date', None)
    start_date = None
    try:
        start_date = parse_date(_start_date)
    except ValueError:
        raise ArgumentError('paramter start_date was either missing or invalid')
    
    _end_date = params.get('end_date', None)
    end_date = None
    try:
        end_date = parse_date(_end_date)
    except ValueError:
        raise ArgumentError('paramter start_date was either missing or invalid')
    pass

    buy_point = params.get('buy_point', None)
    if buy_point == None:
        raise ArgumentError('parameter buy_point was either missing or invalid')
    sell_point = params.get('sell_point', None)
    if sell_point == None:
        raise ArgumentError('parameter sell_point was either missing or invalid')
        
    buy_point = float(buy_point)
    sell_point = float(sell_point)
    
    
    h_actions = buying_enclosed_vix_daily(con, buy_point, sell_point, start_date, end_date)
    
    gains = percent_return_daily(con, h_actions, symbol)
    return gains
