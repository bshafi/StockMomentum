from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from psycopg2.extras import execute_batch
import time
import psycopg2
import requests
from sm_util import iter_csv_rows_from_request, historical_database

alphavantage_key = None

class AlphvantageError(BaseException):
    def __init__(self, msg):
        self.msg = msg

_last_request_time = datetime.now()
def alphavantage_request(uri):
    global _last_request_time
    delta_time = (datetime.now() - _last_request_time)
    if delta_time < timedelta(seconds=60/5):
        time.sleep(timedelta(seconds=60/5).seconds - delta_time.seconds)
    req = requests.get(uri)
    _last_request_time = datetime.now()

    error = None
    try:
        error = req.json()
    except:
        pass

    if error != None:
        # When alphavantage gets a symbol that doesn't exist ex: SPX
        # it returns an empty json file (just '{}') this checks for that case
        if error == {}:
            error = "Invalid symbol"
        raise AlphvantageError(error)
    return req

def intraday_extended_slices():
    for year in range(1, 3):
        for month in range(1, 13):
            yield f"year{year}month{month}"

def update_vix_daily(con):
    url = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
    req = requests.get(url)
    cursor = con.cursor()
    cursor.execute("BEGIN TRANSACTION")

    def convert_row(row):
        timestamp, open, high, low, close = row
        return (timestamp, Decimal(open), Decimal(high), Decimal(low), Decimal(close))
    
    vars_list = list(map(convert_row, iter_csv_rows_from_request(req)))
    execute_batch(cursor, "INSERT vix_daily(timestamp, open, high, low, close) VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT vix_daily_pkey DO NOTHING", vars_list)


def convert_candlestick_row(symbol, row):
    timestamp, open, high, low, close, volume = row
    return (symbol, timestamp, Decimal(open), Decimal(high), Decimal(low), Decimal(close), Decimal(volume))

def update_candlestick_daily(con, symbol):
    cursor = con.cursor()
    daily_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={alphavantage_key}&datatype=csv&outputsize=full'
    daily_req = alphavantage_request(daily_url)


    vars_list_daily = list(map(lambda row: convert_candlestick_row(symbol, row), iter_csv_rows_from_request(daily_req)))
    cursor.execute("BEGIN TRANSACTION")
    execute_batch(cursor, """
        INSERT INTO candlestick_daily (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, (%s AT TIME ZONE 'America/New_York')::TIMESTAMPTZ, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT candlestick_daily_symbol_timestamp_key DO NOTHING
    """, vars_list_daily)
    cursor.execute("COMMIT TRANSACTION")

def update_candlestick_5min(con, symbol):
    cursor = con.cursor()
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&adjusted=false&symbol={symbol}&apikey={alphavantage_key}&datatype=csv&outputsize=full'
    req = alphavantage_request(url)
    vars_list_intraday = list(map(lambda row: convert_candlestick_row(symbol, row), iter_csv_rows_from_request(req)))
    cursor.execute("BEGIN TRANSACTION")
    execute_batch(cursor, """
        INSERT INTO candlestick_5min (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, (%s AT TIME ZONE 'America/New_York')::TIMESTAMPTZ, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT candlestick_5min_symbol_timestamp_key DO NOTHING
    """, vars_list_intraday)
    cursor.execute("COMMIT TRANSACTION")

def update_symbols_in_db():
    print('Updating')
    con = historical_database()
    cursor = con.cursor()
    update_vix_daily(con)

    print(cursor.execute("SELECT name FROM symbols").fetchall())
    for (symbol_name,) in cursor.execute("SELECT DISTINCT symbol FROM meta_updates_table").fetchall():
        update_candlestick_daily(con, symbol_name)
        update_candlestick_5min(con, symbol_name)

    con.commit()
    con.close()


def add_symbol_2year_hist(con, symbol):
    cursor = con.cursor()
    cursor.execute("""
        SELECT year2_history
        FROM meta_updates_table
        WHERE symbol = %s AND table_name = 'candlestick_5min'
        LIMIT 1
    """, (symbol,))
    rows = cursor.fetchall()
    if len(rows) == 0:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO meta_updates_table VALUES(%s, 'candlestick_5min')", (symbol,))
        cursor.execute("COMMIT TRANSACTION")
        cursor.execute("""
            SELECT year2_history
            FROM meta_updates_table
            WHERE symbol = %s AND table_name = 'candlestick_5min'
            LIMIT 1
        """, (symbol,))
        rows = cursor.fetchall()

    (year2_history,) = rows[0]
    if year2_history:
        return
    for slice_name in intraday_extended_slices():
        uri = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=5min&slice={slice_name}&apikey={alphavantage_key}&adjusted=false"
        req = alphavantage_request(uri)
        vars_list = list(map(lambda row: convert_candlestick_row(symbol, row), iter_csv_rows_from_request(req)))
        cursor.execute("BEGIN TRANSACTION")
        execute_batch(cursor,"""
            INSERT INTO candlestick_5min VALUES(
                %s,
                (%s AT TIME ZONE 'America/New_York')::TIMESTAMPTZ,
                %s,
                %s,
                %s,
                %s,
                %s
            ) ON CONFLICT ON CONSTRAINT candlestick_5min_symbol_timestamp_key DO NOTHING
        """, vars_list)
        cursor.execute("COMMIT TRANSACTION")

    cursor.execute("""
        UPDATE meta_updates_table 
            SET year2_history = 'true' 
        WHERE symbol = %s AND table_name = 'candlestick_5min'
    """, (symbol,))
    print(f'finished {symbol}')