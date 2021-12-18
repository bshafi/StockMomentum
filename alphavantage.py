from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from psycopg2.extras import execute_batch
import time
import psycopg2
from sm_util import iter_csv_rows_from_request, historical_database, requests_get

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
    req = requests_get(uri)
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
    req = requests_get(url)
    with con.cursor() as cursor:
        cursor.execute("BEGIN TRANSACTION")

        def convert_row(row):
            timestamp, open, high, low, close = row
            return (timestamp, Decimal(open), Decimal(high), Decimal(low), Decimal(close))
        
        vars_list = list(map(convert_row, iter_csv_rows_from_request(req)))
        execute_batch(cursor, "INSERT INTO vix_daily(timestamp, open, high, low, close) VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT vix_daily_pkey DO NOTHING", vars_list)



def convert_candlestick_row(symbol, row):
    timestamp, open, high, low, close, volume = row
    return (symbol, timestamp, Decimal(open), Decimal(high), Decimal(low), Decimal(close), Decimal(volume))

def update_candlestick_daily(con, symbol):
    with con.cursor() as cursor:
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
    with con.cursor() as cursor:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=5min&adjusted=false&symbol={symbol}&apikey={alphavantage_key}&datatype=csv&outputsize=full'
        req = alphavantage_request(url)
        vars_list_intraday = list(map(lambda row: convert_candlestick_row(symbol, row), iter_csv_rows_from_request(req)))
        cursor.execute("BEGIN TRANSACTION")
        execute_batch(cursor, """
            INSERT INTO candlestick_5min (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, (%s AT TIME ZONE 'America/New_York')::TIMESTAMPTZ, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT candlestick_5min_symbol_timestamp_key DO NOTHING
        """, vars_list_intraday)
        cursor.execute("COMMIT TRANSACTION")


# Once the updating lasts longer than TIME_LIMIT_MIN it will abort
# Rember that it is possible for it to last longer than TIME_LIMIT_MIN
# Ex: If you want the function to last 15 minutes use update_symbols_id_db(TIME_LIMIT_MIN=10) instead so it has enough breathing room
def update_symbols_in_db(con, TIME_LIMIT_MINUTES=float('Inf')):
    start_time = datetime.now()
    print('Updating')
    cursor = con.cursor()
    update_vix_daily(con)

    cursor.execute("SELECT DISTINCT ON (symbol) symbol, last_update FROM meta_updates_table")
    for (symbol, last_update) in cursor.fetchall():
        if last_update == None or datetime.now(timezone.utc) - last_update > timedelta(days=1/2):
            cursor.execute("BEGIN TRANSACTION")
            update_candlestick_daily(con, symbol)
            update_candlestick_5min(con, symbol)
            cursor.execute("UPDATE meta_updates_table SET last_update = %s WHERE symbol = %s", (datetime.now(timezone.utc), symbol))
            cursor.execute("COMMIT TRANSACTION")
        con.commit()
        if (datetime.now() - start_time) / timedelta(minutes=1) > TIME_LIMIT_MINUTES:
            break



def add_symbol_2year_hist(con, symbol):
    with con.cursor() as cursor:
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
        con.commit()

    print(f'finished {symbol}')