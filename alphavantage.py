from datetime import datetime, timedelta, timezone
from json.decoder import JSONDecodeError
import time
import sqlite3
import requests
import csv

def check_version():
    major, minor, patch = sqlite3.sqlite_version_info
    if major < 3 or minor < 28:
        raise Exception(f"Invalid sqlite version {major}.{minor}.{patch}, sqlite must have version >3.28")


alphavantage_key = None

class AlphvantageError(BaseException):
    def __init__(self, msg):
        self.msg = msg

DEFAULT_RSI_TIME_PERIOD = 14


def alphavantage_request(uri):
    time.sleep(60/5)
    req = requests.get(uri)

    error = None
    try:
        error = req.json()
    except:
        pass

    if error != None:
        # When alphavantage gets a symbol that doesn't exist ex: SPX
        # it returns an empty json file just '{}' this checks for that case
        if error == {}:
            error = "Invalid symbol"
        raise AlphvantageError(error)
    return req

def iter_csv_rows_from_request(req, skip_first = True):
    rdr = csv.reader(req.content.decode('utf-8').splitlines(), delimiter=',')
    first_row = True
    for row in rdr:
        if first_row and skip_first:
            first_row = False
            continue
        yield row

def alphavantage_db():
    con = sqlite3.connect('databases/alphavantage.db');
    # This forces sqlite to not mess up transactions
    con.isolation_level = None
    return con

def initialize_alphavantage_db():
    con = alphavantage_db()
    tables = con.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    table_names = [table_name for (table_name,) in tables]
    view_names = [view_name for (view_name,) in con.execute("SELECT name FROM sqlite_master WHERE type='view';").fetchall()]
    if "candlestick_5min" not in table_names:
        con.execute("""
            CREATE TABLE candlestick_5min(
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL
            );
        """)
        con.execute("""
            CREATE UNIQUE INDEX symbol_timestamp
            ON candlestick_5min(symbol, timestamp);
        """)
    if "candlestick_daily" not in table_names:
        con.execute("""
            CREATE TABLE candlestick_daily(
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL
            );
        """)
        con.execute("""
            CREATE UNIQUE INDEX symbol_timestamp_daily
            ON candlestick_daily(symbol, timestamp);
        """)
    if "symbols" not in table_names:
        con.executet("""
            CREATE TABLE symbols(
                name TEXT PRIMARY KEY NOT NULL
            );
        """)
    if "config" not in table_names:
        con.execute("""
            CREATE TABLE config(
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT
            );
        """)
        con.execute("""
            INSERT OR IGNORE INTO config (key)
            VALUES
                ('last_request_timestamp'),
                ('total_requests')
            ;
        """)
    if "rsi_5min" not in table_names:
        con.execute("""
            CREATE TABLE rsi_5min(
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                time_period INTEGER NOT NULL,
                close REAL NOT NULL
            );
        """)
        con.execute("""
            CREATE UNIQUE INDEX rsi_5min_symbol_timestamp_time_period
            ON rsi_5min(symbol, timestamp, time_period);
        """)
    if "vix_daily" not in table_names:
        con.execute("""
            CREATE TABLE vix_daily(
                timestamp INTEGER NOT NULL PRIMARY KEY,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL
            );
        """)
    if "delta_candlestick_5min" not in view_names:
        con.execute("""CREATE VIEW delta_candlestick_5min AS
        SELECT symbol, timestamp, open, high, low, close
        FROM (SELECT
                symbol,
                timestamp,
                open  - LEAD(open, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS open,
                high  - LEAD(high, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS high,
                low   - LEAD(low, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS low,
                close - LEAD(close, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS close
            FROM candlestick_5min
            ORDER BY timestamp DESC
        ) WHERE timestamp not in (SELECT MIN(timestamp) FROM candlestick_5min GROUP BY symbol);
        """)
    if "delta_vix_daily" not in view_names:
        con.execute("""CREATE VIEW delta_vix_daily AS
            SELECT timestamp, open, high, low, close 
            FROM (SELECT
                timestamp,
                open  - LEAD(open, 1, 0)  OVER (ORDER BY timestamp DESC)  AS open,
                high  - LEAD(high, 1, 0)  OVER (ORDER BY timestamp DESC)  AS high,
                low   - LEAD(low, 1, 0)  OVER (ORDER BY timestamp DESC)  AS low,
                close - LEAD(close, 1, 0)  OVER (ORDER BY timestamp DESC)  AS close
            FROM vix_daily
            ORDER BY timestamp DESC
            ) WHERE  timestamp not in (SELECT MIN(timestamp) FROM vix_daily);
        """)
    if "delta_candlestick_daily" not in view_names:
        con.execute("""CREATE VIEW delta_candlestick_daily AS 
        SELECT symbol, timestamp, open, high, low, close
            FROM (SELECT
                symbol,
                timestamp,
                open  - LEAD(open, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS open,
                high  - LEAD(high, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS high,
                low   - LEAD(low, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS low,
                close - LEAD(close, 1, 0)  OVER (PARTITION BY symbol ORDER BY timestamp DESC)  AS close
            FROM candlestick_daily
            ORDER BY timestamp DESC)
            WHERE timestamp not in (SELECT MIN(timestamp) FROM candlestick_daily GROUP BY symbol);
        """)


    con.commit()
    con.close()


def intraday_extended_slices():
    for year in range(1, 3):
        for month in range(1, 13):
            yield f"year{year}month{month}";


def update_vix_daily(con: sqlite3.Connection):
    url = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
    req = requests.get(url)
    con.execute("BEGIN TRANSACTION;")
    for (date, open, high, low, close) in iter_csv_rows_from_request(req):
        timestamp = datetime.strptime(date, '%m/%d/%Y').timestamp()
        con.execute(f"INSERT OR IGNORE INTO vix_daily (timestamp, open, high, low, close) VALUES ({timestamp}, {open}, {high}, {low}, {close})")
    con.execute("END TRANSACTION;")

def update_symbols_in_db():
    print('Updating')
    con = alphavantage_db()
    update_vix_daily(con)
    print(con.execute("SELECT name FROM symbols").fetchall())
    symbol_names =  [symbol_name for (symbol_name,) in con.execute("SELECT name FROM symbols").fetchall()]
    for symbol_name in symbol_names:
        last_date_s = con.execute(f"""
            SELECT timestamp FROM candlestick_5min
            WHERE symbol = '{symbol_name}'
            ORDER BY timestamp DESC
            LIMIT 1;
        """).fetchone()

        daily_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol_name}&apikey={alphavantage_key}&datatype=csv&outputsize=full'
        daily_req = alphavantage_request(daily_url)
        con.execute("BEGIN TRANSACTION")
        for (date, open, high, low, close, volume) in iter_csv_rows_from_request(daily_req):
            timestamp = datetime.strptime(date, '%Y-%m-%d').timestamp()
            con.execute(f"""
                INSERT OR IGNORE INTO candlestick_daily (symbol, timestamp, open, high, low, close, volume)
                VALUES ('{symbol_name}', {timestamp}, {open}, {high}, {low}, {close}, {volume});
            """)
        con.execute("END TRANSACTION")
        
        # TODO: Rewrite the following code so that it makes fewer api calls
        rsi_url = f"https://www.alphavantage.co/query?function=RSI&symbol={symbol_name}&interval=5min&time_period={DEFAULT_RSI_TIME_PERIOD}&series_type=close&apikey={alphavantage_key}&datatype=csv"
        rsi_req = alphavantage_request(rsi_url)
        con.execute("BEGIN TRANSACTION;")
        for (timestamp, rsi_close) in iter_csv_rows_from_request(rsi_req):
            con.execute(f"""
                INSERT OR IGNORE INTO rsi_5min (symbol, timestamp, time_period, close)
                VALUES ('{symbol_name}', strftime('%s','{timestamp}') + strftime('%%H', '5'), {DEFAULT_RSI_TIME_PERIOD}, {rsi_close});
            """)
        con.execute("END TRANSACTION;")

        last_datetime_stored = datetime.fromtimestamp(last_date_s[0], timezone.utc)
        uri = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol_name}&interval=5min&apikey={alphavantage_key}&datatype=csv&outputsize=full'
        req = alphavantage_request(uri)
        last_time_entry = None
        con.execute("BEGIN TRANSACTION;")
        for (timestamp, open, high, low, close, volume) in iter_csv_rows_from_request(req):
            # Alphavantage stores its timestamp in EST Since EST = UTC - 5
            # then UTC = EST + 5
            
            con.execute(f"""
                INSERT OR IGNORE INTO candlestick_5min (symbol, timestamp, open, high, low, close, volume)
                VALUES ('{symbol_name}', strftime('%s','{timestamp}') + strftime('%%H', '5'), {open}, {high}, {low}, {close}, {volume});
            """)
            # TODO Use Instead
            cur_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') + timedelta(hours=5)
            cur_datetime = datetime.fromtimestamp(cur_datetime.timestamp(), timezone.utc)
            if last_time_entry == None:
                last_time_entry = cur_datetime
            last_time_entry = min(last_time_entry, cur_datetime)
        con.execute("COMMIT;")
        print(last_time_entry)
        if last_time_entry > last_datetime_stored:
            add_symbol_to_db(symbol_name, last_datetime_stored)

    con.commit()
    con.close()

def add_symbol_to_db(symbol, last_datetime_stored=None):
    if alphavantage_key == None:
        raise Exception()
    con = alphavantage_db()
    symbols =  con.execute("SELECT name FROM symbols").fetchall()
    symbol_names = [symbol_name for (symbol_name,) in symbols]
    if symbol in symbol_names:
        print("WARNING", symbol, "is already in the database")

    con.execute(f"INSERT OR REPLACE INTO symbols (name) VALUES('{symbol}')")

    rsi_url = f"https://www.alphavantage.co/query?function=RSI&symbol={symbol}&interval=5min&time_period={DEFAULT_RSI_TIME_PERIOD}&series_type=close&apikey={alphavantage_key}&datatype=csv"
    rsi_req = alphavantage_request(rsi_url)
    con.execute("BEGIN TRANSACTION;")
    for (timestamp, rsi_close) in iter_csv_rows_from_request(rsi_req):
        con.execute(f"""
            INSERT OR IGNORE INTO rsi_5min (symbol, timestamp, time_period, close)
            VALUES ('{symbol}', strftime('%s','{timestamp}') + strftime('%%H', '5'), {DEFAULT_RSI_TIME_PERIOD}, {rsi_close});
        """)
    con.execute("END TRANSACTION;")


    daily_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={alphavantage_key}&datatype=csv&outputsize=full'
    daily_req = alphavantage_request(daily_url)
    con.execute("BEGIN TRANSACTION")
    for (date, open, high, low, close, volume) in iter_csv_rows_from_request(daily_req):
        timestamp = datetime.strptime(date, '%Y-%m-%d').timestamp()
        con.execute(f"""
            INSERT OR IGNORE INTO candlestick_daily (symbol, timestamp, open, high, low, close, volume)
            VALUES ('{symbol}', {timestamp}, {open}, {high}, {low}, {close}, {volume});
        """)
    con.execute("END TRANSACTION")

    #for slice_name in intraday_extended_slices():
    #    uri = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=5min&slice={slice_name}&apikey={alphavantage_key}&adjusted=false"
    #    req = alphavantage_request(uri)
    #    last_time_entry = None
    #    con.execute("BEGIN TRANSACTION;")
    #    for (timestamp, open, high, low, close, volume) in iter_csv_rows_from_request(req):
    #        # Alphavantage stores its timestamp in EST Since EST = UTC - 5
    #        # then UTC = EST + 5
    #        
    #        con.execute(f"""
    #            INSERT OR IGNORE INTO candlestick_5min (symbol, timestamp, open, high, low, close, volume)
    #            VALUES ('{symbol}', strftime('%s','{timestamp}') + strftime('%%H', '5'), {open}, {high}, {low}, {close}, {volume});
    #        """)
    #        cur_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') + timedelta(hours=5)
    #        if last_time_entry == None:
    #            last_time_entry = cur_datetime
    #        last_time_entry = min(cur_datetime, last_time_entry)
    #    
    #    con.execute("END TRANSACTION;")
    #    if last_datetime_stored != None and last_time_entry <= last_datetime_stored:
    #        break

    con.commit()
    con.close()


initialize_alphavantage_db()