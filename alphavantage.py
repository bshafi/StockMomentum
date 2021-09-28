from datetime import datetime, timedelta, timezone
from json.decoder import JSONDecodeError
import time
import sqlite3
import requests
import csv

alphavantage_key = None

class AlphvantageError(BaseException):
    def __init__(self, msg):
        self.msg = msg

def alphavantage_request(uri):
    time.sleep(1/5)
    req = requests.get(uri)
    error = get_alphavantage_error_code(req)
    if error != None:
        raise AlphvantageError(error)
    return req

def alphavantage_db():
    con = sqlite3.connect('databases/alphavantage.db');
    # This forces sqlite to not mess up transactions
    con.isolation_level = None
    return con

def initialize_alphavantage_db():
    con = alphavantage_db()
    tables = con.execute("""
        SELECT name FROM sqlite_master WHERE type='table';
    """).fetchall()
    table_names = [table_name for (table_name,) in tables]
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

            INSERT INTO config (key)
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
            CREATE UNIQUE INDEX rsi_5min_symbol_timestamp
            ON rsi_5min(symbol, timestamp);
        """);
    con.commit()
    con.close()


def intraday_extended_slices():
    for year in range(1, 3):
        for month in range(1, 13):
            yield f"year{year}month{month}";

def get_alphavantage_error_code(req):
    jayson = None
    try:
        jayson = req.json()
    except:
        pass
    return jayson

def update_symbols_in_db():
    print('Updating')
    con = alphavantage_db()
    print(con.execute("SELECT name FROM symbols").fetchall())
    symbol_names =  [symbol_name for (symbol_name,) in con.execute("SELECT name FROM symbols").fetchall()]
    for symbol_name in symbol_names:
        last_date_s = con.execute(f"""
            SELECT timestamp FROM candlestick_5min
            WHERE symbol = '{symbol_name}'
            ORDER BY timestamp DESC
            LIMIT 1;
        """).fetchone()
        last_datetime_stored = datetime.fromtimestamp(last_date_s[0], timezone.utc)
        uri = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol_name}&interval=5min&apikey={alphavantage_key}&datatype=csv&outputsize=full'
        req = alphavantage_request(uri)
        rdr = csv.reader(req.content.decode('utf-8').splitlines(), delimiter=',')
        last_time_entry = None
        first_line = True
        con.execute("BEGIN TRANSACTION;")
        for (timestamp, open, high, low, close, volume) in rdr:
            if first_line:
                first_line = False
                continue
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
    for slice_name in intraday_extended_slices():
        uri = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=5min&slice={slice_name}&apikey={alphavantage_key}&adjusted=false"
        req = alphavantage_request(uri)
        first_row = True
        last_time_entry = None
        rdr = csv.reader(req.content.decode('utf-8').splitlines(), delimiter=',')
        con.execute("BEGIN TRANSACTION;")
        
        for (timestamp, open, high, low, close, volume) in rdr:
            if first_row:
                first_row = False
                continue
            # Alphavantage stores its timestamp in EST Since EST = UTC - 5
            # then UTC = EST + 5
            
            con.execute(f"""
                INSERT OR IGNORE INTO candlestick_5min (symbol, timestamp, open, high, low, close, volume)
                VALUES ('{symbol}', strftime('%s','{timestamp}') + strftime('%%H', '5'), {open}, {high}, {low}, {close}, {volume});
            """)
            cur_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') + timedelta(hours=5)
            if last_time_entry == None:
                last_time_entry = cur_datetime
            last_time_entry = min(cur_datetime, last_time_entry)
        
        con.execute("END TRANSACTION;")
        if last_datetime_stored != None and last_time_entry <= last_datetime_stored:
            break

    con.commit()
    con.close()


initialize_alphavantage_db()