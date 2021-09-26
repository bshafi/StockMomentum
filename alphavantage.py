from json.decoder import JSONDecodeError
import time
import sqlite3
import requests
import time
import csv

alphavantage_key = None

class AlphvantageError(BaseException):
    def __init__(self, msg):
        self.msg = msg
        pass

def alphavantage_db():
    return sqlite3.connect('databases/alphavantage.db');

def initialize_alphavantage_db():
    con = alphavantage_db()
    tables = con.execute("""
        SELECT name FROM sqlite_master WHERE type='table';
    """).fetchall()
    table_names = [table_name for (table_name,) in tables]
    if "candlestick_5min" not in table_names:
        con.executescript("""
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
        con.executescript("""
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
        con.executescript("""
            CREATE TABLE symbols(
                name TEXT PRIMARY KEY NOT NULL
            );
        """)
    if "config" not in table_names:
        con.executescript("""
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

initialize_alphavantage_db()

def intraday_extended_slices():
    for year in range(1, 3):
        for month in range(1, 13):
            yield f"year{year}month{month}";

def add_symbol_to_db(symbol):
    if alphavantage_key == None:
        raise Exception()
    con = alphavantage_db()
    symbols =  con.execute("SELECT name FROM symbols").fetchall()
    symbol_names = [symbol_name for (symbol_name,) in symbols]
    if symbol in symbol_names:
        print("WARNING", symbol, "is already in the database")
        con.execute(f"INSERT INTO symbols (name) VALUES('{symbol}')")

    for slice_name in intraday_extended_slices():
        uri = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=5min&slice={slice_name}&apikey={alphavantage_key}&adjusted=false"
        time.sleep(0.2);
        req = requests.get(uri)
        jay = None
        try:
            jay = req.json()
        except:
            pass
        if jay != None:
            raise AlphvantageError(jay)

        con.execute("BEGIN TRANSACTION;")
        first_row = True
        rdr = csv.reader(req.content.decode('utf-8').splitlines(), delimiter=',');
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
            
        con.execute("COMMIT;")

    con.commit()
    con.close()

