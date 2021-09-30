from datetime import date, datetime, timezone
from typing import Tuple,List
from alphavantage import alphavantage_db
from enum import Enum
import sqlite3

class StockAction(Enum):
    BUY = 0
    SELL = 1

def buying_enclosed_vix_daily(sell_point, buy_point, start_date: datetime, end_date: datetime) -> List[Tuple[datetime, StockAction]]:
    assert(0 <= sell_point <= 100)
    assert(0 <= buy_point <= 100)
    assert(buy_point < sell_point)

    con = alphavantage_db()
    QUERY = f"""SELECT timestamp, close as vix
        FROM vix_daily
        WHERE timestamp BETWEEN {start_date.timestamp()} AND {end_date.timestamp()}
        ORDER BY timestamp ASC
    """
    actions = []
    has_bought = False
    vix_data = con.execute(QUERY).fetchall()
    for (timestamp, vix) in vix_data:
        date = datetime.fromtimestamp(timestamp, timezone.utc)
        if has_bought:
            if vix < buy_point:
                actions.append((date, StockAction.SELL))
                has_bought = False
        else:
            if vix > sell_point:
                actions.append((date, StockAction.BUY))
                has_bought = True
    return actions

def percent_return_daily(con: sqlite3.Connection, h_actions: List[Tuple[datetime, StockAction]], symbol) -> float:
    actions = sorted(h_actions, key=lambda x: x[0])
    min_date = actions[0][0]
    max_date = actions[-1][0]

    data = con.execute(f"""
        SELECT timestamp, close FROM candlestick_daily
        WHERE symbol = '{symbol}' AND timestamp BETWEEN {min_date.timestamp()} AND {max_date.timestamp()}
        ORDER BY timestamp ASC
    """).fetchall()

    gains = 0
    last_buying_price = None
    i = 0
    for (timestamp, close) in data:
        date = datetime.fromtimestamp(timestamp, timezone.utc)
        if actions[i][0] <= date:
            action_date, action = actions[i]
            i = i + 1
            if action == StockAction.BUY:
                assert(last_buying_price == None)
                last_buying_price = close
            elif action == StockAction.SELL:
                assert(last_buying_price != None)
                gains = gains + (close - last_buying_price)
                last_buying_price = None

        if i >= len(actions):
            break
    return gains



