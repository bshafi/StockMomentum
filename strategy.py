from datetime import date, datetime, timedelta, timezone
from types import CellType
from typing import Any, Tuple,List

from alphavantage import alphavantage_db
from enum import Enum
import psycopg2

class StockAction(Enum):
    BUY = 0
    SELL = 1

def buying_enclosed_vix_daily(con: psycopg2.connection, sell_point, buy_point, start_date: datetime, end_date: datetime) -> List[Tuple[datetime, StockAction]]:
    assert(0 <= sell_point <= 100)
    assert(0 <= buy_point <= 100)
    assert(buy_point > sell_point)

    cursor = con.cursor()


    actions = []
    has_bought = False
    vix_data = cursor.execute("""SELECT timestamp, close as vix
        FROM vix_daily
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """, (start_date, end_date))
    vix_data = cursor.fetchall()
    for (timestamp, vix) in vix_data:
        if has_bought:
            if vix >= buy_point:
                actions.append((date, StockAction.SELL))
                has_bought = False
        else:
            if vix <= sell_point:
                actions.append((date, StockAction.BUY))
                has_bought = True
    return actions

def get_mins_and_maxs(data, key) -> Tuple[List[Any], List[Any]]:
    mins = []
    maxs = []
    for i in range(1, len(data) - 1):
        left = key(data[i - 1])
        right = key(data[i + 1])
        center = key(data[i])
        if left < center and right < center:
            maxs.append(data[i])
        if left > center and right > center:
            mins.append(data[i])
    return (mins, maxs)

def moving_avg_vix_daily(con, days_forward=1, days_prior=10) -> List[Tuple[datetime, float, float]]:
    cursor = con.cursor()
    cursor.execute("""
        SELECT timestamp, close FROM vix_daily
    """)
    vix_daily_close = cursor.fetchall()

    vix_perdictions = []
    for i in range(days_prior, len(vix_daily_close)-1):
        timestamp, close = vix_daily_close[i]
        date = timestamp
        mins, maxs = get_mins_and_maxs(vix_daily_close[i-days_prior:i+1], lambda row: row[1])
        avg_mins = 0
        for (timestamp, min) in mins:
            avg_mins = avg_mins + min
        if len(mins) == 0:
            avg_mins = 0
        else:
            avg_mins = avg_mins / len(mins)
        avg_maxs = 0
        for (timestamp, max) in maxs:
            avg_maxs = avg_maxs + max
        if len(maxs) == 0:
            avg_maxs = 0
        else:
            avg_maxs = avg_maxs / len(maxs)
        vix_perdictions.append((date + timedelta(days=days_forward), avg_mins, avg_maxs))
    return vix_perdictions

def buy_moving_avg_vix_daily(con, start_date: datetime, end_date: datetime, weight=1, days_prior=10) -> List[Tuple[datetime, StockAction]]:
    if weight < 0:
        raise Exception("Invalid weight")

    real_min_time = start_date - timedelta(days=days_prior)
    cursor = con.cursor()
    cursor.execute(f"""
        SELECT timestamp, close FROM vix_daily
        WHERE timestamp BETWEEN %s AND %s
    """, (real_min_time, end_date))
    vix_daily_close = cursor.fetchall()
    actions = []
    has_bought = False
    for i in range(days_prior, len(vix_daily_close)-1):
        timestamp, vix = vix_daily_close[i]
        minimums, maximums = get_mins_and_maxs(vix_daily_close[i-days_prior:i+1], lambda row: row[1])
        avg_mins = 0
        for (timestamp, minimum) in minimums:
            avg_mins = avg_mins + minimum
        if len(minimums) == 0:
            avg_mins = 0
        else:
            avg_mins = avg_mins / len(minimums)
        avg_maxs = 0
        for (timestamp, maximum) in maximums:
            avg_maxs = avg_maxs + maximum
        if len(maximums) == 0:
            avg_maxs = 0
        else:
            avg_maxs = avg_maxs / len(maximums)
        center = (avg_mins + avg_maxs) / 2
        offset = abs(avg_maxs - center)
        # assert(offset >= 0)
        buy_point = min(center + weight * offset, 90)
        sell_point = max(center - weight * offset, 10)

        if has_bought:
            if vix >= buy_point:
                actions.append((date, StockAction.SELL))
                has_bought = False
        else:
            if vix <= sell_point:
                actions.append((date, StockAction.BUY))
                has_bought = True
        
    return actions



def percent_return_daily(con, h_actions: List[Tuple[datetime, StockAction]], symbol) -> float:
    if len(h_actions) == 0:
        return 0

    actions = sorted(h_actions, key=lambda x: x[0])
    min_date = actions[0][0]
    max_date = actions[-1][0]

    cursor = con.cursor()
    cursor.execute(f"""
        SELECT timestamp, close FROM candlestick_daily
        WHERE symbol = %s AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """, (symbol, min_date, max_date))
    data = cursor.fetchall()

    if len(data) == 0:
        return 0

    min_symbol_date = data[0][0]
    max_symbol_date = data[-1][0]
    actions = list(filter(lambda x: min_symbol_date <= x[0] <= max_symbol_date, actions))
    if len(actions) != 0 and actions[0][1] == StockAction.SELL:
        actions = actions[1:]
    
    if len(actions) == 0:
        return 0

    gains = 0
    last_buying_price = None
    i = 0

    for (timestamp, close) in data:
        date = timestamp
        if actions[i][0] <= date:
            action_date, action = actions[i]
            i = i + 1
            if action == StockAction.BUY:
                assert(last_buying_price == None)
                last_buying_price = close
            elif action == StockAction.SELL:
                assert(last_buying_price != None)
                gains = gains + (close - last_buying_price) / last_buying_price
                last_buying_price = None

        if i >= len(actions):
            break
    return gains * 100


