from datetime import date, datetime, timedelta, timezone, tzinfo, time
from types import CellType
from typing import Any, Tuple, List

import numpy as np
from numpy.core.fromnumeric import std
from sm_util import historical_database
from enum import Enum
import psycopg2
import fastdtw

class StockAction(Enum):
    BUY = 0
    SELL = 1

def sentiment_trader_antivix(con, buy_point, observation_period, symbol, start_date: datetime, end_date: datetime):
    cursor = con.cursor()
    cursor.execute("""
        SELECT timestamp, close
        FROM vix_daily
        WHERE timestamp BETWEEN %s AND %s 
        ORDER BY timestamp ASC
    """, (start_date, end_date))
    vix_data = cursor.fetchall()
    cursor.execute("""
        SELECT timestamp, close
        FROM candlestick_daily
        WHERE timestamp BETWEEN %s AND %s AND symbol = %s
        ORDER BY timestamp ASC
    """, (start_date, end_date, symbol))
    
    candlestick_data = cursor.fetchall()
    candlestick_i = 0
    obs_date = None
    actions = []
    for (timestamp, vix) in vix_data:
        while candlestick_i < len(candlestick_data) and candlestick_data[candlestick_i][0] < timestamp:
            candlestick_i = candlestick_i + 1
        if candlestick_i >= len(candlestick_data):
            break

        (candlestick_timestamp, price) = candlestick_data[candlestick_i]
        if obs_date == None and vix >= buy_point:
            actions.append((candlestick_timestamp, StockAction.BUY, price))
            obs_date = candlestick_timestamp
        if obs_date != None and (timestamp - obs_date) >= timedelta(days=observation_period):
            actions.append((candlestick_timestamp, StockAction.SELL, price))
            obs_date = None
    return actions


def sentiment_trader_antivix_5min(con, buy_point, observation_period, symbol, start_date: datetime, end_date: datetime):
    assert(end_date <= datetime(2021, 10, 1, tzinfo=timezone.utc))
    with con.cursor() as cursor:
        cursor.execute("""
            SELECT timestamp, close
            FROM vix_5min
            WHERE timestamp BETWEEN %s AND %s 
            ORDER BY timestamp ASC
        """, (start_date, end_date))
        vix_data = cursor.fetchall()
        cursor.execute("""
            SELECT timestamp, close
            FROM candlestick_5min
            WHERE timestamp BETWEEN %s AND %s AND symbol = %s
            ORDER BY timestamp ASC
        """, (start_date, end_date, symbol))
        
        candlestick_data = cursor.fetchall()
        candlestick_i = 0
        obs_date = None
        actions = []
        for (timestamp, vix) in vix_data:
            while candlestick_i < len(candlestick_data) and candlestick_data[candlestick_i][0] < timestamp:
                candlestick_i = candlestick_i + 1
            if candlestick_i >= len(candlestick_data):
                break

            if (9 <= timestamp.hour <= 17):
                (candlestick_timestamp, price) = candlestick_data[candlestick_i]
                if obs_date == None and vix >= buy_point:
                    actions.append((candlestick_timestamp, StockAction.BUY, price))
                    obs_date = candlestick_timestamp
                if obs_date != None and (timestamp - obs_date) >= timedelta(days=observation_period):
                    actions.append((candlestick_timestamp, StockAction.SELL, price))
                    obs_date = None
        return actions

def buying_enclosed_vix_daily(con, buy_point, sell_point, start_date: datetime, end_date: datetime) -> List[Tuple[datetime, StockAction]]:
    assert(0 <= buy_point <= 100)
    assert(0 <= sell_point <= 100)
    assert(sell_point > buy_point)

    cursor = con.cursor()

    actions = []
    has_bought = False
    cursor.execute("""SELECT timestamp, close as vix
        FROM vix_daily
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """, (start_date, end_date))
    vix_data = cursor.fetchall()
    for (timestamp, vix) in vix_data:
        if has_bought:
            if vix >= sell_point:
                actions.append((date, StockAction.SELL))
                has_bought = False
        else:
            if vix <= buy_point:
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


def percent_gains(actions: List[Tuple[datetime, StockAction, float]]):
    gains = 0
    last_buying_price = None
    for timestamp, action, price in actions:
        assert(action in [StockAction.BUY, StockAction.SELL])

        if action == StockAction.BUY:
            assert(last_buying_price == None)
            last_buying_price = price
        elif action == StockAction.SELL:
            assert(last_buying_price != None)
            gains = gains + (price - last_buying_price) / last_buying_price
            last_buying_price = None
    return gains

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



def avg_min_loss(SYMBOL, start_date, end_date, actions_list = None, buy_point = 19, observation_period = 3, wins_only = False):
    con = historical_database()
    data = None
    with con.cursor() as cursor:
        #TODO: not getting data from earlier than oct 8
        cursor.execute("SELECT timestamp, close FROM candlestick_5min WHERE symbol = %s AND timestamp BETWEEN %s AND %s ORDER BY timestamp ASC", (SYMBOL, start_date, end_date))
        data = cursor.fetchall()
    actions = sentiment_trader_antivix_5min(con, buy_point, observation_period, SYMBOL, start_date, end_date)

    def chunk2(l):
        for i in range(0, len(l), 2):
            data = l[i:i+2]
            if len(data) == 2:
                yield data

    last_i = 0
    values = []
    for (buy, sell) in chunk2(actions):
        buy_timestamp, buy_action, buy_price = buy
        assert(buy_action == StockAction.BUY)
        sell_timestamp, sell_action, sell_price = sell
        assert(sell_action == StockAction.SELL)
        buy_i = last_i
        while data[buy_i][0] < buy_timestamp:
            buy_i = buy_i + 1
        sell_i = last_i
        while data[sell_i][0] < sell_timestamp:
            sell_i = sell_i + 1
        
        min_value = None
        for i in range(buy_i, sell_i):
            if min_value == None or data[i][1] < min_value:
                min_value = data[i][1]
        assert(min_value != None)
        if min_value != None and float(min_value) < 0.99 * float(buy_price) and (not wins_only or sell_price > buy_price):
            values.append((buy_timestamp, sell_timestamp, float((min_value - buy_price)/ buy_price) * 100))
        
        last_i = sell_i
    return values

def count_retests(con, symbol, start_date: datetime, end_date: datetime, look_forward: timedelta = timedelta(days=1)):
    data = None
    with con.cursor() as cursor:
        cursor.execute("""
            SELECT timestamp, open, high, low, close, volume FROM candlestick_5min
            WHERE symbol = %s AND timestamp BETWEEN %s AND %s
            ORDER BY TIMESTAMP ASC
        """, (symbol, start_date, end_date))
        data = cursor.fetchall()
    last_datetime = data[-1] - abs(look_forward)
    last_candlestick_index = None
    for i in range(len(data)-1, 0-1, -1):
        timestamp, open, high, low, close, volume = data[i]
        if last_datetime >= timestamp:
            last_candlestick_index = i
            break
    retest_counts = []
    for i in range(0, last_candlestick_index):
        start_timestamp, start_open, start_high, start_low, start_close, start_volume = data[i]
        prev_timestamp, prev_open, prev_high, prev_low, prev_close, prev_volume = data[i]
        retest_count = 0
        j = i + 1
        while j < last_candlestick_index and (data[j][0] - start_timestamp) <= look_forward:
            assert((data[j][0] - start_timestamp) > timedelta(days=0))
            cur_timestamp, cur_open, cur_high, cur_low, cur_close, cur_volume = data[j]

            if prev_high <= start_close <= cur_high or prev_close <= start_close <= cur_close:
                retest_count = retest_count + 1

            prev_timestamp, prev_open, prev_high, prev_low, prev_close, prev_volume = data[j]
            j = j + 1
        retest_counts.append((start_timestamp, retest_count))
    return retest_counts

def normalize_nd_array_(arr):
    width, height = arr.shape;

    assert(width >= 2)

    mins_ = [float('Inf')] * height
    maxs_ = [-float('Inf')] * height
    for i in range(width):
        for j in range(height):
            mins_[j] = min(arr[i][j], mins_[j])
            maxs_[j] = max(arr[i][j], maxs_[j])
    mins = np.array(mins_)
    maxs = np.array(maxs_)
    return (arr - mins) / (maxs - mins)

def normalize_nd_array(arr):
    width, height = arr.shape;

    assert(width >= 2)

    sums = [0] * height
    sums_squared = [0] * height
    for i in range(width):
        for j in range(height):
            sums[j] = sums[j] + float(arr[i][j])
            sums_squared[j] = sums_squared[j] + float(arr[i][j] ** 2)
    
    means = np.array(sums) / width
    stddev = np.sqrt((np.array(sums_squared) / width) - (means ** 2))
    arr_ = np.array([[float(arr[i][j]) for j in range(height)] for i in range(width)])
    ret =  (arr_ - means) / stddev
    return ret
    

def retests_early_low(con, symbol, timestamp: date):
    data = None
    start_date = datetime.combine(timestamp, time(9, 30), tzinfo=timezone.utc)
    end_date = datetime.combine(timestamp, time(16, 0, tzinfo=timezone.utc))
    with con.cursor() as cursor:
        cursor.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM candlestick_5min
            WHERE symbol = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp ASC
        """, (symbol, start_date, end_date))
        data = cursor.fetchall()
        if len(data) == 0:
            return None

    time_offset_i = 0
    while data[time_offset_i][0] < start_date + timedelta(hours=3):
        time_offset_i = time_offset_i + 1
    
    lowest_low_in_first_3_hours = min(data[:time_offset_i], key=lambda row: row[3])[3]
    retest_count = 0
    for i in range(time_offset_i, len(data)):
        prev_high = data[i-1][2]
        cur_low = data[i][3]
        if cur_low <= lowest_low_in_first_3_hours <= prev_high:
            retest_count = retest_count + 1
    first_3_hours = np.array(list(map(lambda row: [row[1], row[2], row[3], row[4], row[5]], data[:time_offset_i])))
    return (first_3_hours, retest_count)

def k_most_similar_starts(con, k, symbol, cur_date: date, start_date: date, end_date: date):
    cur_first_3_hours, cur_retest_count = retests_early_low(con, symbol, cur_date)
    normalized_cur_first_3_hours = normalize_nd_array(cur_first_3_hours)
    dates = []
    for i in range((end_date - start_date).days):
        train_date = start_date + timedelta(days=i)
        if train_date == cur_date:
            continue
        row = retests_early_low(con, symbol, train_date)
        if row == None:
            continue
        train_first_3_hours, train_retest_count = row
        normalized_train_first_3_hours = normalize_nd_array(train_first_3_hours)
        distance, path = fastdtw.dtw(normalized_cur_first_3_hours, normalized_train_first_3_hours)
        dates.append((distance, train_date))

    return sorted(dates, key=lambda row: row[0])[:k]

        
