
from typing import Iterable, Tuple
from datetime import datetime


def candlestick_is_red(candlestick: Tuple[datetime, float, float, float, float]):
    timetamp, open, high, low, close = candlestick
    return open > close

def candlestick_is_green(candlestick: Tuple[datetime, float, float, float, float]):
    return not candlestick_is_red(candlestick)

def niner(data: Iterable[Tuple[datetime, float, float, float, float]]):
    red_else_green = True
    chain = []
    for candlestick in data:
        if candlestick_is_red(candlestick) == red_else_green:
            chain.append(candlestick)
        else:
            if len(chain) >= 9:
                yield chain
            chain = []
            red_else_green = candlestick_is_red(candlestick)
            chain.append(candlestick)
            
def tweezer_bottom(data: Iterable[Tuple[datetime, float, float, float, float]]):
    prev = None
    ERROR = 0.01
    for (timestamp, open, high, low, close) in data:
        if prev == None:
            prev = (timestamp, open, high, low, close)
            continue
        prev_timestamp, prev_open, prev_high, prev_ow, prev_close = prev
        if abs(prev_close - open) < ERROR:
            yield prev
        prev = (timestamp, open, high, low, close)
        
#def bullish_divergence(data: Iterable[Tuple[datetime, float, float, float, float, float]]):


def price_opens_lower_than_prev(data: Iterable[Tuple[datetime, float, float, float, float]]):
    prev = None
    for (timestamp, open, high, low, close) in data: 
        if prev == None:
            prev = (timestamp, open, high, low, close)
            continue
        prev_timestamp, prev_open, prev_high, prev_ow, prev_close = prev
        if open < prev_open:
            yield prev
        prev = (timestamp, open, high, low, close)

        
