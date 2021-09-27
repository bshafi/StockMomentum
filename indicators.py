
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
            

