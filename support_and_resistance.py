
from datetime import timedelta, datetime, date, timezone
import itertools
from os import terminal_size
from typing import Set
import numpy as np
from itertools import product
import multiprocessing
import sm_util
sm_util.credentials_file = 'credentials/admin_credentials'
con = sm_util.historical_database()
from math import sqrt

# assumes x in [0, 1]
def how_long_out_of_bounds(m, b, avg_closes, stddef_closes):
    _x0 = (-stddef_closes + avg_closes - b) / m
    _x1 = (stddef_closes + avg_closes - b) / m
    x0 = min(_x0, _x1)
    if x0 < 0 or x0 > 1:
        x0 = 0
    x1 = max(_x0, _x1)
    if x1 < 0 or x1 > 1:
        x1 = 1
    return (x0 +  (1 - x1))

def _avg(arr, key=lambda x: x):
    sum = 0
    for i in range(len(arr)):
        sum = sum + key(arr[i])
    return sum / len(arr)

def extract_local_extrema(arr, key=lambda x: x, look_around=1):
    maxs = []
    mins = []
    for i in range(look_around, len(arr)-look_around):
        left = arr[i - look_around:i]
        center = arr[i]
        right = arr[i + 1: i + look_around + 1]
        if key(center) > _avg(left, key=key) and key(center) > _avg(right, key=key):
            maxs.append(center)
        if key(center) < _avg(left, key=key) and key(center) < _avg(left, key=key):
            mins.append(center)
    return (maxs, mins)

def error_two_lines(start, stop, m0, b0, m1, b1):
    C = (m0 - m1)
    D = (b0 - b1)
    F = lambda x: (1/3) * (C ** 2) * (x ** 3) + D * C * (x ** 2) + (D ** 2) * x
    return F(stop) - F(start)

def support_and_resistance(data, ERROR = 10):
    if sorted(data, key=lambda row: row[0].timestamp()) != data:
        raise ValueError("data is not sorted")
    ERROR = 7

    min_datetime = min(data, key=lambda row: row[0].timestamp())[0]
    min_timestamp = min_datetime.timestamp()
    max_datetime = max(data, key=lambda row: row[0].timestamp())[0]
    max_timestamp = max_datetime.timestamp()
    
    closes = [close for (timestamp, open, high, low, close) in data]
    avg_closes = np.average(closes)
    stddef_closes = np.std(closes)
    
        

    def normalize_timestamp(datetime_):
        return (datetime_.timestamp() - min_timestamp) / (max_timestamp - min_timestamp)

    (local_maxs, local_mins) = extract_local_extrema(data, lambda row: row[4])
    local_extrema = local_maxs + local_mins
    lines = []
    for i in range(len(local_extrema)):
        for j in range(i + 1, len(local_extrema)):
            a = local_extrema[i]
            a_x = ((a[0] - min_datetime) / (max_datetime - min_datetime))
            a_y = float(a[4])
            b = local_extrema[j]
            b_x = ((b[0] - min_datetime) / (max_datetime - min_datetime))
            b_y = float(b[4])
            
            slope = (a_y - b_y) / (a_x - b_x)
            intercept = a_y - a_x * slope
            
            def f(x):
                return x * slope + intercept
            
            pass_throughs = 0
            touches = 0

            count_below = 0
            count_total = 0
            for k in range(1, len(data) - 1):
                left = data[k - 1]
                left_x = normalize_timestamp(left[0])
                left_y = float(left[4])
                center = data[k]
                center_x = normalize_timestamp(center[0])
                center_y = float(center[4])
                right = data[k + 1]
                right_x = normalize_timestamp(right[0])
                right_y = float(right[4])
                left_diff = left_y - f(left_x)
                center_diff = center_y - f(center_x)
                right_diff = right_y - f(right_x)
                if center_y > f(center_x):
                    count_below = count_below + 1
                count_total = count_total + 1
                if abs(center_diff) < stddef_closes:
                    if left_diff * right_diff > 0:
                        touches = touches + 1
                    else:
                        pass_throughs = pass_throughs + 1
            out_of_bounds_time = how_long_out_of_bounds(slope, intercept, avg_closes, stddef_closes)
            lines.append((a, b, touches, pass_throughs, out_of_bounds_time, count_below / count_total))
    lines.sort(key=lambda row: row[5])
    lines = lines[:len(lines) // 2 + len(lines) % 2]
    lines.sort(key=lambda row: row[4])
    lines = lines[:len(lines) // 2 + len(lines) % 2]
    return sorted(lines, key=lambda row: row[3] - row[2])

# Only works for 1-dimensional values currently
class CodeBook:
    # key takes a datapoint and returns a float
    def __init__(self, ERROR=10):
        self.ERROR = ERROR
        self._means = []
        self._lengths = []
    
    def iterate(self, val):
        codebook_i = self.classify(val)
        if codebook_i != -1:
            if abs(self._means[codebook_i] - val) > self.ERROR:
                codebook_i = -1
        
        if codebook_i == -1:
            self._lengths.append(1)
            self._means.append(val)
        else:
            old_len = self._lengths[codebook_i]
            self._lengths[codebook_i] = self._lengths[codebook_i] + 1
            self._means[codebook_i] = (self._means[codebook_i] * old_len + val) / (old_len + 1)

    @property
    def means(self):
        return self._means
    # returns -1 if the means is empty
    def classify(self, val):
        return min(range(len(self._means)), key=lambda i: abs(self._means[i] - val), default= -1)
RESISTANCES_6_MONTH = [
    date(2012, 7, 12),
    date(2012, 12, 27),
    date(2013, 6, 20),
    date(2013, 10, 8),
    date(2014, 2, 3),
    date(2014, 10, 10),
    date(2014, 12, 12),
    date(2015, 1, 6),
    date(2015, 1, 13),
    date(2015, 1, 28),
    date(2015, 1, 30),
    date(2015, 7, 9),
    date(2015, 8, 24),
    date(2015, 12, 11),
    date(2016, 1, 7),
    date(2016, 2, 4),
    date(2016, 6, 24),
    date(2016, 11, 3),
    date(2017, 4, 12),
    date(2017, 5, 17),
    date(2017, 8, 10),
    date(2017, 8, 17),
    date(2018, 2, 5),
    date(2018, 3, 23),
    date(2018, 4, 24),
    date(2018, 5, 29),
    date(2018, 6, 25),
    date(2018, 6, 27),
    date(2018, 10, 11),
    date(2018, 10, 28),
    date(2018, 12, 24),
    date(2019, 5, 7),
    date(2019, 5, 13),
    date(2019, 6, 3),
    date(2019, 8, 5),
    date(2019, 8, 12),
    date(2019, 8, 14),
    date(2019, 8, 23),
    date(2019, 8, 30),
    date(2019, 10, 1),
    date(2019, 10, 8),
]

def hits_vix_resistance(data, LOOK_BACK=None, ERROR=None, RISE_AMOUNT=None, SKIP=None):
    if LOOK_BACK == None:
        LOOK_BACK = 10
    if ERROR == None:
        ERROR = 10
    if RISE_AMOUNT == None:
        RISE_AMOUNT = 5
    if SKIP == None:
        SKIP = 50
    codebook = CodeBook(ERROR=ERROR)
    for i in range(LOOK_BACK, len(data)):
        segment = data[i - LOOK_BACK:i + 1]
        center = segment[-1]
        before_center = min(segment[:-1], key=lambda row: row[4])
        if all([center[4] >= elem[4] for elem in segment]):
            if i - LOOK_BACK >= SKIP and any([center[4] >= mean for mean in codebook.means]) and center[4] - before_center[4] >= RISE_AMOUNT:
                yield (data[i+1][0], data[i+1][4])
            codebook.iterate(center[4])


train_data = RESISTANCES_6_MONTH[:len(RESISTANCES_6_MONTH) // 2]
test_data = RESISTANCES_6_MONTH[len(RESISTANCES_6_MONTH) // 2:]
def optimize_params(t):
    data, actual, LOOK_BACK, ERROR, RISE_AMOUNT, SKIP = t
    hits = list(hits_vix_resistance(data, LOOK_BACK=LOOK_BACK, ERROR=ERROR, RISE_AMOUNT=RISE_AMOUNT, SKIP=SKIP))
    hits_set = set([row[0].date() for row in hits])
    true_positive = hits_set.intersection(set(actual))
    false_positive = hits_set.difference(set(actual))
    false_negative = set(actual).difference(true_positive)
    f1_score = (2 * len(true_positive)) / (2 * len(true_positive) + len(false_positive) + len(false_negative))
    print(t[2:])
    print('F_1 score:', round(f1_score, 2))
    print('TP:', len(true_positive))
    print('FP:', len(false_positive))
    print('FN:', len(false_negative))
    return f1_score
if __name__ == "__main__":
    # From Feb 3 2014 - Nov 29 2021
    START_DATE = min(test_data)
    END_DATE = max(test_data)
    data = None
    with con.cursor() as cur:
        cur.execute("""
            SELECT timestamp, open, high, low, close
            FROM vix_daily
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp ASC
        """, (START_DATE, END_DATE))
        data = ([(timestamp, float(open), float(high), float(low), float(close)) for (timestamp, open, high, low, close) in cur.fetchall()])

    print(optimize_params((data, test_data, None, None, None, None)))
    """
    possible_args = list(itertools.product([data], range(0, 50), range(0, 50), range(0, 10), range(0, 100)))
    print(len(possible_args))
    if __name__ == '__main__':
        with multiprocessing.Pool(32) as pool:
            scores_and_args = zip(pool.map(optimize_params, possible_args), possible_args)
            best_score_and_arg = max(scores_and_args, key=lambda row: row[0])
            print(best_score_and_arg[0], best_score_and_arg[1][1:])
    """