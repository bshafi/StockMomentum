from typing import Iterable, Tuple
from ipycanvas import Canvas
from datetime import date, datetime, time, timedelta, timezone
from ipycanvas.canvas import hold_canvas
from ipycanvas import Canvas
from threading import Event, Thread

from traitlets.traitlets import Enum

def draw_ticker(ticker, canvas):
    pass

class TimeFrame(Enum):
    MINUTES_5 = 5 * 60 * 1000
    HOURS = 60 * 60 * 1000
    DAYS = 24 * HOURS
    MONTHS = 30 * DAYS

def choose_time_frame(delta_time_ms: float) -> TimeFrame:
    time_frames = [TimeFrame.MS, TimeFrame.SECONDS, TimeFrame.MINUTES, TimeFrame.HOURS, TimeFrame.DAYS, TimeFrame.WEEK, TimeFrame.MONTH]
    time_frames_gt_10 = filter(lambda x: delta_time_ms / x > 10, time_frames)
    return max(time_frames_gt_10)

def consolidate(stocks: Iterable[Tuple[datetime, float, float, float, float]]) -> Tuple[datetime, float, float, float, float]:
    first_date = None
    first_open = None
    last_close = None
    min_low = None
    max_high = None
    for (timestamp, open, high, low, close) in stocks:
        if first_date == None:
            first_date = timestamp
        if first_open == None:
            first_open = open
        last_close = close
        if min_low == None or min_low > low:
            min_low = low
        if max_high == None or max_high < high:
            max_high = high
    return (first_date, first_open, max_high, min_low, last_close)



def draw_stocks(data: Iterable[Tuple[datetime, float, float, float, float]], canvas: Canvas, min_x: datetime, min_y: float, max_y: float):
    CANDLE_STICK_WIDTH_PX = 20
    max_x = min_x + timedelta(minutes=5) * (canvas.width / CANDLE_STICK_WIDTH_PX)
    for (timestamp, open, high, low, close) in data:
        if min_x > timestamp or  timestamp > max_x:
            continue
        
        time_range_ms = (max_x.timestamp() - min_x.timestamp())
        time_off_of_cur = (timestamp.timestamp() - min_x.timestamp())
        x1 = (time_off_of_cur / time_range_ms) * canvas.width
        # TODO: Update this later
        # \/ Assumes it a 5min chart
        x2 = ((time_off_of_cur + 5 * 60) / time_range_ms) * canvas.width
        width = x2 - x1
        y_low = canvas.height - ((low - min_y) / (max_y - min_y)) * canvas.height
        y_high = canvas.height - ((high - min_y) / (max_y - min_y)) * canvas.height
        y_open = canvas.height - ((open - min_y) / (max_y - min_y)) * canvas.height
        y_close = canvas.height - ((close - min_y) / (max_y - min_y)) * canvas.height

        canvas.fill_style = 'green';
        canvas.stroke_style = 'green'
        height = abs(y_close - y_open)
        top = y_close
        if open > close:
            canvas.fill_style = 'red'
            canvas.stroke_style = 'red'
        canvas.stroke_line((x1 + x2) / 2, y_high, (x1 + x2) / 2, y_low)
        canvas.fill_rect(x1 + width / 10, top, width - (width / 5), height)
        

    
class StockChartCanvas:
    def __init__(self, canvas: Canvas, data):
        self.data = data
        self.canvas = canvas
        self.mouse_down = False
        self.x_offset = data[0][0]
        self.y_offset = 0
        self.prev_pos = (0, 0)

        self.canvas.on_client_ready(lambda: self.redraw)
        self.canvas.on_mouse_down(lambda x, y: self._mouse_down(x, y))
        self.canvas.on_mouse_up(lambda x, y: self._mouse_up(x, y))
        self.canvas.on_mouse_move(lambda x, y: self._mouse_move(x, y))
        self.canvas.on_mouse_out(lambda x, y: self._mouse_out(x, y))
        self.stopped = Event()
        self.event_loop = Thread(target=lambda: self._update())
        pass

    def start(self):
        self.event_loop.start()

    def stop(self):
        self.stopped.set()
        self.event_loop.join()

    def _update(self):
        while not self.stopped.wait(1/60):
            self.redraw()

    def redraw(self):
        with hold_canvas(self.canvas):
            self.canvas.clear()
            draw_stocks(self.data, self.canvas, self.x_offset, self.y_offset + 135, self.y_offset + 140)
    
    def _mouse_down(self, x, y):
        self.mouse_down = True
    
    def _mouse_up(self, x, y):
        self.mouse_down = False

    def _mouse_out(self, x, y):
        self.mouse_down = False

    def _mouse_move(self, x, y):
        if self.mouse_down:
            self.x_offset = self.x_offset + timedelta(minutes=(x - self.prev_pos[0]))
            self.y_offset = self.y_offset + (y - self.prev_pos[1]) / 100
            self.prev_pos = (x, y)
