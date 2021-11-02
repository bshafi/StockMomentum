import csv
from datetime import datetime
import psycopg2

credentials_file = None

def historical_database():
    return psycopg2.connect(open(credentials_file).read())

class ArgumentError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def get_msg(self):
        return self.msg

def iter_csv_rows_from_request(req, skip_first = True):
    rdr = csv.reader(req.content.decode('utf-8').splitlines(), delimiter=',')
    first_row = True
    second_row_checked = False
    for row in rdr:
        # Checks if the second row is empty
        # In that case return an empty iterator
        if not first_row and not second_row_checked:
            if all([elem == '' for elem in row]):
                break
    
        if first_row and skip_first:
            first_row = False
            continue

        yield row


def parse_date(s):
    date = None

    try:
        date = datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        pass
    if date != None:
        return date

        
    try:
        date = datetime.strptime(s, '%Y-%m-%d %H:%M')
    except ValueError:
        pass
    if date != None:
        return date


    try:
        date = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass
    
    if date != None:
        return date
    else:
        raise ValueError("Date was in an improper format")
