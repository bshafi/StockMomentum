import csv
from datetime import date, datetime
from typing import Dict
import psycopg2
import numpy as np

credentials_file = None

def historical_database():
    return psycopg2.connect(open(credentials_file).read())

class ArgumentError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def get_msg(self):
        return self.msg

def check_args(args, arg_requirements):
    arg_errors = []
    parsed_args = {}
    for (key, parse_arg) in arg_requirements.items():
        try:
            arg = args.get(key, None)
            arg_value = parse_arg(arg)
            parsed_args[key] = arg_value
        except ArgumentError as arg_error:
            arg_errors.append((key, arg_error))
    if len(arg_errors) > 0:
        all_errors_str = str([key + ":" + arg_error.get_msg() + "\n" for key, arg_error in arg_errors])
        raise ArgumentError(all_errors_str)
    return parsed_args


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
        raise ArgumentError("Date was in an improper format")
