import re
import datetime
import arrow
from .types import *


float_re = re.compile(r'^\-?\d+\.\d{1,8}$')
numeric_re = re.compile(r'^(\-?\d+\.)?\-?[\d]+$')
date_re = re.compile(r'^\d{2,4}[\-\.\/]\d{2,2}[\-\.\/]\d{2,4}')
datetime_re = re.compile(
    r'\d{1,4}[\-\.\/]\d{1,2}[\-\.\/]\d{1,4}[T\s]\d{1,2}:\d{1,3}:\d{1,2}')


def is_date(v):
    return isinstance(v, Date) or v == Date


def isfloat_re(v):
    return bool(float_re.match(v))


def isnumeric_re(v):
    return bool(numeric_re.match(v))


def isdate_dirty_re(v):
    return bool(date_re.match(v))


def isdatetime_dirty_re(v):
    return bool(datetime_re.match(v))


def cast_string(v):
    if isdate_dirty_re(v):
        try:
            dt = arrow.get(v)
            dtt = dt.time()
            if dtt.hour == 0 and dtt.minute == 0 and dtt.second == 0:
                return Date
            else:
                return DateTime
        except:
            pass
    if isnumeric_re(v):
        if v.count('.') == 0:
            return Int64
        if isfloat_re(v):
            return Float64
    return String


def max_type(counter):
    max_name = None
    max_value = 0
    for cname in counter:
        value = TYPES_PRIORITY[cname]
        if value > max_value:
            max_value = value
            max_name = cname
    return max_name
