import re
import inspect
import arrow
import ujson
import datetime
from collections import defaultdict
from typing import List, Dict, Mapping, Set, Callable, Any
from pydantic import BaseModel


float_re = re.compile(r'^\d+\.\d{1,8}$')
numeric_re = re.compile(r'^(\d+\.)?[\d]+$')
date_re = re.compile(r'^\d{1,4}[\-\.\/]\d{1,2}[\-\.\/]\d{1,4}')
datetime_re = re.compile(r'\d{1,4}[\-\.\/]\d{1,2}[\-\.\/]\d{1,4}[T\s]\d{1,2}:\d{1,3}:\d{1,2}')

def is_date(v):
    return isinstance(v, datetime.date) or v == datetime.date

def isfloat_re(v):
    return bool(float_re.match(v))

def isnumeric_re(v):
    return bool(numeric_re.match(v))

def isdate_dirty_re(v):
    return bool(date_re.match(v))

def isdatetime_dirty_re(v):
    return bool(datetime_re.match(v))

def handle_string(v):
    if isdate_dirty_re(v):
        try:
            dt = arrow.get(v)
            dtt = dt.time()
            if dtt.hour == 0 and dtt.minute == 0 and dtt.second == 0:
                return datetime.date
            else:
                return datetime.datetime
        except:
            pass
    if isnumeric_re(v):
        if v.isdecimal():
            return int
        if isfloat_re(v):
            return float
    return str

weight = {
    str: 99,
    datetime.datetime: 88,
    datetime.date: 77,
    float: 66,
    int: 44,
}


type_map = defaultdict(lambda: 'Unknown')
type_map.update({
    str: 'String',
    float: 'Float64',
    int: 'UInt64',
    datetime.date: 'Date',
    datetime.datetime: 'DateTime'
})

def final_choose(v_set):
    lst = list(v_set)
    if len(lst) == 0:
        return
    elif len(lst) == 1:
        return lst[0]
    else:
        return lst.index(max(lst))


class TableDescription(BaseModel):
    table: str = None
    date_field: str = None
    index_granularity: int = 8192
    cols: Mapping[str, Set[Any]] = dict()
    idx: List[str] = None
    metrics: Set[str] = set()
    dimensions: Set[str] = set()


class TableDiscovery:
    def __init__(self, records, table, ch=None, **kwargs):
        """
        arguments:
        records - one record (dict) or list of records (list[dict])
        limit - discover by only x records
        """

        if not isinstance(records, list):
            records = [records]
        
        self.tc = TableDescription()
        self.ch = ch
        self.tc.table = table
        self.analize(records, **kwargs)
    
    @property
    def date_field(self):
        return self.tc.date_field

    @property
    def table(self):
        return self.tc.table

    def analize(self, records, analyze_strings=True, limit=500):
        for i, d in enumerate(records):
            if not isinstance(d, dict):
                raise TypeError(f'Wrong data type. Expected dict, but given {type(d)}')
            for k, v in d.items():
                t = type(v)
                if analyze_strings and t == str:
                    t = handle_string(v)
                self.tc.cols[k] = self.tc.cols.get(k, set())
                self.tc.cols[k].add(t)
            if i == limit:
                break

    def date(self, *args):
        for k in args:
            self.set(k, datetime.date)
        return self
    
    def float(self, *args):
        for k in args:
            self.set(k, float)
        return self
    
    def int(self, *args):
        for k in args:
            self.set(k, int)
        return self

    def idx(self, *args):
        for f in args:
            if f not in self.tc.cols:
                raise KeyError(f'Key {f} not found')
        self.tc.idx = list(args)
        return self

    def get_dimensions(self):
        if len(self.tc.dimensions):
            return list(self.tc.dimensions)
        raise ValueError('Dimensions not yet defined')

    def get_metrics(self):
        if len(self.tc.metrics):
            return list(self.tc.metrics)
        raise ValueError('Metrics not yet defined')

    def metrics(self, *args):
        for f in args:
            if f not in self.tc.cols:
                raise KeyError(f'Key {f} not found')
            self.tc.metrics.update(args)
            self.tc.dimensions = set(self.tc.cols.keys()) - self.tc.metrics
        return self
    
    def set(self, *args, set_main=False, **kwargs):
        """
        Possible to user classes int, str, float and values 1, 'val', 1.0
        For date can set is main
        """
        
        from_args = dict(zip(args[::2], args[1::2]))
        kwargs.update(from_args)

        for key, type_py in kwargs.items():            
            if key not in self.tc.cols:
                raise KeyError(f'Key {key} not found')
            if not inspect.isclass(type_py):
                type_py = type(type_py)
            self.tc.cols[key] = set([type_py])        
            if is_date(type_py):
                if not self.tc.date_field or set_main:
                    self.tc.date_field = key
        return self
    
    @property
    def config(self):
        return self.tc
    
    def __repr__(self):
        return "<Instance of {} class, value={}>".format(self.__class__.__name__, self.config)

    def __str__(self):
        return self.merge_tree()
    
    def drop(self, execute=False):
        query = f'DROP TABLE IF EXISTS `{self.table}`\n'
        if execute == True:
            return self.ch.run(query)
        return query

    
    def final_cols(self):
        return {k: final_choose(t) for k, t in self.tc.cols.items()}

    def merge_tree(self, execute=False):
        idx = ', '.join([f'`{f}`' for f in self.tc.idx or []])
        query = f'CREATE TABLE IF NOT EXISTS `{self.table}` (\n'
        query += ",\n".join([f'  `{f}`  {type_map[t]}' for f, t in self.final_cols().items()]) + '\n'
        query += f') ENGINE MergeTree() PARTITION BY toYYYYMM(`{self.tc.date_field}`) ORDER BY ({idx}) SETTINGS index_granularity={self.tc.index_granularity}\n' 
        if execute == True:
            return self.ch.run(query)
        return query
        