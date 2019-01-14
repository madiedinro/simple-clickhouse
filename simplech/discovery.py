import re
import inspect
import arrow
import ujson
from datetime import datetime, date
from collections import defaultdict
from typing import List, Dict, Mapping


float_re = re.compile(r'^\d+\.\d{1,8}$')
numeric_re = re.compile(r'^(\d+\.)?[\d]+$')
date_re = re.compile(r'^\d{1,4}[\-\.\/]\d{1,2}[\-\.\/]\d{1,4}')
datetime_re = re.compile(r'\d{1,4}[\-\.\/]\d{1,2}[\-\.\/]\d{1,4}[T\s]\d{1,2}:\d{1,3}:\d{1,2}')

def is_date(v):
    return isinstance(v, date) or v == date

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
                return date
            else:
                return datetime
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
    datetime: 88,
    date: 77,
    float: 66,
    int: 44,
}


type_map = defaultdict(lambda: 'Unknown')
type_map.update({
    str: 'String',
    float: 'Float64',
    int: 'UInt64',
    date: 'Date',
    datetime: 'DateTime'
})

def final_choose(v_set):
    lst = list(v_set)
    if len(lst) == 0:
        return
    elif len(lst) == 1:
        return lst[0]
    else:
        return lst.index(max(lst))


class TableDiscovery:
    def __init__(self, records, table=None, analyze_strings=True, limit=100):
        """
        
        arguments:
        records - one record (dict) or list of records (list[dict])
        limit - discover by only x records
        """
        if not isinstance(records, list):
            records = [records]
            
        self.table = table
        self.fields = defaultdict(set)
        self.date_field = None
        self._idx = []
        self.analyze_strings = analyze_strings
        self.index_granularity = 8192
        
        for i, d in enumerate(records):
            if not isinstance(d, dict):
                raise TypeError(f'Wrong data type. Expected dict, but given {type(d)}')
            for k, v in d.items():
                t = type(v)
                if analyze_strings and t == str:
                    t = handle_string(v)
                self.fields[k].add(t)
            if i == limit:
                break
    
    def __getattr__(self, key):
        if key not in self.fields:
            raise KeyError(f'Key {key} not found.')
        def wrapper(type_py, set_main=False):
            return self.set(type_py, set_main)
            
        return wrapper

    def date(self, *args):
        for k in args:
            self.set(k, date)
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
            if f not in self.fields:
                raise KeyError(f'Key {f} not found')
        self._idx = args
        return self
    
    def set(self, *args, set_main=False, **kwargs):
        """
        Possible to user classes int, str, float and values 1, 'val', 1.0
        For date can set is main
        """
        
        from_args = dict(zip(args[::2], args[1::2]))
        kwargs.update(from_args)
        
        for key, type_py in kwargs.items():            
            
            if key not in self.fields:
                raise KeyError(f'Key {key} not found')
            
            if not inspect.isclass(type_py):
                type_py = type(type_py)
            
            self.fields[key] = set([type_py])        
            
            if is_date(type_py) == date:
                if not self.date_field or set_main:
                    self.date_field = key
        return self
    
    def config(self):
        fields = {f: final_choose(v) for f, v in self.fields.items()}
        datefields = [f for f, t in fields.items() if is_date(t)]
        date_field = self.date_field or (datefields[0] if len(datefields) else None)
        return {
            'cols': fields,
            'date_field': date_field,
            'idx': self._idx if len(self._idx) else ([date_field] if date_field else []),
            'table': self.table,
            'index_granularity': self.index_granularity
        }
    
    def __repr__(self):
        config = self.config()
        return "<Instance of {} class, value={}>".format(self.__class__.__name__, config)

    def __str__(self):
        return self.merge_tree()
    
    def drop(self):
        config = self.config()
        return f'DROP TABLE `{config["table"]}` IF EXISTS;'
    
    def merge_tree(self, drop=False):
        
        config = self.config()
        idx = ', '.join([f'`{f}`' for f in config.get('idx')])
        sql = f'CREATE TABLE `{config["table"]}` (\n'
        sql += ",\n".join([f'  `{f}`  {type_map[t]}' for f, t in config['cols'].items()]) + '\n'
        sql += f') ENGINE MergeTree() PARTITION BY toYYYYMM(`{config["date_field"]}`) ORDER BY ({idx}) SETTINGS index_granularity={config["index_granularity"]};\n' 
        return sql
        