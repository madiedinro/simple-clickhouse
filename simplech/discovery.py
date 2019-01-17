import re
import inspect
import arrow
import ujson
import datetime
from collections import defaultdict
from typing import List, Dict, Mapping, Set, Callable, Any
from pydantic import BaseModel
from .deltagen import DeltaGenerator


float_re = re.compile(r'^\-?\d+\.\d{1,8}$')
numeric_re = re.compile(r'^(\-?\d+\.)?\-?[\d]+$')
date_re = re.compile(r'^\d{2,4}[\-\.\/]\d{2,2}[\-\.\/]\d{2,4}')
datetime_re = re.compile(
    r'\d{1,4}[\-\.\/]\d{1,2}[\-\.\/]\d{1,4}[T\s]\d{1,2}:\d{1,3}:\d{1,2}')


weight = {
    str.__name__: 99,
    datetime.datetime.__name__: 88,
    datetime.date.__name__: 77,
    float.__name__: 66,
    int.__name__: 44,
}


type_map = defaultdict(lambda: 'Unknown')
type_map.update({
    str: 'String',
    float: 'Float64',
    int: 'UInt64',
    datetime.date: 'Date',
    datetime.datetime: 'DateTime'
})


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
        if v.count('.') == 0:
            return int
        if isfloat_re(v):
            return float
    return str


def final_choose(v_set):
    lst = list(v_set)
    if len(lst) == 0:
        return
    elif len(lst) == 1:
        return lst[0]
    else:
        mapped = [weight[x.__name__] for x in lst]
        maxi = mapped.index(max(mapped))
        return lst[maxi]

class uint64:
    """
    Class for unsigned int64
    """

class DeltaRunner:
    def __init__(self, ch, discovery, **kwargs):
        self.kwargs = kwargs
        self.ch = ch
        self.disco = discovery

    def __enter__(self):
        self.gen = DeltaGenerator(ch=self.ch, discovery=self.disco, **self.kwargs)
        return self.gen

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_value:
            self.ch.flush(self.disco.table)



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
        
        self.ch = ch
        self.table = table
        self.tc = TableDescription()
        
        self.tc.cols = self.discover_by_data(records, **kwargs)
        # By default all cols is dimensions
        self.tc.dimensions = set(self.tc.cols.keys())
        self.stat = {
            'push': 0
        }


    @property
    def date_field(self):
        return self.tc.date_field

    def discover_by_data(self, records, analyze_strings=True, limit=500):
        if isinstance(records, dict):
            records = dict.values()
        
        cols = dict()
        for i, d in enumerate(records):
            if not isinstance(d, dict):
                raise TypeError(
                    f'Wrong data type. Expected dict given {type(d)}')
            for k, v in d.items():
                t = type(v)
                if analyze_strings and t == str:
                    t = handle_string(v)
                cols[k] = cols.get(k, set())
                cols[k].add(t)
            if i == limit:
                break
        return cols

    def push(self, row):
        self.stat['push'] += 1
        return self.ch.push(self.table, row)

    def difference(self, d1, d2, dimensions_criteria=None):
        return DeltaRunner(discovery=self, ch=self.ch, d1=d1, d2=d2, dimensions_criteria=dimensions_criteria)

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

    def dimensions(self, *args):
        for f in args:
            if f not in self.tc.cols:
                raise KeyError(f'Key {f} not found')
            self.tc.dimensions.update(args)
            self.tc.metrics = set(self.tc.cols.keys()) - self.tc.dimensions
        return self

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

    def get_metrics_funcs(self):
        return {k: final_choose(self.tc.cols[k]) for k in self.get_metrics()}

    def cols(self):
        return list(self.tc.cols.keys())

    @property
    def columns(self):
        return list(self.tc.cols.keys())

    def final_cols(self):
        return {k: final_choose(t) for k, t in self.tc.cols.items()}

    def merge_tree(self, execute=False):
        """
        Generate ClickHouse MergeTree create statement
        """
        idx = ', '.join([f'`{f}`' for f in self.tc.idx or []])
        query = f'CREATE TABLE IF NOT EXISTS `{self.table}` (\n'
        query += ",\n".join([f'  `{f}`  {type_map[t]}' for f,
                             t in self.final_cols().items()]) + '\n'
        query += f') ENGINE MergeTree() PARTITION BY toYYYYMM(`{self.tc.date_field}`) ORDER BY ({idx}) SETTINGS index_granularity={self.tc.index_granularity}\n'
        if execute == True:
            return self.ch.run(query)
        return query
