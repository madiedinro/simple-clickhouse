import re
import inspect
import ujson
import datetime
from collections import defaultdict, Counter
from typing import List, Dict, Mapping, Set, Callable, Any
from pydantic import BaseModel
from .deltagen import DeltaGenerator
from .helpers import cast_string, is_date


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

    async def __aenter__(self):
        self.gen = DeltaGenerator(ch=self.ch, discovery=self.disco, **self.kwargs)
        return self.gen

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not exc_value:
            coro = self.ch.flush(self.disco.table)
            if coro:
                await coro

         


class TableDescription(BaseModel):
    table: str = None
    date_field: str = None
    index_granularity: int = 8192
    columns: Mapping[str, Any] = dict()
    idx: List[str] = None
    metrics_set: Set[str] = set()
    metrics: Mapping[str, Any] = dict()
    dimensions_set: Set[str] = set()
    dimensions: Mapping[str, Any] = dict()


class Guesstimator:
    pass



class TableDiscovery:
    def __init__(self, table, ch=None, records=None, **kwargs):
        """
        arguments:
        records - one record (dict) or list of records (list[dict])
        limit - discover by only x records
        """
        
        self.ch = ch
        self.table = table
        self.tc = TableDescription()
        self.fillfuled = False
        
        if records:
            self.tc.columns = self.discover_by_data(records, **kwargs)
            # By default all cols are dimensions
            self.tc.dimensions_set = set(self.tc.columns.keys())
            self.after_classification()
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
                    t = cast_string(v)
                cols[k] = cols.get(k, set())
                cols[k].add(t)
            if i == limit:
                break
        if i > 0:
            self.fillfuled = True
        cols = {key: final_choose(v_set) for key, v_set in cols.items()}
        return cols

    def push(self, row):
        self.stat['push'] += 1
        return self.ch.push(self.table, row)

    def difference(self, d1, d2, data, dimensions_criteria=None):
        return DeltaRunner(discovery=self, ch=self.ch, d1=d1, d2=d2, data=data, dimensions_criteria=dimensions_criteria)

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

    def str(self, *args):
        for k in args:
            self.set(k, str)
        return self

    def idx(self, *args):
        for f in args:
            if f not in self.tc.columns:
                raise KeyError(f'Key {f} not found')
        self.tc.idx = list(args)
        return self

    @property
    def columns(self):
        return self.tc.columns

    def get_dimensions(self):
        if len(self.tc.dimensions):
            return self.tc.dimensions
        raise ValueError('Dimensions not yet defined')

    def get_metrics(self):
        if len(self.tc.metrics):
            return self.tc.metrics
        raise ValueError('Metrics not yet defined')

    def after_classification(self):
        self.tc.metrics = {c: self.tc.columns[c] for c in self.tc.metrics_set}
        self.tc.dimensions = {c: self.tc.columns[c] for c in self.tc.dimensions_set}

    def dimensions(self, *args):
        for f in args:
            if f not in self.tc.columns:
                raise KeyError(f'Key {f} not found')
            self.tc.dimensions_set.update(args)
            self.tc.metrics_set = set(self.tc.columns.keys()) - self.tc.dimensions_set
        self.after_classification()
        return self

    def metrics(self, *args):
        for f in args:
            if f not in self.tc.columns:
                raise KeyError(f'Key {f} not found')
            self.tc.metrics_set.update(args)
            self.tc.dimensions_set = set(self.tc.columns.keys()) - self.tc.metrics_set
        self.after_classification()
        return self

    def set(self, *args, set_main=False, **kwargs):
        """
        Possible to user classes int, str, float and values 1, 'val', 1.0
        For date can set is main
        """

        from_args = dict(zip(args[::2], args[1::2]))
        kwargs.update(from_args)
        for key, type_py in kwargs.items():
            # if key not in self.tc.columns:
            #     raise KeyError(f'Key {key} not found')
            if not inspect.isclass(type_py):
                type_py = type(type_py)
            self.tc.columns[key] = type_py
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

    def merge_tree(self, execute=False):
        """
        Generate ClickHouse MergeTree create statement
        """
        idx = ', '.join([f'`{f}`' for f in self.tc.idx or []])
        query = f'CREATE TABLE IF NOT EXISTS `{self.table}` (\n'
        query += ",\n".join([f'  `{f}`  {type_map[t]}' for f,  t in self.columns.items()]) + '\n'
        query += f') ENGINE MergeTree() PARTITION BY toYYYYMM(`{self.tc.date_field}`) ORDER BY ({idx}) SETTINGS index_granularity={self.tc.index_granularity}\n'
        if execute == True:
            return self.ch.run(query)
        return query
