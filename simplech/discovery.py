import re
import inspect
import ujson
import datetime
from collections import defaultdict, Counter
from typing import List, Dict, Mapping, Set, Callable, Any
from pydantic import BaseModel
from .deltagen import DeltaGenerator, DeltaRunner
from .helpers import cast_string, is_date, max_type
from . import types as cht
from .log import logger

PYTOCH_MAP = {
    str: cht.String,
    float: cht.Float64,
    int: cht.Int64,
    datetime.date: cht.Date,
    datetime.datetime: cht.DateTime
}


class Guesstimator:
    pass


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


class TableDiscovery:

    def __init__(self, table, ch=None, records=None, columns=None, **kwargs):
        """
        arguments:
        table - table name
        ch - ClikHouse / AsyncClickHouse instance
        records - one record (dict) or list of records (list[dict])
        limit - discover by only x records
        """
        
        self.ch = ch
        self.table = table
        self.tc = TableDescription()
        self.fillfuled = False
        self._stat = {
            'push': 0,
            'used_rows': 0
        }

        if records:
            self.tc.columns = self.discover_by_data(records, **kwargs)
            # By default all cols are dimensions
            self.tc.dimensions_set = set(self.tc.columns.keys())
            self.after_classification()
        
        if columns:
            self.tc.columns = self.process_provided_config(columns)


    @property
    def stat(self):
        return self._stat

    @property
    def date_field(self):
        return self.tc.date_field

    def process_provided_config(self, columns):
        res = {}
        for cname, ctype in columns.items():
            if isinstance(ctype, str):
                ctype = getattr(cht, ctype)
            elif not hasattr(cht, ctype):
                raise TypeError('Wrong data type')
            res[cname] = ctype
        return res

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
                typech = PYTOCH_MAP.get(t)
                if typech:
                    t = typech
                if analyze_strings and (t == str or t == cht.String):
                    t = cast_string(v)
                cols[k] = cols.get(k, Counter())
                cols[k].update([t.__name__])
        if i > 0:
            self.fillfuled = True
            self._stat['used_rows'] = i
        
        return {cname: getattr(cht, max_type(counter)) for cname, counter in cols.items()}

    def push(self, row):
        self._stat['push'] += 1
        return self.ch.push(self.table, row)

    def difference(self, d1, d2, data, dimensions_criteria=None):
        return DeltaRunner(discovery=self, ch=self.ch, d1=d1, d2=d2, data=data, dimensions_criteria=dimensions_criteria)

    def date(self, *args):
        for k in args:
            self.set(k, cht.Date)
        return self

    def float(self, *args):
        for k in args:
            self.set(k, cht.Float64)
        return self

    def int(self, *args):
        for k in args:
            self.set(k, cht.Int64)
        return self

    def str(self, *args):
        for k in args:
            self.set(k, cht.String)
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

    def pycode(self, return_dimensions=True):
        """
        """
        date = ', '.join([f"'{d}'" for d in [self.date_field] if d != None])
        idx = ', '.join([f"'{i}'" for i in self.tc.idx if i != None])
        metrics = ', '.join([f"'{k}'" for k in self.get_metrics()])
        dimensions = ', '.join([f"'{k}'" for k in self.get_dimensions()])
        cols = '{' + ', '.join([f"\n        '{k}': '{t.__name__ }'" for k, t in self.columns.items()]) + '}'
        code = f"td_{self.table} = ch.discover('{self.table}', columns={cols})\\\n"
        code += f"    .metrics({metrics})\\\n"
        if return_dimensions:
            code += f"    .dimensions({dimensions})\\\n"
        code += f"    .date({date})\\\n"
        code += f"    .idx({idx})\n"
        return code


    def merge_tree(self, execute=False):
        """
        Generate ClickHouse MergeTree create statement
        """
        idx = ', '.join([f'`{f}`' for f in self.tc.idx or []])
        query = f'CREATE TABLE IF NOT EXISTS `{self.table}` (\n'
        query += ",\n".join([f'  `{f}`  {t.__name__}' for f,  t in self.columns.items()]) + '\n'
        query += f') ENGINE MergeTree() PARTITION BY toYYYYMM(`{self.tc.date_field}`) ORDER BY ({idx}) SETTINGS index_granularity={self.tc.index_granularity}\n'
        if execute == True:
            return self.ch.run(query)
        return query
