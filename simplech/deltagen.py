import ujson


class DeltaGenerator:
    def __init__(self, discovery, ch, d1, d2, data, dimensions_criteria=None):
        
        """
        Restrictions
        Work only with additionable and substractable metrics.
        do not store calculable values like CTR. 
        """
        
        self.data = data
        self.ch = ch
        self.disco = discovery
        self.d1 = d1
        self.d2 = d2
        self.extra_db_keys = []
        self.handled_keys = []
        self.recs_map = dict()
        self.dimensions_criteria = dimensions_criteria

    def push(self, row):
        return self.ch.push(self.disco.table, row)

    @staticmethod
    def dim_key(dims, row):
        key = ''
        for k in dims:
            key += ':'+str(row.get(k)) 
        return key

    @staticmethod
    def def_metric(mtype, m, row):
        return mtype() if row.get(m) == None else mtype(row[m])
    
    @classmethod
    def metrics_diff(cls, metrics, new, old):
        delta = {}
        delta_zise = 0
        for m, mtype in metrics.items():
            delta[m] = cls.def_metric(mtype, m, new) - cls.def_metric(mtype, m, old)
            delta_zise += abs(delta[m])
        return delta if delta_zise > 0 else None
    
    @classmethod
    def negative_row(cls, metrics, row):
        for m, mtype in metrics.items():
            row[m] = -1 * cls.def_metric(mtype, m, row)
        return row
    
    def prepare_query(self, dims, metrics):

        # Selecting current Data
        where = [
            f"`{self.disco.date_field}` >= '{self.d1}'",
            f"`{self.disco.date_field}` <= '{self.d2}'"
        ]
        if self.dimensions_criteria:
            for param, val in self.dimensions_criteria.items():
                val = ujson.dumps(val).replace('"', '\'')
                where.append(f'`{param}` == {val}')
        
        sfrom = f"`{self.disco.table}`"
        where = " AND ".join(where)
        select = ", ".join([f'`{f}`' for f in dims] + [f' sum(`{f}`) `{f}`' for f in metrics])
        groupby = ", ".join([f'`{f}`' for f in dims])

        q = f"SELECT {select} FROM {sfrom} WHERE {where} GROUP BY {groupby}"
        return q
    
    def handle_record(self, row, dimensions, metrics):
        key = self.dim_key(dimensions, row)
        new_row = self.recs_map.get(key)
        if new_row:
            self.handled_keys.append(key)
            delta = self.metrics_diff(metrics, new_row, row)
            if delta:
                correct_row = new_row.copy()
                correct_row.update(delta)
                return correct_row
        else:
            rm_row = self.negative_row(metrics, row)
            return rm_row

    def __iter__(self):
        dimensions = self.disco.get_dimensions()
        metrics = self.disco.get_metrics()
        for row in self.data:
            self.recs_map[self.dim_key(dimensions, row)] = row
        q = self.prepare_query(dimensions, metrics)

        for row in self.ch.objects_stream(q):
            rec = self.handle_record(row, dimensions, metrics)
            if rec:
                yield

        for new_key in set(self.recs_map.keys()) - set(self.handled_keys):
            row = self.recs_map.get(new_key)
            yield row 
    
    async def __aiter__(self):
        dimensions = self.disco.get_dimensions()
        metrics = self.disco.get_metrics()
        for row in self.data:
            self.recs_map[self.dim_key(dimensions, row)] = row
        q = self.prepare_query(dimensions, metrics)

        async for row in self.ch.objects_stream(q):
            rec = self.handle_record(row, dimensions, metrics)
            if rec:
                yield
        
        for new_key in set(self.recs_map.keys()) - set(self.handled_keys):
            row = self.recs_map.get(new_key)
            yield row 

    def run(self, data):
        metrics = self.disco.get_metrics()
        dimensions = self.disco.get_dimensions()
        for row in data:
            self.recs_map[self.dim_key(dimensions, row)] = row

        q = self.prepare_query(dimensions, metrics)
        
        for row in self.ch.objects_stream(q):
            key = self.dim_key(dimensions, row)
            new_row = self.recs_map.get(key)
            if new_row:
                self.handled_keys.append(key)
                delta = self.metrics_diff(metrics, new_row, row)
                if delta:
                    correct_row = new_row.copy()
                    correct_row.update(delta)
                    yield correct_row
            else:
                rm_row = self.negative_row(metrics, row)
                yield rm_row
        for new_key in set(self.recs_map.keys()) - set(self.handled_keys):
            row = self.recs_map.get(new_key)
            yield row
