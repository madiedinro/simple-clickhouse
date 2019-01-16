import ujson


class DeltaGenerator:
    def __init__(self, discovery, ch, d1, d2, dimensions_criteria=None):
        
        """
        Restrictions
        Work only with additionable and substractable metrics.
        do not store calculable values like CTR. 
        """
        
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

    def run(self, data):
        dims = self.disco.get_dimensions()
        metrics = self.disco.get_metrics()
        metricsf = self.disco.get_metrics_funcs()        
        def dim_key(row):
            key = ''
            for k in dims:
                key += ':'+str(row[k]) 
            return key

        def def_metric(m, row):
            return metricsf[m]() if row.get(m) == None else metricsf[m](row[m])
        
        def metrics_diff(new, old):
            delta = {}
            delta_zise = 0
            for m in metrics:
                delta[m] = def_metric(m, new) - def_metric(m, old)
                delta_zise += abs(delta[m])
            return delta if delta_zise > 0 else None
        
        def negative_row(row):
            for m in metrics:
                row[m] = -row[m]
            return row
        
        for row in data:
            self.recs_map[dim_key(row)] = row

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
        for row in self.ch.objects_stream(q):
            key = dim_key(row)
            new_row = self.recs_map.get(key)
            if new_row:
                self.handled_keys.append(key)
                delta = metrics_diff(new_row, row)
                if delta:
                    correct_row = new_row.copy()
                    correct_row.update(delta)
                    yield correct_row
            else:
                rm_row = negative_row(row)
                yield rm_row
        for new_key in set(self.recs_map.keys()) - set(self.handled_keys):
            row = self.recs_map.get(new_key)
            yield row
