import http.client
import urllib.parse
from collections import defaultdict
import random
import sys
import ujson

ITER_CHUNK_SIZE = 512


class HTTPResp:
    def __init__(self, resp):
        self.resp = resp

    def iter_lines(self,
                   chunk_size=ITER_CHUNK_SIZE,
                   decode_unicode=None,
                   delimiter=None):
        pending = None
        while not self.resp.isclosed():
            chunk = self.resp.read(chunk_size)
            if pending is not None:
                chunk = pending + chunk

            if delimiter:
                lines = chunk.split(delimiter)
            else:
                lines = chunk.splitlines()
            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None
            for line in lines:
                yield line
        if pending is not None:
            yield pending


class ClickHouse:
    def __init__(self, host='localhost', db='default', session_id=None):
        self.host = host
        self.db = db
        self.base_url = self.host + ":8123"
        self.buffer = defaultdict(str)
        self.buffer_i = defaultdict(int)
        self.session_id = session_id
        self.buffer_limit = 1000

    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def get_params(self, query):
        params = {
            'query': query,
            'database': self.db,
            'session_id': self.session_id
        }
        if self.session_id is not None:
            params['session_id'] = session_id
        return params

    def flush(self, table):
        self.ch_insert(table, self.buffer[table])
        self.buffer[table] = ''

    def push(self, table, doc, jsonDump=True):
        try:
            if jsonDump == True:
                doc = ujson.dumps(doc, ensure_ascii=False)
        except Exception as e:
            print("Error:", sys.exc_info()[0])
            print('err: ', str(e))
            print('doc:', doc)
            raise e
        self.buffer[table] += doc + '\n'
        self.buffer_i[table] += 1
        if self.buffer_i[table] % self.buffer_limit == 0:
            self.flush(table)

    def ch_insert(self, table, body):
        conn = http.client.HTTPConnection(self.base_url)
        query = 'INSERT INTO {table} FORMAT JSONEachRow'.format(table=table)
        url = "?" + urllib.parse.urlencode(self.get_params(query))
        conn.request("POST", url, body.encode())
        result = conn.getresponse()
        resp = result.read()
        if result.status != 200:
            print(resp)
            raise Exception('ClickHouse error:' + result.reason)

    def post_raw(self, table, data):
        conn = http.client.HTTPConnection(self.base_url)
        query = 'INSERT INTO {table} FORMAT JSONEachRow'.format(table=table)
        url = "?" + urllib.parse.urlencode(self.get_params(query))
        conn.request("POST", url, data)
        result = conn.getresponse()
        resp = result.read()
        if result.status != 200:
            print(resp)
            raise Exception('ClickHouse error:' + result.reason)

    def __select_resp(self, query):
        conn = http.client.HTTPConnection(self.base_url)
        params = "?" + urllib.parse.urlencode(self.get_params(query))
        conn.request("GET", params)
        return conn.getresponse()

    def select(self, query):
        resp = self.__select_resp(query)
        res = resp.read()
        if resp.status != 200:
            print(res)
            raise Exception('ClickHouse error:' + resp.reason)
        return res

    def select_stream(self, query):
        resp = self.__select_resp(query)
        if resp.status != 200:
            res = resp.read()
            print(res)
            raise Exception('ClickHouse error:' + resp.reason)
        return HTTPResp(resp)

    def run(self, query):
        conn = http.client.HTTPConnection(self.base_url)
        query_str + urllib.parse.urlencode(self.get_params(query))
        conn.request("POST", f"{url}?{query_str}")
        result = conn.getresponse()
        res = result.read()
        if result.status != 200:
            print(res)
            raise Exception('ClickHouse error:' + result.reason)
        return res
