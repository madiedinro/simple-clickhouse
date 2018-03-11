import http.client
import urllib.parse
from collections import defaultdict
import requests
import random
import sys
import json

class ClickHouse:

    def __init__(self, host='localhost', db='default', session_id=None):

        self.host = host
        self.db = db
        self.base_url = self.host+":8123"
        self.buffer = defaultdict(str)
        self.buffer_i = defaultdict(int)
        self.session_id = session_id
        self.buffer_limit = 3000

    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def get_params(self, query):
        params =  {
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
        self.buffer_i[table] = 0

    def push(self, table, doc, jsonDump=True):

        try:
            if jsonDump == True:
                doc = json.dumps(doc, ensure_ascii=False)

        except Exception as e:

            print("Error:", sys.exc_info()[0])
            print('err: ', str(e))
            print('doc:', doc)
            raise e

        self.buffer[table] += doc + '\n'
        self.buffer_i[table] += 1

        if self.buffer_i[table] == self.buffer_limit:
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



    def select(self, query):

        conn = http.client.HTTPConnection(self.base_url)
        params = "?" + urllib.parse.urlencode(self.get_params(query))
        conn.request("GET",  params)
        result = conn.getresponse()
        res = result.read()

        if result.status != 200:
            print(res)
            raise Exception('ClickHouse error:' + result.reason)
        return res


    def select_stream(self, query):

        r = requests.get('http://' +self.base_url, params=self.get_params(query), stream=True)
        if r.status_code != requests.codes.ok:
            print(r.text)
            r.raise_for_status()
        return r


    def run(self, query):

        conn = http.client.HTTPConnection(self.base_url)
        url = "?" + urllib.parse.urlencode(self.get_params(query))
        conn.request("POST", url)
        result = conn.getresponse()
        res = result.read()

        if result.status != 200:
            print(res)
            raise Exception('ClickHouse error:' + result.reason)

        return res
