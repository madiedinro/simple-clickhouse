import http.client
import urllib.parse
import ujson as json
from collections import defaultdict
import requests
import sys

class ClickHouse:

    def __init__(self, host='localhost', db='default'):

        self.host = host
        self.db = db
        self.buffer = defaultdict(list)
        self.buffer_i = defaultdict(int)

        self.buffer_limit = 50000

    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def flush(self, table):

        self.ch_insert(table, self.buffer[table])
        self.buffer[table] = []
        self.buffer_i[table] = 0

    def push(self, table, doc):

        try:
            doc = json.dumps(doc, ensure_ascii=False)

        except Exception as e:

            print("Error:", sys.exc_info()[0])
            print('err: ', str(e))
            print('doc:', doc)

            raise e

        self.buffer[table].append(doc)
        self.buffer_i[table] += 1

        if self.buffer_i[table] == self.buffer_limit:

            self.flush(table)

    def ch_insert(self, table, docs):

        conn = http.client.HTTPConnection(self.host+":8123")
        body = ''
        for doc in docs:
            body += doc + '\n'

        params = urllib.parse.urlencode({'query': 'INSERT INTO ' + table + ' FORMAT JSONEachRow', 'database': self.db})
        url = "?" + params
        conn.request("POST", url, body.encode())
        result = conn.getresponse()
        result.read()

        if result.status != 200:
            raise Exception('ClickHouse error:' + result.reason)

    def select(self, query):

        conn = http.client.HTTPConnection(self.host+":8123")

        params = urllib.parse.urlencode({'query': query, 'database': self.db})
        url = "?" + params
        conn.request("GET", url)
        result = conn.getresponse()
        res = result.read()

        if result.status != 200:
            print(res)
            raise Exception('ClickHouse error:' + result.reason)

        return res

    def select_stream(self, query):

        r = requests.get('http://' +self.host+":8123", params={'query': query, 'database': self.db}, stream=True)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r


    def run(self, query):

        conn = http.client.HTTPConnection(self.host+":8123")

        params = urllib.parse.urlencode({'query': query, 'database': self.db})
        url = "?" + params
        conn.request("POST", url)
        result = conn.getresponse()
        res = result.read()

        if result.status != 200:
            print(res)
            raise Exception('ClickHouse error:' + result.reason)

        return res

