from collections import defaultdict
import logging
import sys
import http.client
import urllib.parse
import random
import sys
import ujson

ITER_CHUNK_SIZE = 512
LOGGER_FORMAT = '%(asctime)s %(levelname)s %(message)s'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter(LOGGER_FORMAT)
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


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
    def __init__(self, host='localhost', port=8123, db='default', user=None, password=None, session_id=None):
        self.host = host
        self.port = port
        self.db = db
        self.base_url = f"{host}:{port}"
        self.buffer = defaultdict(str)
        self.buffer_i = defaultdict(int)
        self.session_id = session_id
        self.user = user
        self.password = password
        self.buffer_limit = 1000
    
    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def get_params(self, query):
        params = {
            'query': query,
            'database': self.db
        }
        if self.session_id: 
            params['session_id'] = self.session_id
        if self.user: 
            params['user'] = self.user
        if self.password: 
            params['password'] = self.password
        return params

    def __get_conn(self):
        logger.debug('Conn base url: %s', self.base_url)
        return http.client.HTTPConnection(self.base_url)

    def __get_query(self, sql_query):
        return urllib.parse.urlencode(self.get_params(sql_query))

    def flush(self, table):
        self.ch_insert(table, self.buffer[table])
        self.buffer[table] = ''

    def push(self, table, doc, jsonDump=True):
        try:
            if jsonDump == True:
                doc = ujson.dumps(doc, ensure_ascii=False)
        except Exception:
            logger.exception('exc during push')
            raise e
        self.buffer[table] += doc + '\n'
        self.buffer_i[table] += 1
        if self.buffer_i[table] % self.buffer_limit == 0:
            self.flush(table)

    def ch_insert(self, table, body):
        conn = self.__get_conn()
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        query_str = self.__get_query(sql_query)
        conn.request("POST", f"/?{query_str}", body.encode())
        response = conn.getresponse()
        content = response.read()
        if response.status != 200:
            logger.error('Wrong HTTP statusCode %s. Return: %s', response.status, content)
            raise Exception(f'ClickHouse error {response.reason} (HTTP {response.status})')

    def post_raw(self, table, data):
        conn = self.__get_conn()
        sql_query = 'INSERT INTO {table} FORMAT JSONEachRow'.format(table=table)
        query_str = self.__get_query(sql_query)
        conn.request("POST", f"/?{query_str}", data)
        response = conn.getresponse()
        content = response.read()
        if response.status != 200:
            logger.error('Wrong HTTP statusCode %s. Return: %s', response.status, content)
            raise Exception(f'ClickHouse error {response.reason} (HTTP {response.status})')

    def __make_query(self, sql_query):
        conn = self.__get_conn()
        query_str = self.__get_query(sql_query)
        logger.debug('Query string: %s', query_str)
        conn.request("GET", f"/?{query_str}")
        return conn.getresponse()

    def select(self, sql_query):
        response = self.__make_query(sql_query)
        content = response.read()
        if response.status != 200:
            logger.error('Wrong HTTP statusCode %s. Return: %s', response.status, content)
            raise Exception(f'ClickHouse error {response.reason} (HTTP {response.status})')
        return content

    def select_stream(self, sql_query):
        response = self.__make_query(sql_query)
        if response.status != 200:
            content = response.read()
            logger.error('Wrong HTTP statusCode %s. Return: %s', response.status, content)
            raise Exception(f'ClickHouse error {response.reason} (HTTP {response.status})')
        return HTTPResp(response)

    def run(self, sql_query):
        conn = self.__get_conn()
        query_str = self.__get_query(sql_query)
        conn.request("POST", f"/?{query_str}")
        response = conn.getresponse()
        content = response.read()
        if response.status != 200:
            logger.error('Wrong HTTP statusCode %s. Return: %s', response.status, content)
            raise Exception(f'ClickHouse error {response.reason} (HTTP {response.status})')
        return content
