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
logger.setLevel(logging.WARN)
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

    def __make_query(self, sql_query, body=None, method=None):
        conn = self.__get_conn()
        query_str = self.__get_query(sql_query)
        logger.debug('Query string: %s', query_str)
        if not method:
            method = 'POST' if body else 'GET'
        conn.request(method, f"/?{query_str}", body=body)
        response = conn.getresponse()
        if response.status != 200:
            content = response.read()
            logger.error('Wrong HTTP statusCode %s. Return: %s', response.status, content)
            raise Exception(f'ClickHouse HTTP Error')
        return response

    def flush(self, table):
        """
        Flushing buffer to DB
        """
        self.ch_insert(table, self.buffer[table])
        self.buffer[table] = ''

    def push(self, table, doc, jsonDump=True):
        """
        Add document to upload chunk
        """
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
        self.__make_query(sql_query, body=body.encode())

    def post_raw(self, table, data):
        conn = self.__get_conn()
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        self.__make_query(sql_query, body=data)

    def select(self, sql_query):
        return self.__make_query(sql_query).read()

    def select_stream(self, sql_query):
        response = self.__make_query(sql_query)
        return HTTPResp(response)

    def run(self, sql_query):
        return self.__make_query(sql_query, method='POST').read()
