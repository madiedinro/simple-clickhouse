from collections import defaultdict
from time import time
import logging
import sys
import os
import http.client
import urllib.parse
import random
import sys
import ujson

ITER_CHUNK_SIZE = 512
LOGGER_FORMAT = '%(asctime)s %(levelname)s %(message)s'
FORMAT_EACHROW = ' FORMAT JSONEachRow'

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
    def __init__(self,
                 scheme='http',
                 host=None,
                 port=None,
                 db=None,
                 user=None,
                 password="",
                 session=False,
                 session_id="",
                 dsn="",
                 debug=False,
                 buffer_limit=1000):

        if debug:
            self.set_debug()

        def_params = (
            not host and not port and not db and not user and not password)
        dsn_lookup = (dsn
                      or os.getenv('CH_DSN', None)
                      or os.getenv('CLICKHOUSE_DSN', None))

        if dsn_lookup and def_params:

            logger.debug(f"using DSN {dsn_lookup}")

            parts = urllib.parse.urlparse(dsn_lookup)
            self.scheme = parts.scheme
            self.host = parts.hostname
            self.port = parts.port
            self.db = str(parts.path).strip('/')
            self.user = parts.username
            self.password = parts.password

        else:
            self.host = host or '127.0.0.1'
            self.port = port or 8123
            self.db = db or 'default'
            self.user = user
            self.password = password
            self.scheme = scheme

        self.base_url = f"{self.host}:{self.port}"
        self.buffer = defaultdict(str)
        self.buffer_i = defaultdict(int)
        self.session_id = session_id
        self.buffer_limit = buffer_limit

        if session:
            self.session_id = session or str(time())

    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def get_params(self, query):
        params = {'query': query, 'database': self.db}
        if self.session_id:
            params['session_id'] = self.session_id
        if self.user:
            params['user'] = self.user
        if self.password:
            params['password'] = self.password
        return params

    def __get_conn(self):
        logger.debug('Conn base url: %s', self.base_url)
        conn = http.client.HTTPConnection(self.base_url)
        if logger.level == logging.DEBUG:
            conn.set_debuglevel(logger.level)
        return conn

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
            logger.error('Wrong HTTP statusCode %s. Return: %s',
                         response.status, content)
            raise Exception(f'ClickHouse HTTP Error')
        logger.debug(
            f'Server response status: {response.status}, content-length: {response.length}')
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
        except Exception as e:
            logger.exception('exc during push')
            raise e
        self.buffer[table] += doc + '\n'
        self.buffer_i[table] += 1
        if self.buffer_i[table] % self.buffer_limit == 0:
            self.flush(table)

    def ch_insert(self, table, body):
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        self.__make_query(sql_query, body=body.encode())

    def post_raw(self, table, data):
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        self.__make_query(sql_query, body=data)

    def select(self, sql_query, decode=True):
        data = self.__make_query(sql_query).read()
        return data.decode() if decode else data

    def select_stream(self, sql_query):
        response = self.__make_query(sql_query)
        return HTTPResp(response)

    def lines_stream(self, sql_query):
        response = self.__make_query(sql_query)
        for line in HTTPResp(response).iter_lines():
            yield line

    def objects_stream(self, sql_query):
        response = self.__make_query(sql_query + FORMAT_EACHROW)
        for line in HTTPResp(response).iter_lines():
            if line:
                yield ujson.loads(line)

    def run(self, sql_query):
        return self.__make_query(sql_query, method='POST').read()

    @staticmethod
    def set_debug(level=logging.DEBUG):
        logger.setLevel(level)
