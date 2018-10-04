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

import asyncio
import aiohttp

ITER_CHUNK_SIZE = 512
LOGGER_FORMAT = '%(asctime)s %(levelname)s %(message)s'
FORMAT_EACHROW = ' FORMAT JSONEachRow'

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)
log_formatter = logging.Formatter(LOGGER_FORMAT)
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


class HTTPRespAsync:
    def __init__(self, resp):
        self.resp = resp

    async def iter_lines(self,
                         chunk_size=ITER_CHUNK_SIZE,
                         decode_unicode=None,
                         delimiter=None):
        pending = None
        while not self.resp.isclosed():
            chunk = await self.resp.content.read(chunk_size)
            if not chunk:
                break
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


class ClickHouseBase:
    def __init__(self,
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

        self.scheme = 'http'

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
            # temporary only http supported
            # self.scheme = parts.scheme
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
            

        self.base_url = f"{self.host}:{self.port}"
        self.buffer = defaultdict(str)
        self.buffer_i = defaultdict(int)
        self.session_id = session_id
        self.buffer_limit = buffer_limit
        self.timeout = 10
        self.flush_every = 5

        if session:
            self.session_id = session or str(time())

    @property
    def _get_stream_reader(self):
        pass

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

    def _make_query(self, sql_query, body=None, method=None, read=False, decode=True):
        pass

    def flush(self, table):
        """
        Flushing buffer to DB
        """
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        self._make_query(sql_query, body=self.buffer[table].encode())
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

    def post_raw(self, table, data):
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        self._make_query(sql_query, body=data)

    def select(self, sql_query, decode=True):
        return self._make_query(sql_query, read=True)

    def select_stream(self, sql_query):
        response = self._make_query(sql_query)
        return self._get_stream_reader(response)

    def lines_stream(self, sql_query):
        response = self._make_query(sql_query)
        for line in self._get_stream_reader(response).iter_lines():
            yield line

    def objects_stream(self, sql_query):
        response = self._make_query(sql_query + FORMAT_EACHROW)
        for line in self._get_stream_reader(response).iter_lines():
            if line:
                yield ujson.loads(line)

    def run(self, sql_query):
        return self._make_query(sql_query, method='POST', read=True)

    @staticmethod
    def set_debug(level=logging.DEBUG):
        logger.setLevel(level)


class ClickHouse(ClickHouseBase):

    @property
    def _get_stream_reader(self):
        return HTTPResp

    def _make_query(self, sql_query, body=None, method=None, read=False, decode=True):
        logger.debug('Conn base url: %s', self.base_url)
        conn = http.client.HTTPConnection(self.base_url)
        if logger.level == logging.DEBUG:
            conn.set_debuglevel(logger.level)

        query_str = urllib.parse.urlencode(self.get_params(sql_query))
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
        if read:
            data = response.read()
            return data.decode() if decode else data
        return response


class AsyncClickHouse(ClickHouseBase):

    @property
    def _get_stream_reader(self):
        return HTTPRespAsync

    async def _make_query(self, sql_query, body=None, method=None, read=False, decode=True):
        try:
            if not method:
                method = 'post' if body else 'get'
            async with aiohttp.ClientSession() as session:
                func = getattr(session, method.lower())
                logger.debug(
                    f"Making query to {self.base_url} with %s. timeout:{self.timeout}", self.get_params(sql_query))
                async with func(
                        self.scheme + '://' + self.base_url,
                        timeout=self.timeout,
                        params=self.get_params(sql_query),
                        data=body) as response:
                    if response.status == 200:
                        if read:
                            data = await response.read()
                            if decode:
                                return data.decode()
                            return data
                        return response
                    else:
                        content = await response.text()
                        logger.error('Wrong HTTP statusCode %s. Return: %s',
                                     response.status, content)
                        raise Exception(f'ClickHouse HTTP Error')
        except Exception:
            logger.exception('ch_query_exc')
