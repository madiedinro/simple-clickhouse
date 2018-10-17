


from collections import defaultdict
from time import time
import logging
import os
import http.client
import urllib.parse
import ujson
import asyncio
import aiohttp

LOGGER_FORMAT = '%(asctime)s %(levelname)s %(message)s'
FORMAT_JSONEACHROW = ' FORMAT JSONEachRow'

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)
log_formatter = logging.Formatter(LOGGER_FORMAT)
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


def none_decoder(val):
    return val


def json_decoder(val):
    return ujson.loads(val)


def bytes_decoder(val):
    return val.decode()


decoders = {
    'none': none_decoder,
    'json': json_decoder,
    'bytes': bytes_decoder
}


class BaseClickHouse():
    """
    This module implements the ...

    # Arguments

    host: (str, None) Хост с clickhouse. Default to `127.0.0.1`.
    port: (int, None) Порт подключения. Default to `8123`.
    db: (str, None) Название базы данных. Default to `default`.
    user: (str, None) Имя пользователя. Default to `default`.
    password: (str, None) Пароль. Default to `""`.
    session: (bool) Использовать сессию. Идентификатор сессии генерируется автоматически. Default to `False`.
    session_id: (str, None) Идентификатор сессии взамен автоматически сгенериованного. Default to `None`.
    dsn: (str, None) Использовать для подключения DSN, например: `http://default@127.0.0.1:8123/stats`. Default to `None`. 
        При наличии переменной окружеения `CH_DSN` или `CLICKHOUSE_DSN` будет использовано ее значение.
    debug: (bool) Переключение логов в режим отладки. Default to `False`.
    buffer_limit: (int) Буффер записи на таблицу. При достижении будет произведена запись в БД. Default to `1000`.
    loop: (EventLoop, None) При необходимости указать конкретный loop (для асинхронной версии). Default to `None`.
    """
    def __init__(self,
                 host=None,
                 port=None,
                 db=None,
                 user=None,
                 password=None,
                 session=False,
                 session_id=None,
                 dsn=None,
                 debug=False,
                 loop=None,
                 buffer_size=1000):

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
        self.buffer_size = buffer_size
        self.timeout = 10
        self.flush_every = 5
        self.loop = loop
        self.session_id = session_id or str(time())

        # init loop and others
        self._init()

    def _init(self):
        pass

    def _build_params(self, query):
        params = {'query': query, 'database': self.db}
        if self.session_id:
            params['session_id'] = self.session_id
        if self.user:
            params['user'] = self.user
        if self.password:
            params['password'] = self.password
        return params

    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def flush(self, table):
        pass

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
        if self.buffer_i[table] % self.buffer_size == 0:
            self.flush(table)

    @staticmethod
    def set_debug(level=logging.DEBUG):
        logger.setLevel(level)


class AsyncClickHouse(BaseClickHouse):

    def _init(self):
        if not self.loop:
            self.loop = asyncio.get_event_loop()

    def flush(self, table):
        """
        Flush buffer of table
        """
        asyncio.ensure_future(self._do_flush(table))

    async def _do_flush(self, table):
        """
        Flushing buffer to DB
        """
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        buff = self.buffer[table].encode()
        self.buffer[table] = ''
        resp_data = await self.run(sql_query, data=buff)
        return resp_data

    async def run(self, sql_query, data=None, decoder=bytes_decoder):
        """
        Executes SQL code
        """
        async with aiohttp.ClientSession() as session:
            async with self._make_request(sql_query, session, body=data, method='POST') as response:
                return decoder(await response.read())

    async def select(self, sql_query, decoder=bytes_decoder):
        async with aiohttp.ClientSession() as session:
            async with self._make_request(sql_query, session) as response:
                return decoder(await response.read())

    async def objects_stream(self, sql_query, decoder=json_decoder):
        async with aiohttp.ClientSession() as session:
            async with self._make_request(sql_query + FORMAT_JSONEACHROW, session) as response:
                async for line in response.content:
                    if line:
                        yield decoder(line)

    def _make_request(self,
                      sql_query,
                      session,
                      body=None,
                      method=None):
        if not method:
            method = 'post' if body else 'get'
        func = getattr(session, method.lower())
        logger.debug(
            f"Making query to {self.base_url} with %s. timeout:{self.timeout}", self._build_params(sql_query))
        return func(
            self.scheme + '://' + self.base_url,
            timeout=self.timeout,
            params=self._build_params(sql_query),
            data=body, chunked=True)


class ClickHouse(BaseClickHouse):

    def flush_all(self):
        for k in self.buffer:
            self.flush(k)

    def flush(self, table):
        """
        Flushing buffer to DB
        """
        if table in self.buffer:
            sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
            resp_data = self.run(sql_query, self.buffer[table])
            self.buffer[table] = ''
            return resp_data

    def run(self, sql_query, data=None, decoder=bytes_decoder):
        response = self._make_request(sql_query, body=data, method='POST')
        return decoder(response.read())

    def select(self, sql_query, decoder=bytes_decoder):
        response = self._make_request(sql_query)
        return decoder(response.read())

    def objects_stream(self, sql_query, decoder=json_decoder):
        response = self._make_request(sql_query + FORMAT_JSONEACHROW)
        while True:
            line = response.readline()
            if line:
                yield decoder(line)
            else:
                break

    def _make_request(self, sql_query, body=None, method=None):
        logger.debug('Conn base url: %s', self.base_url)
        conn = http.client.HTTPConnection(self.base_url)
        if logger.level == logging.DEBUG:
            conn.set_debuglevel(logger.level)

        query_str = urllib.parse.urlencode(self._build_params(sql_query))
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
