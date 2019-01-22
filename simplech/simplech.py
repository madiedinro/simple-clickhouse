

from collections import defaultdict
from time import time
import logging
import os
import io
import http.client
import urllib.parse
import ujson
import asyncio
import aiohttp
from . import TableDiscovery
from . import DeltaGenerator


LOGGER_FORMAT = '%(asctime)s %(levelname)s %(message)s'
FORMAT_JSONEACHROW = ' FORMAT JSONEachRow'
FORMAT = 'FORMAT'
JSONEACHROW = 'JSONEachRow'


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)
log_formatter = logging.Formatter(LOGGER_FORMAT)
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


def format_format(val):
    return ' ' + FORMAT + ' ' + val if val else ''


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



class Buffer:
    def __init__(self, buffer_limit=5000):
        self.buffer_limit = buffer_limit
        self.buffer = io.BytesIO()
        self.counter = 0
        self.full = False

    def __len__(self):
        return self.counter

    def prepare(self):
        self.buffer.seek(0)

    def append(self, rec):
        self.buffer.write((rec + '\n').encode())
        self.counter += 1
        if self.counter >= self.buffer_limit:
            self.full = True


class WriterContext:
    def __init__(self, ch, table, dump_json=True, ensure_ascii=False, buffer_limit=5000):
        self.ch = ch
        self.ensure_ascii = ensure_ascii
        self.dump_json = dump_json
        self.buffer_limit = buffer_limit
        self.table = table
        self.set_buffer()

    def flush(self):
        self.buffer.prepare()
        self.ch._flush(self.table, self.buffer)
        self.set_buffer()

    def set_buffer(self):
        self.buffer = Buffer(buffer_limit=self.buffer_limit)

    def push(self, *docs):
        try:
            for doc in docs:
                if self.dump_json == True:
                    doc = ujson.dumps(doc, self.ensure_ascii)
                self.buffer.append(doc)
                if self.buffer.full:
                    self.flush()
        except Exception as e:
            logger.exception('exc during push')
            raise e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_value:
            self.flush()


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

        self.base_url = f"{self.host}:{self.port}"
        self._buffer = defaultdict(Buffer)
        self._buffer_limit = buffer_limit
        self.timeout = 10
        self.flush_every = 5
        self._flush_timer = None
        self.loop = loop
        self.session_id = session_id or str(time())
        self.conn_class = None

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

    def discover(self, table, records=None):
        return TableDiscovery(table=table, ch=self, records=records)

    def flush(self, table):
        pass

    def push(self, table, doc, jsonDump=True):
        """
        Add document to upload chunk
        """
        if jsonDump == True:
            try:
                doc = ujson.dumps(doc, ensure_ascii=False)
            except Exception as e:
                logger.exception('exc during push')
                raise e

        self._buffer[table].append(doc)
        if self._buffer[table].full:
            self.flush(table)

    def flush_all(self):
        for k in self._buffer:
            self.flush(k)

    @staticmethod
    def set_debug(level=logging.DEBUG):
        logger.setLevel(level)

    def close(self):
        pass

class AsyncClickHouse(BaseClickHouse):

    def _init(self):
        if not self.loop:
            self.loop = asyncio.get_event_loop()
        self._flush_timer = asyncio.ensure_future(self._timer(), loop=self.loop)
        self.conn_class = aiohttp.ClientSession

    async def _timer(self):
        while True:
            await asyncio.sleep(self.flush_every)
            self.flush_all()

    def close(self):
        if self._flush_timer:
            self._flush_timer.cancel()

    def flush(self, table):
        """
        Flush buffer of table
        """
        buff = self._buffer.get(table)
        if buff and len(buff):
            return asyncio.ensure_future(self._flush(table))

    def flush_all(self):
        tasks = []
        for k in self._buffer:
            fut = self.flush(k)
            if fut:
                tasks.append(fut)
        return asyncio.gather(*tasks)
        
    async def _flush(self, table):
        """
        Flushing buffer to DB
        """
        try:
            sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
            buff = self._buffer[table]
            if buff and len(self._buffer[table]):
                self._buffer[table] = Buffer()
                resp_data = await self.run(sql_query, data=buff.buffer)
                return resp_data
        except Exception:
            logger.exception('ch ex')

    async def run(self, sql_query, data=None, decoder=bytes_decoder):
        """
        Executes SQL code
        """
        async with self.conn_class() as session:
            async with self._make_request(sql_query, session, body=data, method='POST') as response:
                if response.status == 200:
                    result = decoder(await response.read())
                    if result != '':
                        return result
                else:
                    logger.error('wrong http code %s %s', response.status, await response.text())

    async def select(self, sql_query, decoder=bytes_decoder):
        async with self.conn_class() as session:
            async with self._make_request(sql_query, session) as response:
                if response.status == 200:
                    return decoder(await response.read())
                else:
                    logger.error('wrong http code %s %s', response.status, await response.text())

    async def objects_stream(self, sql_query, decoder=json_decoder, format=JSONEACHROW):
        async with self.conn_class() as session:
            async with self._make_request(sql_query + format_format(format), session) as response:
                if response.status == 200:
                    async for line in response.content:
                        if line:
                            yield decoder(line)
                else:
                    logger.error('wrong http code %s %s', response.status, await response.text())

    def _make_request(self,
                      sql_query,
                      session,
                      body=None,
                      method=None):
        if not method:
            method = 'post' if body else 'get'
        logger.debug(
            f"Making query to {self.base_url} with %s. timeout:{self.timeout}", self._build_params(sql_query))
        return session.request(
            method,
            url=self.scheme + '://' + self.base_url,
            timeout=self.timeout,
            params=self._build_params(sql_query),
            data=body, chunked=True)


class ClickHouse(BaseClickHouse):

    def _init(self):
        self.conn_class = http.client.HTTPSConnection if self.scheme == 'https' else http.client.HTTPConnection

    def batch(self, table):
        return WriterContext(ch=self, table=table)

    def flush(self, table):
        """
        Flushing buffer to DB
        """
        buff = self._buffer.get(table)
        if buff and len(buff):
            buff.prepare()
            result = self._flush(table, buff)
            self._buffer[table] = Buffer()
            return result

    def _flush(self, table, buff: io.BytesIO):
        sql_query = f'INSERT INTO {table} FORMAT JSONEachRow'
        result = self._make_request(sql_query, body=buff.buffer, method='POST')
        if result.code != 200:
            return result
        if result != '':
            return result

    def run(self, sql_query, data=None, decoder=bytes_decoder):
        if data:
            data = data.encode('utf-8')
        response = self._make_request(sql_query, body=data, method='POST')
        if response.code != 200:
            return response
        result = decoder(response.read())
        if result != '':
            return result

    def select(self, sql_query, decoder=bytes_decoder):
        response = self._make_request(sql_query)
        return decoder(response.read())

    def objects_stream(self, sql_query, decoder=json_decoder, format=JSONEACHROW):
        response = self._make_request(sql_query + format_format(format))
        while True:
            line = response.readline()
            if line:
                yield decoder(line)
            else:
                break

    def _make_request(self, sql_query, body=None, method=None):
        logger.debug('Conn base url: %s', self.base_url)
        conn = self.conn_class(self.base_url)

        if logger.level == logging.DEBUG:
            conn.set_debuglevel(logger.level)

        query_str = urllib.parse.urlencode(
            self._build_params(sql_query), encoding='utf-8')
        logger.debug('Query string: %s', query_str)

        if not method:
            method = 'POST' if body else 'GET'

        # print(body.getvalue())

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
