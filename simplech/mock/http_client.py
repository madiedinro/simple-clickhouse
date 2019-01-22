from urllib.parse import urlparse, parse_qsl, urlencode
import io
import asyncio
import json as jsonlib


class AsyncContent:
    def __init__(self, content):
        self.content = content
        self.buff = io.BytesIO(content)
        
    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.05)
        line = self.buff.readline(999)
        if not line:
            raise StopAsyncIteration
        return line

class HttpClientMock:
    status = 200
    code = 200
    length = 0
    buff = io.BytesIO()
    content = b''

    def __init__(self, url):
        self.url = url
        self.items = []
        self.last_method = None

    def process_select(self):
        self.buff = io.BytesIO(self.__class__.content)
        self.buff.seek(0)

    def request(self, method, url, body=None, **kwargs):
        u = urlparse(url)
        params = {**dict(parse_qsl(u.query)), **kwargs.get('params', {}) }
        print(u)
        q = params.get('query')
        self.last_method = method.lower()
        self.last_query = q.lower()
        
        json = kwargs.get('json')
        if json:
            body = jsonlib.dumps(json).encode()
        data = kwargs.get('data')
        if data:
            body = data
        if self.last_method == 'post' and q and self.last_query.startswith('insert') and body:
            if isinstance(body, io.BytesIO):
                self.__class__.content += body.getvalue()
            else:
                self.__class__.content += body

        if self.last_method == 'get' and q and self.last_query.startswith('select'):
            self.process_select()
        return self

    def getresponse(self):
        return self

    def read(self):
        if self.last_method == 'get' and self.last_query and self.last_query.startswith('select'):
            r = self.content
        else:
            r = b''
        print(r)
        return r

    def readline(self):
        print(self.content)
        r = self.buff.readline()
        print(r)
        return r

    def set_debuglevel(self, level):
        pass


class AsyncHttpClientMock(HttpClientMock):
    content = b''
    def __init__(self):
        pass

    def process_select(self):
        self.content = AsyncContent(self.__class__.content)


    def _make_request(self, *args, **kwargs):
        return self

    async def __aenter__(self, *args, **kwargs):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0.05)

    def request(self, method, url, *args, **kwargs):
        return super().request(method, url, *args, **kwargs)

    async def read(self):
        return super().read()

    async def text(self):
        return super().read().decode()
