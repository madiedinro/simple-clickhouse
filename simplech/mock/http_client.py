from urllib.parse import urlparse, parse_qsl, urlencode
import io
import asyncio
import json as jsonlib



class MockStorage:

    def __init__(self):
        # print('--- creating storage ---')
        self.content = b''
        self.buff = io.BytesIO()

    def add(self, data):
        self.buff.write(data)
        self.content
        
    def get_buff(self):
        return self.buff

class MockAsyncContent:
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

    def __init__(self, mock_store=None):
        self.mock_store = mock_store
        self.last_method = None
    
    def process_select(self):
        self.mock_store.buff.seek(0)
    
    def request(self, method, url, body=None, **kwargs):
        u = urlparse(url)
        params = {**dict(parse_qsl(u.query)), **kwargs.get('params', {}) }
        
        q = params.get('query')
        self.last_method = method.lower()
        self.last_query = q.lower()

        if not q:
            print('WARNING! not query')
        
        json = kwargs.get('json')
        if json:
            body = jsonlib.dumps(json).encode()
        data = kwargs.get('data')
        if data:
            body = data
        # print(q, body)
        if self.last_method == 'post' and q and self.last_query.startswith('insert') and body:
            if isinstance(body, io.BytesIO):
                body = body.getvalue()
            self.mock_store.buff.write(body)
            print('writing', body)
        if self.last_method == 'get' and q and self.last_query.startswith('select'):
            self.process_select()
        return self

    def getresponse(self):
        return self

    def read(self):
        # if self.last_method == 'get' and self.last_query and self.last_query.startswith('select'):
        self.mock_store.buff.seek(0, 0)
        r = self.mock_store.buff.getvalue()
        print('returning', r)
        # else:
            # r = b''
        return r

    def readline(self):
        r = self.mock_store.buff.readline()
        return r

    def set_debuglevel(self, level):
        pass


class AsyncHttpClientMock(HttpClientMock):

    def process_select(self):
        super().process_select()
        self.content = MockAsyncContent(self.mock_store.buff.getvalue())

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


def create_factory(async_mode=False):

    store = MockStorage()

    def factory(*args, **kwargs):
        if async_mode == True:
            return AsyncHttpClientMock( mock_store=store)
        else:
            return HttpClientMock( mock_store=store)
    
    return factory
