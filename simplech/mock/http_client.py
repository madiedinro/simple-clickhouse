from urllib.parse import urlparse, parse_qsl
import io


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

    def request(self, method, url, body, *args, **kwargs):
        u = urlparse(url)
        params = dict(parse_qsl(u.query))
        q = params.get('query')
        self.last_method = method.lower()
        self.last_query = q.lower()        
        if self.last_method == 'post' and q and self.last_query.startswith('insert') and body:
            
            if isinstance(body, io.BytesIO):
                self.__class__.content += body.getvalue()
            else:
                self.__class__.content += body

        if self.last_method == 'get' and q and self.last_query.startswith('select'):
            self.buff = io.BytesIO(self.content)
            self.buff.seek(0)  

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

    async def __aenter__(self, *args, **kwargs):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def request(self, *args, **kwargs):
        return super().request(*args, **kwargs)

    async def read(self):
        return super().read()

    async def text(self):
        return super().read().decode()
