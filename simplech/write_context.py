import io
import ujson
from .log import logger 

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
        buff = self.buffer
        self.set_buffer()
        return self.ch._flush(self.table, buff)

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

