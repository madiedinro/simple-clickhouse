<h1 id="simplech">simplech</h1>


<h1 id="simplech.simplech">simplech.simplech</h1>


This module implements the ...

<h2 id="simplech.simplech.BaseClickHouse">BaseClickHouse</h2>

```python
BaseClickHouse(self, host=None, port=None, db=None, user=None, password=None, session=False, session_id=None, dsn=None, debug=False, loop=None, buffer_size=1000)
```

This module implements the ...

__Arguments__


- __host__: (str, None) Хост с clickhouse. Default to `127.0.0.1`.
- __port__: (int, None) Порт подключения. Default to `8123`.
- __db__: (str, None) Название базы данных. Default to `default`.
- __user__: (str, None) Имя пользователя. Default to `default`.
- __password__: (str, None) Пароль. Default to `""`.
- __session__: (bool) Использовать сессию. Идентификатор сессии генерируется автоматически. Default to `False`.
- __session_id__: (str, None) Идентификатор сессии взамен автоматически сгенериованного. Default to `None`.
- __dsn__: (str, None) Использовать для подключения DSN, например: `http://default@127.0.0.1:8123/stats`. Default to `None`.
    При наличии переменной окружеения `CH_DSN` или `CLICKHOUSE_DSN` будет использовано ее значение.
- __debug__: (bool) Переключение логов в режим отладки. Default to `False`.
- __buffer_limit__: (int) Буффер записи на таблицу. При достижении будет произведена запись в БД. Default to `1000`.
- __loop__: (EventLoop, None) При необходимости указать конкретный loop (для асинхронной версии). Default to `None`.



<h3 id="simplech.simplech.BaseClickHouse.push">push</h3>

```python
BaseClickHouse.push(self, table, doc, jsonDump=True)
```

Add document to upload chunk

<h2 id="simplech.simplech.AsyncClickHouse">AsyncClickHouse</h2>

```python
AsyncClickHouse(self, host=None, port=None, db=None, user=None, password=None, session=False, session_id=None, dsn=None, debug=False, loop=None, buffer_size=1000)
```

<h3 id="simplech.simplech.AsyncClickHouse.flush">flush</h3>

```python
AsyncClickHouse.flush(self, table)
```

Flush buffer of table

<h3 id="simplech.simplech.AsyncClickHouse.run">run</h3>

```python
AsyncClickHouse.run(self, sql_query, data=None, decoder=<function bytes_decoder at 0x1094946a8>)
```

Executes SQL code

<h2 id="simplech.simplech.ClickHouse">ClickHouse</h2>

```python
ClickHouse(self, host=None, port=None, db=None, user=None, password=None, session=False, session_id=None, dsn=None, debug=False, loop=None, buffer_size=1000)
```

<h3 id="simplech.simplech.ClickHouse.flush">flush</h3>

```python
ClickHouse.flush(self, table)
```

Flushing buffer to DB

