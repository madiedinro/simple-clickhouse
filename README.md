# Simple ClickHouse lib

Very simple ClickHouse client that simplify you interration with DBMS by using dicts as payload.
It contains two versions: synchronous for reguar usage and asynchronous for use with `asyncio`. Sync version internally uses low-level python http client. Both are use high-performance json serializer/parser `ujson`.

## Installation

Install using `pip` from pypi repository

```bash
pip install -U simplech
```

Or latest version from git 

```
pip install -U git+https://github.com/madiedinro/simple-clickhouse.git
```

## Использование

Доступно две версии: асинхронная `AsyncClickHouse` и синхронная `ClickHouse`.

- **host:** [default: `127.0.0.1`] Хост с clickhouse
- **port:** [default: `8123`]  Порт подключения
- **db:** [default: `default`]  Название базы данных
- **user:** [default: `default`]  Имя пользователя
- **password:** [default: `""`]  Пароль
- **session:** [default: `False`] Использовать сессию. Идентификатор сессии генерируется автоматически
- **session_id:** [default: `""`] Идентификатор сессии взамен автоматически сгенериованного
- **dsn:** [default: `""`] Использовать DSN для подключения (пример: `http://default@127.0.0.1:8123/stats`)
- **debug:** [default: `False`] Включение логов в режим отладки
- **buffer_limit:** [default: `1000`] Буффер записи на таблицу. При достижении будет произведена запись в БД
- **loop:** [default: `None`] При необходимости указать конкретный loop (для асинхронной версии)

Переменные окружения `CH_DSN`, `CLICKHOUSE_DSN`, при наличии которых, их значение будет использовано в качестве DSN.

Приоритет DSN: 1. аргумент конструктора `dsn`, 2. `CH_DSN` 3. `CLICKHOUSE_DSN`

## Асинхронная версия

### Выполнение запроса и чтение всего результата сразу

```python
>>> from simplech import AsyncClickHouse
>>> ch = AsyncClickHouse(host='localhost', user='default')
>>> print(await ch.select('SHOW DATABASES'))

default
system
```

### Получение записей потоком

Получить записи по отдельности, в виде `dict`.
К запросу автоматически будет добавлено `FORMAT JSONEachRow`.

```python
>>> async for obj in ch.objects_stream('SELECT * from events'):
>>>     print(obj)

{'browser_if': [0, 2],
 'browser_sr_asp': 4000,
 'browser_sr_avail_h': 740,
 'browser_sr_avail_w': 360,
 'browser_sr_oAngle': 0
 #...
}
#...
```

#### Без декодирования

```python
>>> from simplech import bytes_decoder
>>> async for obj in ch.objects_stream('SELECT * from events', decoder=none_decoder):
>>>     print(obj)

b'{"browser_if": [0, 2],"browser_sr_asp": 4000,"browser_sr_avail_h": 740,"browser_sr_avail_w": 360,"browser_sr_oAngle": 0}'
#...
```

Чтобы получить результат в виде строки воспользуйтесь `bytes_decoder`

### Выполнение SQL операций и запись данныз

Для для записи данных, управления БД и других операция (не select) слудует использовать метод `run`

```python
>>> await ch.run('CREATE TABLE my_table (name String, num UInt64) ENGINE=Log ')

''
```

Если все хорошо, сервер возвращает пустую строку `''`.

Можно использовать для записи данных в произвольном формате.

```python
>>> await ch.run('INSERT INTO my_table (name, num) VALUES("myname", 7)')

''
```

### Пакетная запись данных

В simplech запись объекта производится при помощи метода `push`, но непосредственно запись
будет произведена при достижении лимита буффера, устанавливаемого параметром конструктора `buffer_limit`.

```python
>>> for i in range(1, 1500):
>>> 	ch.push('my_table', {'name': 'hux', 'num': i})
>>> ch.flush('my_table')

>>> await ch.select('SELECT count() FROM my_table')

1499
```

Доступен метод `flush_all()`, он производит запись всех буфферов.

```python
>>> ch.push('my_table', {'name': 'hux', 'num': 1})
>>> ch.push('other_table', my_other_obj)
>>> ch.flush_all()
```


## Синхронная версия

### Выполнение запроса и чтение всего результата сразу

```python
>>> from simplech import ClickHouse
>>> ch = ClickHouse(host='localhost', user='default')
>>> print(ch.select('SHOW DATABASES'))
```

### Получение записей потоком

```python
>>> for obj in ch.objects_stream('SELECT * from events'):
>>>     print(obj)
```

### Выполнение SQL операций

```python
>>> ch.run('CREATE TABLE my_table (name String, num UInt64) ENGINE=Log ')
```
### Запись данных

```python
>>> for i in range(1, 1500):
>>> 	ch.push('my_table', {'name': 'hux', 'num': i})
>>> ch.flush('my_table')
```

или

```python
>>> ch.flush_all()
```

### License

The MIT License (MIT)

Copyright (c) 2018 Dmitry Rodin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
