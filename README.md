# Simple ClickHouse lib

Very simple library for interract with ClickHouse. Internally it operate JSON documents.
For performance reasons used low-level `http.client.HTTPConnection` and `ujson` dumper/parser.



## Установка

```s
$ pip install simplech
```


## Быстрый старт

Есть синхронная версия `ClickHouse` и асинхронная `AsyncClickHouse`.


```py
from simplech import AsyncClickHouse
ch = AsyncClickHouse(host='localhost', user='default')

# Чтение 

print(await ch.select('SHOW DATABASES'))

# Запись

ch.push('my_table', {'name': 'hux', 'num': 1})

#...
#...
#...

ch.flush_all()


```	

## Параметры

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
- **loop:** [default: `None`] При необходимости указать конкретный loop

Есть переменные окружения `CH_DSN`, `CLICKHOUSE_DSN`, при наличии которых, их значение будет использовано в качестве DSN.

Приоритет DSN: 1. переменная конструктора `dsn`, 2. `CH_DSN` 3. `CLICKHOUSE_DSN`


## Асинхронная версия

### Выполнение запроса и чтение всего результата сразу

```python
>>> from simplech import AsyncClickHouse
>>> print(await ch.select('SHOW DATABASES'))

default
system
```

### Получение записей потоком

**Получить записи по отдельности, в виде `dict`**

`FORMAT` будет поставлен автоматически

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


### Выполнение SQL операций

Для управления таблицами и других операций воспользуйтесь `run`

```python
>>> await ch.run('CREATE TABLE my_table (name String, num UInt64) ENGINE=Log ')

''
```

Если все хорошо, сервер возвращает пустой `bytes`.
Можно использовать для записи данных в произвольном формате.

### Запись данных

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
>>> t = ch.select('SHOW DATABASES')
>>> print(t)

default
system
```

### Получение записей потоком

**Получить записи по отдельности, в виде `dict`**

`FORMAT` будет поставлен автоматически

```python
>>> for obj in ch.objects_stream('SELECT * from events'):
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


### Выполнение SQL операций

Для управления таблицами и других операций воспользуйтесь `run`

```python
>>> ch.run('CREATE TABLE my_table (name String, num UInt64) ENGINE=Log ')

b''
```

Если все хорошо, сервер возвращает пустой `bytes`.
Можно использовать для записи данных в произвольном формате.

### Запись данных

В simplech запись объекта производится при помощи метода `push`, но непосредственно запись
будет произведена при достижении лимита буффера, устанавливаемого параметром конструктора `buffer_limit`.

```python
>>> for i in range(1, 1500):
>>> 	ch.push('my_table', {'name': 'hux', 'num': i})
>>> ch.flush('my_table')

>>> ch.select('SELECT count() FROM my_table')

1499
```

Доступен метод `flush_all()`, он производит запись всех буфферов.

```python
>>> ch.push('my_table', {'name': 'hux', 'num': 1})
>>> ch.push('other_table', my_other_obj)
>>> ch.flush_all()
```

### Оптимизация

Операции с json в python не такие быстрые, стоит использовать альтернативную библиотеку, например

	import ujson as json

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
