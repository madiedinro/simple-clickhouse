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

## Connection params

Comes with async version `AsyncClickHouse` and sync `ClickHouse`.

При использовании в Rockstat, параметры указывать не требуется. Они подставляются автоматически из переменных окружения.

```python
from simplech import AsyncClickHouse
ch = AsyncClickHouse()
```

- **host:** [default: `127.0.0.1`] Хост с clickhouse
- **port:** [default: `8123`]  Порт подключения
- **db:** [default: `default`]  Название базы данных
- **scheme:** [default: `http`]  Протокол http/https
- **user:** [default: `default`]  Имя пользователя
- **password:** [default: `""`]  Пароль
- **session:** [default: `False`] Использовать сессию. Идентификатор сессии генерируется автоматически
- **session_id:** [default: `""`] Идентификатор сессии взамен автоматически сгенериованного
- **dsn:** [default: `""`] Использовать DSN для подключения (пример: `http://default@127.0.0.1:8123/stats`)
- **debug:** [default: `False`] Включение логов в режим отладки
- **flush_every:** [default: `5`] Every X seconds data will be flushed to db
- **buffer_limit:** [default: `1000`] Буффер записи на таблицу. При достижении будет произведена запись в БД
- **loop:** [default: `None`] При необходимости указать конкретный loop (для асинхронной версии)
- **timeout:** [default: `10`] Время ожидания запроса в секундах

Переменные окружения `CH_DSN`, `CLICKHOUSE_DSN`, при наличии которых, их значение будет использовано в качестве DSN.

Приоритет DSN: 1. аргумент конструктора `dsn`, 2. `CH_DSN` 3. `CLICKHOUSE_DSN`

## Async version

### Selecting without decoding

```python
from simplech import AsyncClickHouse

ch = AsyncClickHouse(host='localhost', user='default')

print(await ch.select('SHOW DATABASES'))

[Out]:  default
        system
```

### Selecting as dict's steam

Получить записи по отдельности, в виде `dict`.
К запросу автоматически будет добавлено `FORMAT JSONEachRow`.

```python
async for obj in ch.objects_stream('SELECT * FROM events'):
    print(obj)

[Out]:  {
            'browser_if': [0, 2],
            'browser_sr_asp': 4000,
            'browser_sr_avail_h': 740,
            'browser_sr_avail_w': 360,
            'browser_sr_oAngle': 0
            ...
        }
        ...
```

#### Disabling decoding for streaming data

```python
from simplech import bytes_decoder

async for obj in ch.objects_stream('SELECT * from events', decoder=none_decoder):
     print(obj)

[Out]: b'{"browser_if": [0, 2],"browser_sr_asp": 4000,"browser_sr_avail_h": 740,"browser_sr_avail_w": 360,"browser_sr_oAngle": 0}'
#...
```

Чтобы получить результат в виде строки воспользуйтесь `bytes_decoder`

### Executing sql statements

Для для записи данных, управления БД и других операция (не select) слудует использовать метод `run`

```python
await ch.run('CREATE TABLE my_table (name String, num UInt64) ENGINE=Log ')
```

Можно использовать для "ручной" записи данных

```python
>>> await ch.run('INSERT INTO my_table (name, num) VALUES("myname", 7)')
```

### Microbatch writing using context manager

В simplech запись объекта производится при помощи метода `push`, но непосредственно запись
будет произведена при достижении лимита буффера, устанавливаемого параметром конструктора `buffer_limit`.

**new**

```python
with ch.table('tablename') as w:
    for rec in recs:
        w.push(rec)
```

On exit context all data will be flushed.

Old manual conrolled mechanic.

```python
for i in range(1, 1500):
    ch.push('my_table', {'name': 'hux', 'num': i})
ch.flush('my_table')

await ch.select('SELECT count() FROM my_table')

[Out]: 1499
```

Доступен метод `flush_all()`, он производит запись всех буфферов.

```python
ch.push('my_table', {'name': 'hux', 'num': 1})
ch.push('other_table', my_other_obj)
# or
ch.flush_all()
```

## Some Simpe Magick

### Schema detection

To create instance of TableDiscovery call

```
ch.discover(table, records=None, columns=None)
``` 

- records is a list with records
- columnts is a dict where key is table columnt name / field name; value is the field data type.

One of records or columns should be filled.

#### ch.discover('table_name', records=[...]) 

-> TableDiscovery instanse



#### ch.discover('table', columns={...})

```python
td_deals = ch.discover('deals', columns={
    'id': 'Int64', 
    'uid': 'Int64', 
    'cid': 'String', 
    'sale': 'Int64', 
    'date': 'Date', 
    'date_time': 'DateTime', 
    'account_id': 'Int64'
})
```

Detect using present data

```python
ch = ClickHouse()
td = ch.discovery(deals, 'deals')
td.date('date').idx('account_id', 'date').metrics('sale')

```

#### TableDiscovery.merge_tree()

```
ch.merge_tree()
```

result 

```
CREATE TABLE IF NOT EXISTS `deals` (
  `id`  UInt64,
  `uid`  UInt64,
  `cid`  String,
  `sale`  UInt64,
  `date`  Date,
  `date_time`  DateTime,
  `account_id`  UInt64
) ENGINE MergeTree() PARTITION BY toYYYYMM(`date`) ORDER BY (`account_id`, `date`) SETTINGS index_granularity=8192
```


#### Code generationm

Next times after use table auto discovery you shoud use fixed layout. To to this easy try `TableDiscovery.pycode()`

```python
code = td.pycode()
print(code)
```

will be returned

```
td = ch.discover('deals', columns={
    'id': 'Int64', 
    'uid': 'Int64', 
    'cid': 'String', 
    'sale': 'Int64', 
    'date': 'Date', 
    'date_time': 'DateTime', 
    'account_id': 'Int64'
})\
.metrics('sale')\
.dimensions('date_time', 'account_id', 'cid', 'uid', 'id', 'date')\
.date('date')\
.idx('account_id', 'date')
```

#### Correct detected / implicit set data-types

`TableDiscovery.int(*args)` set columnts to int

returns self

**Set date columns**

`TableDiscovery.date(*args)`

Set date column

returns self

**Set str columns**

`TableDiscovery.str(*args)`

Set strinmg column

returns self

#### Columns configuration

**Set primary key columns**

.idx(*args)

returns self

**Set metrics**

.metrics(*args)

returns self

other marked as dimensions

**Set dimensions**

.dimensions(*args)

other marked as metrics

#### Print table create statement / execute query

td.merge_tree(Execute=True|False)

#### Chaining

td.date('date').metrics('sale').idx('account_id', 'date')

#### Discovery TODO

- [ ] Support all ClickHouse types, especially Arrays
- [ ] Discovery by DB Table structure


```python
with td.table('tablename') as w:
    for rec in recs:
        w.push(rec)
```


### Difference handling. Be careful currently it Proof of concept

#### Sync version


```python

ch = ClickHouse()

upd = [{'name': 'lalala', 'value': 1}, {'name': 'bababa', 'value': 2}, {'name': 'nanana', 'value': 3}]
td = ch.discover('test1', upd).metrics('value')

d1 = '2019-01-10'
d2 = '2019-01-13'

new_recs = []
with td.difference(d1, d2, upd) as d:
    for row in d:
        td.push(row)
        print(row)
```

All records will be flushed to DB on context exit

#### Async version

```python

ch = AsyncClickHouse()

# new data
upd = [{'name': 'lalala', 'value': 1}, {'name': 'bababa', 'value': 2}, {'name': 'nanana', 'value': 3}]
td = ch.discover('test1', upd).metrics('value')

d1 = '2019-01-10'
d2 = '2019-01-13'

async with td.difference(d1, d2, upd) as d:
    async for row in d:
        td.push(row)

# Graceful unload
ch.close()
```

#### Difference TODO

- [ ] Focus on CollapsingMergeTree



## Синхронная версия

### Выполнение запроса и чтение всего результата сразу

```python
from simplech import ClickHouse
ch = ClickHouse(host='localhost', user='default')
print(ch.select('SHOW DATABASES'))
```

### Получение записей потоком

```python
for obj in ch.objects_stream('SELECT * from events'):
    print(obj)
```

### Выполнение SQL операций

```python
ch.run('CREATE TABLE my_table (name String, num UInt64) ENGINE=Log ')
```
### Запись данных

```python
for i in range(1, 1500):
	ch.push('my_table', {'name': 'hux', 'num': i})
ch.flush('my_table')
```

или

```python
>>> ch.flush_all()
```

better approach


```python

my_data = [
    {'name': 'lalala', 'value': 1}, 
    {'name': 'bababa', 'value': 2}, 
    {'name': 'nanana'}
]

with ch.table('mytbl') as c:
    for rec in my_data:
        c.push(record)
```

all data will be flushed on exit context

## License

The MIT License (MIT)

Copyright (c) 2018-2019 Dmitry Rodin

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
