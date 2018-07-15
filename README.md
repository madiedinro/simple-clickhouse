# Simple ClickHouse lib

Very simple library for interract with ClickHouse. Internally it operate JSON documents.
For performance reasons used low-level `http.client.HTTPConnection` and `ujson` dumper/parser.

## Использование

	# установка
	pip install "git+https://github.com/madiedinro/simple-clickhouse"


	# быстрее обычного json
	import ujson as json

	from simplech import ClickHouse
	ch = ClickHouse(host='host', port=9090, user='default', password="default",  db='default')


### Пример чтения данных

	dt = date.today() - timedelta(days=2)
	df = dt - timedelta(days=30)

	query = '''
	SELECT phone
	FROM users
	WHERE date > '{df}' and date <= '{dt}'
	FORMAT JSONEachRow
	'''.format(df=df.strftime('%Y-%m-%d'), dt=dt.strftime('%Y-%m-%d'))

	res = ch.select(query).decode('utf-8')

### Построчное чтение данных

	dt = date.today() - timedelta(days=2)
	df = dt - timedelta(days=30)

	query = '''
	SELECT phone
	FROM users
	WHERE date > '{df}' and date <= '{dt}'
	FORMAT JSONEachRow
	'''.format(df=df.strftime('%Y-%m-%d'), dt=dt.strftime('%Y-%m-%d'))

	for line in ch.select_stream(query).iter_lines():
		decoded_line = line.decode('utf-8')
		print(json.loads(decoded_line))

### Запись 

	for i in range(1, 1000):
		self.storage.push('table', {'num': i})

	self.storage.flush('metrika_events')

если буфер доходит до 1к записей, происходит автоматическая отправка данных. Цифру можно поменять в конфиге




