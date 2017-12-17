# Simple ClickHouse lib

Очень простая и библиотечка для работы с Yandex ClickHouse

	import json
	from clickhose import ClickHouse
	ch = ClickHouse('m84s1.nktch.com', db='name')


Пример чтения данных

	dt = date.today() - timedelta(days=2)
	df = dt - timedelta(days=30)

	query = '''
	SELECT phone
	FROM users
	WHERE date > '{df}' and date <= '{dt}'
	FORMAT JSONEachRow
	'''.format(df=df.strftime('%Y-%m-%d'), dt=dt.strftime('%Y-%m-%d'))

	res = ch.select(query).decode('utf-8')

Чтение стримом

	dt = date.today() - timedelta(days=2)
	df = dt - timedelta(days=30)

	query = '''
	SELECT phone
	FROM users
	WHERE date > '{df}' and date <= '{dt}'
	FORMAT JSONEachRow
	'''.format(df=df.strftime('%Y-%m-%d'), dt=dt.strftime('%Y-%m-%d'))

	res = ch.select_stream(query)

	for line in res.iter_lines():
	    if line:
	        decoded_line = line.decode('utf-8')
	        print(json.loads(decoded_line))


Запись 

	for i in range(1, 1000):
		self.storage.push('table', {'num': i})

	self.storage.flush('metrika_events')

если буфер доходит до 50к записей, происходит автоматическая отправка данных. Цифру можно поменять в конфиге




