#!/bin/env python3
import asyncio
from pprint import pprint

from simplech import AsyncClickHouse, ClickHouse


test_host = 'stage.rstat.org'
test_user = 'default'
test_db = 'stats'
test_debug = True


async def main():
    ch = AsyncClickHouse(
        debug=test_debug, host=test_host, user=test_user, db=test_db)

    # run
    await ch.run('CREATE TABLE IF NOT EXISTS libtest (id UInt64, date Date) ENGINE = Log')
    await ch.run("INSERT INTO libtest (id, date) VALUES (1, '2018-06-01'), (7, '2018-06-03')")
    print(await ch.select('SELECT * FROM libtest'))

    ch.push('libtest', {'id': 8, 'date': '2019-09-01'})
    ch.push('libtest', {'id': 10, 'date': '2019-09-03'})
    ch.flush('libtest')
    ch.push('libtest', {'id': 11, 'date': '2019-09-09'})
    ch.flush_all()

    await asyncio.sleep(1)
    # Objects stream
    async for obj in ch.objects_stream("SELECT * from libtest"):
        print(obj)
        print('---')

    await ch.run('DROP TABLE IF EXISTS libtest')


def sync_test():
    # run
    ch = ClickHouse(
        debug=test_debug, host=test_host, user=test_user, db=test_db)

    ch.run('CREATE TABLE IF NOT EXISTS libtest (id UInt64, date Date) ENGINE = Log')
    ch.run("INSERT INTO libtest (id, date) VALUES (1, '2018-06-01'), (7, '2018-06-03')")
    print(ch.select('SELECT * FROM libtest'))

    ch.push('libtest', {'id': 8, 'date': '2019-09-01'})
    ch.push('libtest', {'id': 10, 'date': '2019-09-03'})
    ch.flush('libtest')
    ch.push('libtest', {'id': 11, 'date': '2019-09-09'})
    ch.flush_all()

    # Objects stream
    for obj in ch.objects_stream("SELECT * from libtest"):
        print(obj)
        print('---')

    ch.run('DROP TABLE IF EXISTS libtest')


ioloop = asyncio.get_event_loop()


test_debug = False

print('silent test')
sync_test()

tasks = [
    ioloop.create_task(main()),
]
ioloop.run_until_complete(asyncio.wait(tasks))


print('louder test')

test_debug = True


sync_test()
tasks = [
    ioloop.create_task(main()),
]
ioloop.run_until_complete(asyncio.wait(tasks))

ioloop.close()



