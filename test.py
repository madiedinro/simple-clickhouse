#!/bin/env python3
import asyncio
from pprint import pprint

from simplech import AsyncClickHouse
ch = AsyncClickHouse(debug=True, host='analytics.virginsclub.ru', user='default', db='alco')

async def main():
      
    t = await ch.select('SHOW DATABASES')
    print(t)

    async for obj in ch.objects_stream("SELECT * from events_v2_test LIMIT 4"):
        pprint(obj)


ioloop = asyncio.get_event_loop()
tasks = [
    ioloop.create_task(main()),
]
ioloop.run_until_complete(asyncio.wait(tasks))
ioloop.close()