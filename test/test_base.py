import json
import ujson
import os
import pytest
from time import sleep
from itertools import count
from simplech import TableDiscovery, ClickHouse, DeltaGenerator, AsyncClickHouse
from simplech.mock import HttpClientMock, AsyncHttpClientMock, create_factory
import datetime
import asyncio

set1 = [
    {'date': '2018-12-31', 'ga_channelGrouping': 'Organic Search', 'ga_dateHourMinute': '201812311517', 'ga_dimension2': '128983921.1546258642', 'ga_fullReferrer': 'google', 'ga_newUsers': '1', 'ga_pageviews': '1', 'ga_sessionCount': '1',
     'ga_sessions': '1', 'ga_socialNetwork': '', 'ga_timeOnPage': '0.0', 'ga_users': '1', 'profile_id': '175660279', 'utm_campaign': '', 'utm_content': '', 'utm_medium': 'organic', 'utm_source': 'google', 'utm_term': '(not provided)'},
    {'date': '2018-12-31',  'ga_channelGrouping': 'Direct',  'ga_dateHourMinute': '201812311201',  'ga_dimension2': '1607744401.1543065937',  'ga_fullReferrer':
     '(direct)',  'ga_newUsers': '0',  'ga_pageviews': '1',  'ga_sessionCount': '2',  'ga_sessions': '1',  'ga_socialNetwork': '',  'ga_timeOnPage': '31.0',  'ga_users': '1',  'profile_id': '175660279',  'utm_campaign': '',  'utm_content': '',  'utm_medium': '(none)',  'utm_source': '(direct)',  'utm_term': ''},
    {'date': '2018-12-31',  'ga_channelGrouping': 'Direct',  'ga_dateHourMinute': '201812311202',  'ga_dimension2': '1607744401.1543065937',  'ga_fullReferrer':
     '(direct)',  'ga_newUsers': '0',  'ga_pageviews': '1',  'ga_sessionCount': '2',  'ga_sessions': '0',  'ga_socialNetwork': '',  'ga_timeOnPage': '0.0',  'ga_users': '1',  'profile_id': '175660279',  'utm_campaign': '',  'utm_content': '',  'utm_medium': '(none)',  'utm_source': '(direct)',  'utm_term': ''},
    {'date': '2018-12-31', 'ga_channelGrouping': 'Referral', 'ga_dateHourMinute': '201812310006', 'ga_dimension2': '657379397.1546062444', 'ga_fullReferrer': 'rock.st/ru/', 'ga_newUsers': '0', 'ga_pageviews': '1', 'ga_sessionCount': '2',
     'ga_sessions': '1', 'ga_socialNetwork': '', 'ga_timeOnPage': '0.0', 'ga_users': '1', 'profile_id': '175660279', 'utm_campaign': '', 'utm_content': '', 'utm_medium': 'referral', 'utm_source': 'rock.st', 'utm_term': ''}
]
set2 = [
    {'utm_source': 'vk', 'utm_campaign': 'dr4', 'utm_content': 'kiborg-vid', 'utm_term': 'retarg-mob', 'spent': '2.40',
     'impressions': 13, 'reach': 12, 'date': '2019-01-12', 'ad_id': '48602127', 'campaign_id': 1010819423, 'account_id': 1603421955},
    {'utm_source': 'vk', 'utm_campaign': 'dr4', 'utm_content': 'kiborg-vid', 'utm_term': 'retarg-mob', 'impressions': 12,
     'clicks': 1, 'reach': 11, 'date': '2019-01-13', 'ad_id': '48602127', 'campaign_id': 1010819423, 'account_id': 1603421955},
    {'utm_source': 'vk', 'utm_campaign': 'dr4', 'utm_content': 'kiborg-vid', 'utm_term': 'retarg-mob', 'impressions': 7,
     'reach': 6, 'date': '2019-01-14', 'ad_id': '48602127', 'campaign_id': 1010819423, 'account_id': 1603421955},
]

set3 = [
    {'cid': '69296758.1544679970', 'date': '2019-01-09', 'date_time': '2019-01-10 08:00:22',
        'id': '1795469', 'sale': '10000', 'uid': '6450101900745375744'},
    {'cid': '69296758.1544679972', 'date': '2019-01-10', 'date_time': '2019-01-10 08:00:43',
        'id': '1795469', 'sale': '70000', 'uid': '6450101900745375745'},
    {'cid': '69296758.1544679973', 'date': '2019-01-11', 'date_time': '2019-01-10 08:00:43',
        'id': '1795469', 'sale': '4000', 'uid': '6450101900745375746'},
]




def test_ch_delta_iter():

    ch = ClickHouse()
    ch.conn_class = create_factory()

    ch.run('CREATE TABLE IF NOT EXISTS test1 (name String) ENGINE = Log()')
    upd = [{'name': 'lalala', 'value': 1}, {'name': 'bababa', 'value': 2}, {'name': 'nanana', 'value': 3}]

    td = ch.discover('test1', upd).metrics('value')

    d1 = '2019-01-10'
    d2 = '2019-01-13'

    new_recs = []
    with td.difference(d1, d2, upd) as delta:
        for row in delta:
            new_recs.append(row)

    for r in upd:
        assert r in new_recs

    ch.close()



def test_ch_push():

    ch = ClickHouse()
    ch.conn_class = create_factory()

    ch.run('CREATE TABLE IF NOT EXISTS test1 (name String) ENGINE = Log()')
    ch.push('textxx', {'name': 'lalala'})
    ch.flush_all()
    recs = [*ch.objects_stream('SELECT * FROM textxx')]
    assert len(recs) == 1
    ch.push('textxx', {'name': 'nananan'})
    ch.flush('textxx')
    recs = [*ch.objects_stream('SELECT * FROM textxx')]
    assert len(recs) == 2



async def async_ch_differ():

    ch = AsyncClickHouse()
    ch.conn_class = create_factory(async_mode=True)

    await ch.run('CREATE TABLE IF NOT EXISTS test1 (name String) ENGINE = Log()')
    upd = [{'name': 'lalala', 'value': 1}, {'name': 'bababa', 'value': 2}, {'name': 'nanana', 'value': 3}]

    td = ch.discover('test1', upd).metrics('value')

    d1 = '2019-01-10'
    d2 = '2019-01-13'

    new_recs = []
    async with td.difference(d1, d2, upd) as delta:
        async for row in delta:
            td.push(row)
            new_recs.append(row)

    for r in upd:
        assert r in new_recs

    ch.close()



def test_async_ch_differ():
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_ch_differ())


async def async_ch_push():

    ch = AsyncClickHouse()
    ch.conn_class = create_factory(async_mode=True)

    await ch.run('CREATE TABLE IF NOT EXISTS test1 (name String) ENGINE = Log()')
    ch.push('textxx', {'name': 'lalala'})
    await ch.flush_all()
    recs = []
    async for rec in ch.objects_stream('SELECT * FROM textxx'):
        print(type(rec))
        recs.append(rec)
    assert len(recs) > 0
    ch.push('textxx', {'name': 'nananan'})
    await ch.flush('textxx')
    recs = []
    
    async for rec in ch.objects_stream('SELECT * FROM textxx'):
        print(type(rec))
        recs.append(rec)
    assert len(recs) > 0

    ch.close()


def test_async_ch_push():
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_ch_push())



def test_ch_context_manager():

    ch = ClickHouse()
    ch.conn_class = create_factory()

    ch.run('CREATE TABLE IF NOT EXISTS test1 (name String) ENGINE = Log()')
    with ch.table('test1') as b:
        b.push({'name': 'lalala'})

