import json
import ujson
import os
import pytest
from time import sleep
from itertools import count
from simplech import TableDiscovery, ClickHouse, DeltaGenerator, AsyncClickHouse
from simplech.discovery import cast_string
from simplech.mock import create_factory
from simplech.helpers import max_type
import datetime
import asyncio
from collections import Counter
from simplech.types import *


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

loop = asyncio.get_event_loop()


def test_ch_run():

    ch = ClickHouse()
    ch.conn_class = create_factory()
    ch.run('SELECT')


def test_string_detection():

    assert cast_string('1.0') == Float64
    assert cast_string('1,0') == String
    assert cast_string('1') == Int64
    assert cast_string('10000') == Int64
    assert cast_string('0') == Int64
    assert cast_string('-1') == Int64
    assert cast_string('-10.0') == Float64
    assert cast_string('-1.00001') == Float64
    assert cast_string('-1.00001') == Float64
    assert cast_string('dsfsdfsd') == String
    assert cast_string('2018-12-22') == Date
    assert cast_string('2018-12-22 18:33:44') == DateTime
    assert cast_string('sdfsdf 2018-12-22 18:33:44') == String


def test_wrap_sync():
    td1 = TableDiscovery('ga_stat', records=set1)
    td1.date(
        'date'
    ).metrics(
        'ga_pageviews',
        'ga_newUsers',
        'ga_timeOnPage',
        'ga_sessions',
        'ga_users'
    ).idx(
        'ga_dimension2',
        'date'
    )

    assert td1.tc.idx == ['ga_dimension2', 'date']

    assert td1.tc.date_field == 'date'

    assert td1.tc.columns == {'date': Date,
                              'ga_sessionCount': Int64,
                              'ga_channelGrouping': String,
                              'ga_dateHourMinute': Int64,
                              'ga_dimension2': String,
                              'ga_fullReferrer': String,
                              'ga_newUsers': Int64,
                              'ga_pageviews': Int64,
                              'ga_sessions': Int64,
                              'ga_timeOnPage': Float64,
                              'ga_users': Int64,
                              'profile_id': Int64,
                              'ga_socialNetwork': String,
                              'utm_campaign': String,
                              'utm_content': String,
                              'utm_medium': String,
                              'utm_source': String,
                              'utm_term': String}
    assert 'utm_medium' in td1.get_dimensions()
    assert 'ga_sessions' in td1.get_metrics()
    assert 'ga_stat' == td1.table
    assert 'toYYYYMM' in td1.merge_tree()
    # assert  == [20, 1]

    assert td1.tc.metrics == {
        'ga_newUsers': Int64,
        'ga_pageviews': Int64,
        'ga_sessions': Int64,
        'ga_timeOnPage': Float64,
        'ga_users': Int64,

    }


def test_simplech_wrapping():

    ch = ClickHouse()
    ch.conn_class = create_factory()
    td = ch.discover('ga_stat', set1)
    td.date(
        'date').idx('ga_dimension2',
                    'date').metrics('ga_pageviews',
                                    'ga_newUsers',
                                    'ga_timeOnPage',
                                    'ga_sessions',
                                    'ga_users')
    assert td.tc.idx == ['ga_dimension2', 'date']
    assert 'ga_stat' == td.table


def test_dimensions():

    # 'date', 'cid', 'date_time', 'id', 'sale', 'uid'
    ch = ClickHouse()
    td = ch.discover('ga_stat', set3).date('date').idx('date').metrics('sale')

    assert set(td.tc.dimensions.keys()) == {
        'cid', 'date_time', 'id', 'date', 'uid'}
    td = ch.discover('ga_stat', set3).date('date').idx('date')
    assert set(td.tc.dimensions.keys()) == {
        'cid', 'date_time', 'id', 'date', 'uid', 'sale'}


def test_td_context_manager():

    ch = ClickHouse()
    ch.conn_class = create_factory()
    td = ch.discover('ga_stat', set3).date('date').idx('date').metrics('sale')

    d1 = '2019-01-10'
    d2 = '2019-01-13'

    with td.difference(d1, d2, set3) as delta:
        assert delta.d1 == d1
        assert delta.d2 == d2
        assert delta.disco == td
        assert td.ch == delta.ch
        for row in delta:
            td.push(row)

    recs = [*ch.objects_stream('SELECT * FROM textxx')]

    assert len(recs) == 3
    assert td.tc.idx == ['date']
    assert 'ga_stat' == td.table



async def td_context_manager_async():
    ch = AsyncClickHouse()
    ch.conn_class = create_factory(async_mode=True)
    td = ch.discover('ga_stat', set3).date('date').idx('date').metrics('sale')

    d1 = '2019-01-10'
    d2 = '2019-01-13'

    async with td.difference(d1, d2, set3) as d:
        async for row in d:
            td.push(row)

    recs = []
    async for rec in ch.objects_stream('SELECT * FROM textxx'):
        recs.append(rec)

    print(recs)
    assert len(recs) == 3

def test_td_context_manager_async():
    loop.run_until_complete(td_context_manager_async())


def test_final_type():

    assert max_type(Counter(['String', 'DateTime'])) == 'String'
    assert max_type(Counter(['Date', 'String'])) == 'String'
    assert max_type(Counter(['Date', 'Int64'])) == 'Int64'


# if __name__ == '__main__':
#     test_wrap_sync()
