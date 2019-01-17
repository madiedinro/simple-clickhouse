import json
import ujson
import os
import pytest
from time import sleep
from itertools import count
from simplech import TableDiscovery, ClickHouse, DeltaGenerator
from simplech.discovery import final_choose, handle_string
from simplech.simplech import http_mock_factory
import datetime


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


def test_ch_run():

    ch = ClickHouse()
    ch.conn_factory = http_mock_factory
    ch.run('SELECT')


def test_string_detection():

    assert handle_string('1.0') == float
    assert handle_string('1,0') == str
    assert handle_string('1') == int
    assert handle_string('10000') == int
    assert handle_string('0') == int
    assert handle_string('-1') == int
    assert handle_string('-10.0') == float
    assert handle_string('-1.00001') == float
    assert handle_string('-1.00001') == float
    assert handle_string('dsfsdfsd') == str
    assert handle_string('2018-12-22') == datetime.date
    assert handle_string('2018-12-22 18:33:44') == datetime.datetime
    assert handle_string('sdfsdf 2018-12-22 18:33:44') == str
    

def test_wrap_sync():
    td1 = TableDiscovery(set1, 'ga_stat')
    td1.date(
        'date').idx('ga_dimension2',
                    'date').metrics('ga_pageviews',
                                    'ga_newUsers',
                                    'ga_timeOnPage',
                                    'ga_sessions',
                                    'ga_users')
    assert td1.tc.idx == ['ga_dimension2', 'date']
    
    assert td1.tc.date_field == 'date'

    assert td1.final_cols() == {'date': datetime.date,
                                'ga_sessionCount': int,
                                'ga_channelGrouping': str,
                                'ga_dateHourMinute': int,
                                'ga_dimension2': str,
                                'ga_fullReferrer': str,
                                'ga_newUsers': int,
                                'ga_pageviews': int,
                                'ga_sessions': int,
                                'ga_timeOnPage': float,
                                'ga_users': int,
                                'profile_id': int,
                                'ga_socialNetwork': str,
                                'utm_campaign': str,
                                'utm_content': str,
                                'utm_medium': str,
                                'utm_source': str,
                                'utm_term': str}
    assert 'utm_medium' in td1.get_dimensions()
    assert 'ga_sessions' in td1.get_metrics()
    assert 'ga_stat' == td1.table
    assert 'toYYYYMM' in td1.merge_tree()
    # assert  == [20, 1]

    assert td1.get_metrics_funcs() == {
        'ga_newUsers': int,
        'ga_pageviews': int,
        'ga_sessions': int,
        'ga_timeOnPage': float,
        'ga_users': int,

    }


def test_simplech_wrapping():

    ch = ClickHouse()
    td = ch.discovery(set1, 'ga_stat')
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
    td = ch.discovery(set3, 'ga_stat').date('date').idx('date').metrics('sale')

    assert td.tc.dimensions == {'cid', 'date_time', 'id', 'date', 'uid'}
    

    td = ch.discovery(set3, 'ga_stat').date('date').idx('date')

    assert td.tc.dimensions == {'cid', 'date_time', 'id', 'date', 'uid', 'sale'}

def test_context_manager():

    ch = ClickHouse()
    td = ch.discovery(set3, 'ga_stat').date('date').idx('date').metrics('sale')

    d1 = '2019-01-10'
    d2 = '2019-01-13'

    with td.difference(d1, d2) as delta:

        assert type(delta) == DeltaGenerator
        assert delta.d1 == d1
        assert delta.d2 == d2
        assert delta.disco == td
        assert td.ch == delta.ch
        # for row in delta.run(set2):
        # print(row)
    assert td.tc.idx == ['date']
    assert 'ga_stat' == td.table


def test_final_type():

    assert final_choose(set([str, datetime.date])) == str
    assert final_choose(set([datetime.date, str])) == str
    assert final_choose(set([datetime.date, int])) == datetime.date


if __name__ == '__main__':
    test_wrap_sync()
