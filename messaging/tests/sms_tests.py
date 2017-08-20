#!/usr/bin/python
import pytest

from NGS2.messaging.sms import *


@pytest.mark.parametrize('test_data, test_country, expected', [
    (
        [('A1', 1111111111), ('A2', 2222222222)],
        'US',
        [('A1', '+11111111111'), ('A2', '+12222222222')],
    ),
])
def test_format_phone_numbers(test_data, test_country, expected):
    result = format_phone_numbers(test_data, test_country)
    assert result[0][0] == expected[0][0]
    assert result[0][1] == expected[0][1]
    assert result[1][0] == expected[1][0]
    assert result[1][1] == expected[1][1]


@pytest.mark.parametrize('test_data, test_country, expected', [
    (
        [('A1', 1111111111), ('A2', 2222222222), ('A3', 333333333)],
        'US',
        [('A1', 1111111111), ('A2', 2222222222)],
    ),
])
def test_log_length_issues(test_data, test_country, expected):
    result = log_length_issues(test_data, test_country)
    assert result[0][0] == expected[0][0]
    assert result[0][1] == expected[0][1]
    assert result[1][0] == expected[1][0]
    assert result[1][1] == expected[1][1]
    assert len(result) == 2


@pytest.mark.parametrize('test, expected', [
    (
        pd.DataFrame({
            'ExternalDataReference': ['A', 'B', 'C'],
            'SMS_PHONE_CLEAN': [8888888888, 'a111111111', 9999999999],
        }),
        [('A', 8888888888), ('C', 9999999999)],
    ),
])
def test_log_numeric_issues(test, expected):
    result = log_numeric_issues(test)
    assert result[0][0] == expected[0][0]
    assert result[0][1] == expected[0][1]
    assert result[1][0] == expected[1][0]
    assert result[1][1] == expected[1][1]
    assert len(result) == 2


@pytest.mark.parametrize('test', [
    ('Some text.'),
])
def test_msg_exists_test_pass(test):
    assert msg_exists_test(test) == None


@pytest.mark.parametrize('test', [
    (None),
])
@pytest.mark.xfail(raises=AssertionError)
def test_msg_exists_test_fail(test):
    assert msg_exists_test(test)
