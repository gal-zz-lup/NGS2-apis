#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import pandas as pd
import re
import sys
import time

from twilio.rest import Client


logger = logging.getLogger(__name__)
log_format = '%(asctime)s | %(name)s | %(filename)s (%(lineno)d) | %(levelname)s | %(message)s'
logging.basicConfig(filename='messaging/twilio_processing.log', format=log_format,
                    level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.INFO)


def format_phone_numbers(data, ctry):
    if ctry == 'US':
        return [(pid, '+1{}'.format(nbr)) for pid, nbr in data]
    elif ctry == 'MA':
        return [(pid, '+212{}'.format(nbr)) for pid, nbr in data]
    elif ctry == 'PH':
        pass
    else:
        pass


def check_format_validity(data, ctry):
    if ctry == 'US':
        return log_length_issues(data, 10)
    elif ctry == 'MA':
        return log_length_issues(data, 9)
    elif ctry == 'PH':
        pass
    else:
        pass


def log_length_issues(data, digits):
    valid_nbrs = [(pid, phone) for pid, phone in data if
                  len(str(phone)) == digits]
    if len(valid_nbrs) != len(data):
        logger.info('Not all numbers valid length; {} numbers being dropped \
                    before sending.'.format(len(data) - len(valid_nbrs)))
    return valid_nbrs


def log_numeric_issues(df):
    clean_nbrs = [
        (pid, phone) for pid, phone in
        zip(df.ExternalDataReference, df.SMS_PHONE_CLEAN) if
        isinstance(phone, int)
    ]
    if len(clean_nbrs) != len(df.SMS_PHONE_CLEAN):
        logger.info('Not all numbers numeric; {} numbers being dropped before \
                    sending.'.format(len(df.SMS_PHONE_CLEAN) - len(clean_nbrs)))
    return clean_nbrs


def msg_exists_test(content):
    assert content, 'STOP! Message content is empty.'


def phone_checks(df):
    numeric_numbers = log_numeric_issues(df)
    valid_numbers = check_format_validity(numeric_numbers, args_dict['nation'])
    return valid_numbers


def run(args_dict):
    # start logger
    logger.info('Starting transactions for {}.'.format(args_dict['phones']))

    # load message content
    with open(args_dict['content'], 'r') as f:
        msg_content = f.read().strip('\n')

    # test message content
    msg_exists_test(msg_content)

    # load phone number worksheet
    d = pd.read_csv(args_dict['phones'], sep=None, engine='python')

    # test phone numbers
    valid_numbers = phone_checks(d)

    # format phone numbers
    formatted_numbers = format_phone_numbers(valid_numbers, args_dict['nation'])
    # authenticate client
    twilio = Client(args_dict['auth'][0], args_dict['auth'][1])

    # send messages
    msgs = [
        twilio.messages.create(
            to=phone,
            from_=args_dict['auth'][2],
            body=msg_content,
        ) for _, phone in formatted_numbers
    ]
    logger.info('Processed {} messages.'.format(len(msgs)))

    # sleep for one minute to queue messaging status for return
    time.sleep(60)

    # log delivery code
    delivery = pd.DataFrame(
        [(pid, msg.fetch().status, msg.sid) for pid, msg in
         zip([pid for pid, _ in formatted_numbers], msgs)],
        columns=['ExternalDataReference', 'MessageStatus', 'MessageReference'],
    )

    # merge delivery code back to input data
    d = d.merge(delivery, on='ExternalDataReference', how='left')

    # # output data
    fileparts = os.path.splitext(args_dict['phones'])
    d.to_csv('{}_delivery{}'.format(fileparts[0], fileparts[1]), index=False)
    logger.info('Closing log for {}.\n'.format(args_dict['phones']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send SMS via Twilio.')
    parser.add_argument('-a', '--auth', required=True, nargs=3, help='API '
                        'authorization SID, token, and sending phone number '
                        '(in that order).')
    parser.add_argument('-c', '--content', required=True, help='Path/file to '
                        'TXT with message content information.')
    parser.add_argument('-n', '--nation', required=True,
                        choices=['MA', 'US'], help='Country 2-letter ISO '
                        'code for recipients.')
    parser.add_argument('-p', '--phones', required=True, help='Path/file to '
                        'CSV with phone delivery information.')
    args_dict = vars(parser.parse_args())

    run(args_dict)
