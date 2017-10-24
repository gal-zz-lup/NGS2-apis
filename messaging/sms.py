#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import json
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


def check_format_validity(data, ctry, link):
    if ctry == 'US':
        return log_length_issues(data, 10, link)
    elif ctry == 'MA':
        return log_length_issues(data, 9, link)
    elif ctry == 'PH':
        return log_length_issues(data, 10, link)
    else:
        pass


def format_phone_numbers(data, ctry, link):
    if link:
        if ctry == 'US':
            return [(pid, '+1{}'.format(nbr), url) for pid, nbr, url in data]
        elif ctry == 'MA':
            return [(pid, '+212{}'.format(nbr), url) for pid, nbr, url in data]
        elif ctry == 'PH':
            return [(pid, '+63{}'.format(nbr), url) for pid, nbr, url in data]
        else:
            sys.exit('STOP! Invalid country. Only US, MA, and PH are valid.')
    else:
        if ctry == 'US':
            return [(pid, '+1{}'.format(nbr)) for pid, nbr in data]
        elif ctry == 'MA':
            return [(pid, '+212{}'.format(nbr)) for pid, nbr in data]
        elif ctry == 'PH':
            return [(pid, '+63{}'.format(nbr)) for pid, nbr in data]
        else:
            sys.exit('STOP! Invalid country. Only US, MA, and PH are valid.')


def log_length_issues(data, digits, link):
    if link:
        valid_nbrs = [(pid, phone, url) for pid, phone, url in data if
                      len(str(phone)) == digits]
    else:
        valid_nbrs = [(pid, phone) for pid, phone in data if
                      len(str(phone)) == digits]
    if len(valid_nbrs) != len(data):
        logger.info('Not all numbers valid length; {} numbers being dropped \
                    before sending.'.format(len(data) - len(valid_nbrs)))
    return valid_nbrs


def log_numeric_issues(df, link):
    if link:
        clean_nbrs = [
            (pid, phone, url) for pid, phone, url in
            zip(df.ExternalDataReference, df.SMS_PHONE_CLEAN, df.url) if
            isinstance(phone, int)
        ]
    else:
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


def phone_checks(df, nation, link):
    numeric_numbers = log_numeric_issues(df, link)
    valid_numbers = check_format_validity(numeric_numbers, nation, link)
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
    valid_numbers = phone_checks(d, args_dict['nation'], args_dict['url_link'])

    # format phone numbers
    formatted_numbers = format_phone_numbers(valid_numbers, args_dict['nation'],
                                             args_dict['url_link'])

    # authenticate client
    twilio = Client(args_dict['auth'][0], args_dict['auth'][1])

    # check for bad numbers
    if args_dict['error_check']:
        # gather previous error numbers
        total_errors = [(x.to, x.error_code) for x in twilio.messages.list() if
                        x.error_code]
        prev_badnums = list(set([x[0] for x in total_errors if x[1]!=30001]))

        # open up running tally of bad numbers
        with open('messaging/bad_numbers.json', 'r') as f:
            badnums = json.load(f)

        # update tally
        badnums += [x for x in prev_badnums if x not in badnums]

        # cross-check (and remove) current numbers
        formatted_numbers = [x for x in formatted_numbers if not x[1] in badnums]

        # write out bad numbers for future use
        with open('messaging/bad_numbers.json', 'w') as f:
            json.dump(badnums, f)

    # chunk large sends into batches
    msgs = []
    if len(formatted_numbers) > 75:
        length = len(formatted_numbers)
        beg = [x*75 for x in xrange(length/75 + 1)]
        end = [x+75 for x in beg]
        end[-1] = end[-2] + length%75
        for b, e in zip(beg, end):
            if args_dict['url_link']:
                for _, phone, url in formatted_numbers[b:e]:
                    try:
                        msgs.append(twilio.messages.create(
                            to=phone,
                            from_=args_dict['auth'][2],
                            body='{} {}'.format(msg_content, url),
                        ))
                    except:
                        pass
            else:
                for _, phone in formatted_numbers[b:e]:
                    try:
                        msgs.append(twilio.messages.create(
                            to=phone,
                            from_=args_dict['auth'][2],
                            body=msg_content,
                        ))
                    except:
                        pass
            logger.info('Processed {} messages.'.format(len(msgs)))
            time.sleep(10)
    else:
        if args_dict['url_link']:
            for _, phone, url in formatted_numbers:
                try:
                    msgs.append(twilio.messages.create(
                        to=phone,
                        from_=args_dict['auth'][2],
                        body='{} {}'.format(msg_content, url),
                    ))
                except:
                    pass
        else:
            for _, phone in formatted_numbers:
                try:
                    msgs.append(twilio.messages.create(
                        to=phone,
                        from_=args_dict['auth'][2],
                        body=msg_content,
                    ))
                except:
                    pass
        logger.info('Processed {} messages.'.format(len(msgs)))

    # sleep for one minute to queue messaging status for return
    time.sleep(45)

    # log delivery code
    if args_dict['url_link']:
        delivery = pd.DataFrame(
            [(pid, msg.fetch().status, msg.sid) for pid, msg in
             zip([pid for pid, _, __ in formatted_numbers], msgs)],
            columns=['ExternalDataReference', 'MessageStatus', 'MessageReference'],
        )
    else:
        delivery = pd.DataFrame(
            [(pid, msg.fetch().status, msg.sid) for pid, msg in
             zip([pid for pid, _ in formatted_numbers], msgs)],
            columns=['ExternalDataReference', 'MessageStatus', 'MessageReference'],
        )

    # merge delivery code back to input data
    d = d.merge(delivery, on='ExternalDataReference', how='left')

    # output data
    fileparts = os.path.splitext(args_dict['phones'])
    d.to_csv('{}_delivery{}'.format(fileparts[0], fileparts[1]), index=False)
    logger.info('Closing log for {}.\n'.format(args_dict['phones']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send SMS via Twilio.')
    parser.add_argument('-a', '--auth', required=True, nargs=3, help='API '
                        'authorization SID, token, and sending phone number '
                        '(in that order).')
    parser.add_argument('-c', '--content', required=True, help='Path/file to '
                        'txt with message content information.')
    parser.add_argument('-e', '--error_check', action='store_true',
                        help='Indicate if phone numbers should be checked '
                        'against a known bad/stop list.')
    parser.add_argument('-l', '--url_link', action='store_true',
                        help='Indicates if a URL should be sent with the SMS.')
    parser.add_argument('-n', '--nation', required=True,
                        choices=['MA', 'US', 'PH'], help='Country 2-letter ISO '
                        'code for recipients.')
    parser.add_argument('-p', '--phones', required=True, help='Path/file to '
                        'csv with phone delivery information.')
    args_dict = vars(parser.parse_args())

    run(args_dict)
