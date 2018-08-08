#!/usr/local/Cellar/python/3.6.5/bin
import argparse
import logging
import os
import pandas as pd
import paypalrestsdk as pp
import re
import sys

from NGS2apis.payments.create_worksheet import create_payment_worksheet


logger = logging.getLogger(__name__)
log_format = '%(asctime)s | %(name)s | %(filename)s (%(lineno)d) | %(levelname)s | %(message)s'
logging.basicConfig(filename='payments/paypal_processing.log', format=log_format,
                    level=logging.DEBUG)
logging.getLogger('paypalrestsdk').setLevel(logging.WARN)
logging.getLogger('urllib3').setLevel(logging.INFO)


def data_structure_test(d):
    # ensure all expected columns and no others are present
    EXPECTED_COLUMNS = {
        'batch_id',
        'currency',
        'first_name',
        'item_id',
        'processed_code',
        'receiver_email',
        'value'
    }
    assert all([col in d.columns for col in EXPECTED_COLUMNS]), \
    'STOP! The input worksheet is not structured as expected.'


def batch_size_test(d):
    # ensure no batch has more than 250 entries
    assert d.groupby('batch_id').apply(lambda x: len(x) <= 250).all(), \
    'STOP! Some batches are too large.'


def check_name_test(d):
    # check that names are not missing and place in proper case
    assert d.first_name.notnull().all(), \
    'STOP! Some names are missing.'
    return d.first_name.apply(lambda x: x.title())


def currency_type_test(d):
    # check currencies are valid
    d.currency = d.currency.apply(lambda x: x.upper())
    assert d.currency.all() in ['USD', 'PHP'], \
    'STOP! Not all currency choices are valid.'
    return d.currency


def email_formation_test(d):
    # check if email is well-formed
    EMAIL_CHECK = '^\w*@\w*.(com|org|edu)$'
    assert d.receiver_email.apply(lambda x: re.match(EMAIL_CHECK, x)).all(), \
    'STOP! Not all email addresses are well-formed.'


def unique_transaction_ids_test(d):
    # check all item id's are unique
    assert d.groupby('batch_id').item_id.apply(lambda x: len(x) == len(set(x))).all(), \
    'STOP! Not all transactions IDs are unique within batches.'


def values_numeric_test(d):
    # check currency values are valid
    assert d.value.apply(lambda x: isinstance(x, (int, float))).all(), \
    'STOP! Currencies not numeric.'


def data_checks(d):
    data_structure_test(d)
    batch_size_test(d)
    d.first_name = check_name_test(d)
    email_formation_test(d)
    values_numeric_test(d)
    d.currency = currency_type_test(d)
    unique_transaction_ids_test(d)


def build_payout(df, msg):
    MESSAGES = [msg.format(name) for name in df.first_name]
    return [
        {
            'recipient_type': 'EMAIL',
            'amount': {
                'value': '{0:.2f}'.format(value),
                'currency': currency,
            },
            'receiver': email,
            'note': message,
            'sender_item_id': item_id,
        } for message, value, currency, email, item_id in zip(MESSAGES,
                                                              df.value,
                                                              df.currency,
                                                              df.receiver_email,
                                                              df.item_id)
    ]


def msg_checks(subj, text):
    assert len(subj)>0, 'STOP! Message subject is empty.'
    assert len(subj)<=50, 'STOP! Message subject too long.'

    assert len(text)>0, 'STOP! Message text is empty.'
    assert len(text)<=450, 'STOP! Message text too long.'


def run(args_dict):
    # start logger
    logger.info('Starting transactions for {}.'.format(args_dict['data'][0]))

    # load payout worksheet
    d = create_payment_worksheet(args_dict['data'][0], args_dict['data'][1])

    # load message
    msg = pd.read_excel(args_dict['data'][0], sheet_name='Template')
    msg_subj = msg[msg.study==args_dict['data'][1]].subject.values[0]
    msg_text = msg[msg.study==args_dict['data'][1]].body.values[0]

    # subset to new transactions
    subd = d[d.processed_code.isnull()]
    if subd.shape[0]==0:
        logger.info('STOP! No new transactions to process. Quitting and closing log.\n')
        sys.exit()

    # run data checks
    data_checks(subd)
    msg_checks(msg_subj, msg_text)

    # group data by batches
    subd = subd.groupby('batch_id')

    # authenticate client
    pp.configure(
        {
            'mode': args_dict['environment'],
            'client_id': args_dict['auth'][0],
            'client_secret': args_dict['auth'][1],
        }
    )

    # make payouts
    for batch, details in subd:
        payout = pp.Payout(
            {
                'sender_batch_header': {
                    'sender_batch_id': batch,
                    'email_subject': msg_subj
                },
                'items': build_payout(details, msg_text)
            },
        )

        # send payouts
        if payout.create():
            logger.info('Payout for `batch_id` {} successfully processed '
                        '(processing code: {}).'
                        .format(batch, payout.batch_header.payout_batch_id)
            )
            d.loc[(d.batch_id==batch) & (d.processed_code.isnull()),
                  'processed_code'] = payout.batch_header.payout_batch_id
        else:
            logger.info(payout.error)

    # output data
    FILE = '{}.csv'.format(os.path.splitext(args_dict['data'][0])[0])
    d.to_csv(FILE, index=False)
    logger.info('Closing log for {}.\n'.format(args_dict['data'][0]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make PayPal payments.')
    parser.add_argument('-a', '--auth', required=True, nargs=2, help='API '
                        'authorization key and secret (in that order).')
    parser.add_argument('-e', '--environment', required=False, default='sandbox',
                        choices=['sandbox', 'live'], help= 'Indicates the '
                        'environment for use.')
    parser.add_argument('-d', '--data', required=True, nargs=2,
                        help='Path/file to Excel with message information and '
                        'name of study to parse.')
    args_dict = vars(parser.parse_args())

    run(args_dict)
