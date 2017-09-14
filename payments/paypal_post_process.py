#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import pandas as pd
import paypalrestsdk as pp


VARNAMES = [
    'batch_id',
    'first_name',
    'receiver_email',
    'value',
    'currency',
    'item_id',
    'processed_code',
    'payout_item_id',
    'transaction_status',
    'error',
]


logger = logging.getLogger(__name__)
log_format = '%(asctime)s | %(name)s | %(filename)s (%(lineno)d) | %(levelname)s | %(message)s'
logging.basicConfig(filename='payments/paypal_processing.log', format=log_format,
                    level=logging.DEBUG)
logging.getLogger('paypalrestsdk').setLevel(logging.WARN)
logging.getLogger('urllib3').setLevel(logging.INFO)


def construct_details(items):
    return pd.DataFrame({
        'item_id': [item['payout_item']['sender_item_id'] for item in items['items']],
        'payout_item_id': [item['payout_item_id'] for item in items['items']],
        'transaction_status': [item['transaction_status'] for item in items['items']],
        'error': [item['errors']['name'] if 'FAILED' in
                  item['transaction_status'] else '' for item in items['items']],
    })


def run(args_dict):
    # authenticate client
    pp.configure(
        {
            'mode': args_dict['environment'],
            'client_id': args_dict['auth'][0],
            'client_secret': args_dict['auth'][1],
        }
    )

    # read in payments data
    logger.info('Starting post-processing for {}.'.format(args_dict['payments']))
    d = pd.read_csv(args_dict['payments'], sep=None, engine='python')

    # gather batch ids to find transaction history
    ids = d.processed_code.unique().tolist()

    # get transactions
    post_process = [pp.Payout.find(code).to_dict() for code in ids]

    # extract transaction details
    processed_data = [construct_details(item) for item in post_process]
    processed_data = pd.concat(processed_data, axis=0)

    # merge with payment information
    d = d.merge(processed_data, on='item_id', how='left')

    # output data
    fname = os.path.splitext(args_dict['payments'])
    d[VARNAMES].to_csv('{}_final{}'.format(fname[0], fname[1]), index=False)

    logger.info(
        'Post-processing done. The file is at: {}_final{}'.format(fname[0],
                                                                  fname[1])
    )
    logger.info('Closing log for {}.\n'.format(args_dict['payments']))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Identify status of PayPal '
                                     'transactions after processing payments.')
    parser.add_argument('-a', '--auth', required=True, nargs=2, help='API '
                        'authorization key and secret (in that order).')
    parser.add_argument('-e', '--environment', required=False, default='sandbox',
                        choices=['sandbox', 'live'], help= 'Indicates the '
                        'environment for use.')
    parser.add_argument('-p', '--payments', required=True, help='Path/file to '
                        'payments that have been processed through PayPal.')
    args_dict = vars(parser.parse_args())

    run(args_dict)
