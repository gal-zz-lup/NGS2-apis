#!/usr/local/python
# -*- coding: utf-8 -*-
import argparse
import string
import sys
import pandas as pd
import random


COLS = [
    'batch_id',
    'first_name',
    'receiver_email',
    'value',
    'currency',
    'item_id',
    'processed_code',
]


def enrich_data(d, args):
    if not 'batch_id' in d.columns:
        d = create_batch_id(d, args['batch'])
    if not 'value' in d.columns:
        d['value'] = args['payment']
    if not 'currency' in d.columns:
        d['currency'] = 'USD'
    if not 'item_id' in d.columns:
        d['item_id'] = ['{}_{}'.format(x, n) for x, n in
                        zip(d.batch_id, xrange(len(d.batch_id)))]
    d['processed_code'] = ''

    return d


def create_batch_id(d, n):
    full_batches = len(d) / n
    leftovers = len(d) % n
    full_ids = n * [''.join(random.sample(string.letters+string.digits, 6)) for
                    x in xrange(full_batches)]
    full_ids.sort()
    leftover_ids = leftovers * [''.join(random.sample(string.letters+string.digits, 6))]
    d['batch_id'] = full_ids + leftover_ids

    return d


def run(args_dict):
    # load data
    d = pd.read_csv(args_dict['file'], sep=None, engine='python')
    if not all([x in d.columns for x in ['first_name', 'receiver_email']]):
        sys.exit('STOP! The input file must include `first_name` and '
                 '`receiver_email`.')

    # run data enrichment
    d = enrich_data(d, args_dict)

    # write worksheet to disk
    d[COLS].to_csv(args_dict['output'], index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Take names/emails and create '
                                     'complete workbook.')
    parser.add_argument('-b', '--batch', default=250, type=int, help='The '
                        'batch size for processing in PayPal.')
    parser.add_argument('-f', '--file', required=True, help='Path/file to '
                        'process.')
    parser.add_argument('-o', '--output', required=True, help='Path/file to '
                        'write file.')
    parser.add_argument('-p', '--payment', required=False, type=float,
                        help='Payment amount (constant) to put in worksheet.')
    args_dict = vars(parser.parse_args())

    run(args_dict)
