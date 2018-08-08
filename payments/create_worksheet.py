#!#!/usr/local/Cellar/python/3.6.5/bin
# -*- coding: utf-8 -*-
import json
import openpyxl
import numpy as np
import os
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


def enrich_data(d):
    d = create_batch_id(d, 250)
    d['currency'] = 'USD'
    d['item_id'] = ['{}_{}'.format(x, n) for x, n in
                    zip(d.batch_id, range(len(d.batch_id)))]
    d['processed_code'] = np.nan

    return d


def create_batch_id(d, n):
    full_batches = len(d) / n
    leftovers = len(d) % n
    full_ids = (
        n * [''.join(random.sample(string.ascii_letters+string.digits, 6)) for
             x in range(int(full_batches))]
    )
    full_ids.sort()
    leftover_ids = (
        leftovers *
        [''.join(random.sample(string.ascii_letters+string.digits, 6))]
    )
    d['batch_id'] = full_ids + leftover_ids

    return d


def create_payment_worksheet(file, name):
    # load blacklist
    with open('payments/blacklist.json', 'r') as f:
        blacklist = json.load(f)
    blacklist = [x.lower() for x in blacklist]

    # load data
    d = pd.read_excel(file, sheet_name=name)

    if not all([x in d.columns for x in ['first_name', 'receiver_email',
               'value']]):
        sys.exit('STOP! The input file must include `first_name`, '
                 '`receiver_email`, and `value`.')

    # remove blacklisted addresses
    d = d[d.receiver_email.apply(lambda x: False if x.lower() in
                                 blacklist else True)]

    # run data enrichment
    d = enrich_data(d)

    # return enriched data
    return d
