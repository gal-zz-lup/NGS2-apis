#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import os
import pandas as pd
import time

from NGS2apis.messaging import *


def shorten_url(api, ct, link):
    # avoid rate limits
    if (ct>0) & (ct%99==0):
        time.sleep(60)

    # make api call
    return api.shorten(link)


def run(args_dict):
    # load data
    d = pd.read_csv(args_dict['data'], sep=None, engine='python')

    # authorize client
    client = Bitly(args_dict['auth'])

    # iterate over URLs and return bitlinks
    d['url'] = [shorten_url(client, i, url) for i, url in enumerate(d['link'])]

    # write data to disk
    FILENAME = os.path.splitext(args_dict['data'])
    d.to_csv('{}_bitly{}'.format(FILENAME[0], FILENAME[1]), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run URLs through Bitly for '
                                     'short URLs.')
    parser.add_argument('-a', '--auth', required=True, help='Authentication '
                        'token for Bitly; note, does not use OAuth2.')
    parser.add_argument('-d', '--data', required=True, help='Path/file for '
                        'URLs to shorten; must have a field called `link`.')
    args_dict = vars(parser.parse_args())

    run(args_dict)
