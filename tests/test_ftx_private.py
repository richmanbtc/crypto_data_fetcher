import json
import os
import time
import ccxt
import pandas as pd
from unittest import TestCase, mock
from crypto_data_fetcher.ftx import FtxFetcher

def ftx_config():
    path = os.getenv("HOME") + '/.ftx.json'
    with open(path) as f:
        return json.load(f)

def create_ccxt_client():
    headers = {
        'FTX-SUBACCOUNT': 'bottest'
    }

    return ccxt.ftx({
        'apiKey': ftx_config()['key'],
        'secret': ftx_config()['secret'],
        'enableRateLimit': True,
        'headers': headers,
    })

# class TestFtxPrivate(TestCase):
#     def test_fetch_my_trades(self):
#         ftx = create_ccxt_client()
#         fetcher = FtxFetcher(ccxt_client=ftx)
#
#         df = fetcher.fetch_my_trades(
#             market='BTC-PERP',
#         )
#
#         print(df)
