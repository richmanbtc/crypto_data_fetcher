import time
import ccxt
import pandas as pd
import logging
import sys
from unittest import TestCase, mock
from crypto_data_fetcher.gmo import GmoFetcher

class TestGmo(TestCase):
    def test_fetch_trades(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        stderr_handler = logging.StreamHandler(stream=sys.stderr)
        stderr_handler.setLevel(logging.DEBUG)
        logger.addHandler(stderr_handler)

        fetcher = GmoFetcher(logger=logger)

        df = fetcher.fetch_trades('XLM')
        print(df)
        print(df.dtypes)

        self.assertEqual(df['timestamp'].iloc[0], pd.to_datetime('2021-08-18 07:10:38.186000+00:00', utc=True))
