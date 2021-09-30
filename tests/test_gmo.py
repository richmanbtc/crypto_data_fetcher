import pandas as pd
import logging
import sys
from unittest import TestCase, mock
from crypto_data_fetcher.gmo import GmoFetcher
from joblib import Memory

memory_location = '/tmp/crypto_data_fetcher_test_gmo'

class TestGmo(TestCase):
    def test_fetch_ohlcv(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        stderr_handler = logging.StreamHandler(stream=sys.stderr)
        stderr_handler.setLevel(logging.DEBUG)
        logger.addHandler(stderr_handler)

        memory = Memory(memory_location, verbose=1)
        fetcher = GmoFetcher(logger=logger, memory=memory)

        df = fetcher.fetch_ohlcv(market='XLM', interval_sec=300)
        print(df)
        print(df.dtypes)

        self.assertEqual(df.index[0], pd.to_datetime('2021-08-18 07:10:00+00:00', utc=True))

    def test_fetch_trades(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        stderr_handler = logging.StreamHandler(stream=sys.stderr)
        stderr_handler.setLevel(logging.DEBUG)
        logger.addHandler(stderr_handler)

        memory = Memory(memory_location, verbose=1)
        fetcher = GmoFetcher(logger=logger, memory=memory)

        df = fetcher.fetch_trades(market='XLM')
        print(df)
        print(df.dtypes)

        self.assertEqual(df['timestamp'].iloc[0], pd.to_datetime('2021-08-18 07:10:38.186000+00:00', utc=True))
