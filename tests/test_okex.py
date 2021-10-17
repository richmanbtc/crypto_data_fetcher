import time
import ccxt
import logging
import sys
import pandas as pd
from unittest import TestCase
from crypto_data_fetcher.okex import OkexFetcher

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

market = 'BTC-USDT-SWAP'

class TestOkex(TestCase):
    def test_fetch_ohlcv_initial(self):
        okex = ccxt.okex()
        fetcher = OkexFetcher(ccxt_client=okex, logger=logger)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
        )

        self.assertEqual(df['op'].iloc[0], 7500.0)
        self.assertEqual(df['hi'].iloc[0], 7652.7)
        self.assertEqual(df['lo'].iloc[0], 7315.5)
        self.assertEqual(df['cl'].iloc[0], 7350.0)
        self.assertEqual(df['volume'].iloc[0], 311043.0)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

        # 未確定足が無いことの確認
        shift = 16 * 60 * 60 # okexの日足は16:00
        self.assertEqual(df.index.max(), pd.to_datetime(((time.time() - shift) // (24 * 60 * 60) - 1) * (24 * 60 * 60) + shift, unit='s', utc=True))

    def test_fetch_ohlcv_start_time(self):
        okex = ccxt.okex()
        fetcher = OkexFetcher(ccxt_client=okex)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
            start_time=pd.to_datetime('2021-01-01 16:00:00Z', utc=True), # okexの日足は16:00らしい
        )

        self.assertEqual(df.index[0], pd.to_datetime('2021-01-01 16:00:00Z', utc=True))

    def test_fetch_ohlcv_incremental(self):
        okex = ccxt.okex()
        fetcher = OkexFetcher(ccxt_client=okex)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
        )
        last_row = df.iloc[-1]
        df = df.iloc[:-1]

        df = fetcher.fetch_ohlcv(
            df=df,
            market=market,
            interval_sec=24 * 60 * 60,
        )
        self.assertTrue(df.iloc[-1].equals(last_row))
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_incremental_empty(self):
        okex = ccxt.okex()
        fetcher = OkexFetcher(ccxt_client=okex)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
        )
        before_count = df.shape[0]

        df = fetcher.fetch_ohlcv(
            df=df,
            market=market,
            interval_sec=24 * 60 * 60,
        )
        self.assertEqual(df.shape[0], before_count)

    def test_fetch_ohlcv_initial_minute(self):
        okex = ccxt.okex()
        fetcher = OkexFetcher(ccxt_client=okex)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=60,
            start_time=time.time() - 60 * 60
        )

        self.assertGreater(df.shape[0], 1)
        self.assertLess(df.shape[0], 61)

    def test_fetch_ohlcv_out_of_range(self):
        okex = ccxt.okex()
        fetcher = OkexFetcher(ccxt_client=okex)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
            start_time=time.time() + 60 * 60
        )

        self.assertIsNone(df)
