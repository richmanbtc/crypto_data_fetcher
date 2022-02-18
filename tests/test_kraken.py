import time
import ccxt
import pandas as pd
from unittest import TestCase
from crypto_data_fetcher.kraken import KrakenFetcher

market = 'XXBTZUSD'

class TestKraken(TestCase):
    def test_fetch_ohlcv_initial(self):
        kraken = ccxt.kraken()
        fetcher = KrakenFetcher(ccxt_client=kraken)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
        )

        self.assertEqual(df['op'].iloc[0], 8526.10000)
        self.assertEqual(df['hi'].iloc[0], 8750.00000)
        self.assertEqual(df['lo'].iloc[0], 8405.00000)
        self.assertEqual(df['cl'].iloc[0], 8530.00000)
        self.assertEqual(df['volume'].iloc[0], 3134.04788956)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

        # 未確定足が無いことの確認
        self.assertEqual(df.index.max(), pd.to_datetime((time.time() // (24 * 60 * 60) - 1) * (24 * 60 * 60), unit='s', utc=True))

    def test_fetch_ohlcv_start_time(self):
        kraken = ccxt.kraken()
        fetcher = KrakenFetcher(ccxt_client=kraken)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
            start_time=pd.to_datetime('2021-01-01 00:00:00Z', utc=True),
        )

        self.assertEqual(df.index[0], pd.to_datetime('2021-01-01 00:00:00Z', utc=True))

    def test_fetch_ohlcv_incremental(self):
        kraken = ccxt.kraken()
        fetcher = KrakenFetcher(ccxt_client=kraken)

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
        kraken = ccxt.kraken()
        fetcher = KrakenFetcher(ccxt_client=kraken)

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
        kraken = ccxt.kraken()
        fetcher = KrakenFetcher(ccxt_client=kraken)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=60,
            start_time=time.time() - 60 * 60
        )

        self.assertGreater(df.shape[0], 1)
        self.assertLess(df.shape[0], 61)

    def test_fetch_ohlcv_out_of_range(self):
        kraken = ccxt.kraken()
        fetcher = KrakenFetcher(ccxt_client=kraken)

        df = fetcher.fetch_ohlcv(
            market=market,
            interval_sec=24 * 60 * 60,
            start_time=time.time() + 60 * 60
        )

        self.assertIsNone(df)
