import time
import ccxt
import pandas as pd
from unittest import TestCase
from crypto_data_fetcher.bybit import BybitFetcher

class TestBybitLinear(TestCase):
    def test_fetch_ohlcv_initial(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
        )

        self.assertEqual(df['op'].iloc[0], 6500)
        self.assertEqual(df['hi'].iloc[0], 6745.5)
        self.assertEqual(df['lo'].iloc[0], 6500)
        self.assertEqual(df['cl'].iloc[0], 6698.5)
        self.assertEqual(df['volume'].iloc[0], 1809.520)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

        # 未確定足が無いことの確認
        self.assertEqual(df.index.max(), pd.to_datetime((time.time() // (24 * 60 * 60) - 1) * (24 * 60 * 60), unit='s', utc=True))

    def test_fetch_ohlcv_start_time(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
            start_time=pd.to_datetime('2021-01-01 00:00:00Z', utc=True),
        )

        self.assertEqual(df.index[0], pd.to_datetime('2021-01-01 00:00:00Z', utc=True))

    def test_fetch_ohlcv_incremental(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
        )
        last_row = df.iloc[-1]
        df = df.iloc[:-1]

        df = fetcher.fetch_ohlcv(
            df=df,
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
        )
        self.assertTrue(df.iloc[-1].equals(last_row))
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_incremental_empty(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
        )
        before_count = df.shape[0]

        df = fetcher.fetch_ohlcv(
            df=df,
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
        )
        self.assertEqual(df.shape[0], before_count)

    def test_fetch_ohlcv_mark(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
            price_type='mark',
        )
        print(df)

        self.assertEqual(df['op'].iloc[0], 6718.21)
        self.assertEqual(df['hi'].iloc[0], 6955.88)
        self.assertEqual(df['lo'].iloc[0], 6451.31)
        self.assertEqual(df['cl'].iloc[0], 6677.77)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_index(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
            price_type='index',
        )
        print(df)

        self.assertEqual(df['op'].iloc[0], 6670.01)
        self.assertEqual(df['hi'].iloc[0], 6956.84)
        self.assertEqual(df['lo'].iloc[0], 6450.96)
        self.assertEqual(df['cl'].iloc[0], 6677.77)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_premium_index(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
            price_type='premium_index',
        )
        print(df)

        self.assertEqual(df['op'].iloc[0], 0.000100)
        self.assertEqual(df['hi'].iloc[0], 0.000360)
        self.assertEqual(df['lo'].iloc[0], -0.000865)
        self.assertEqual(df['cl'].iloc[0], -0.000097)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_calc_fr_from_premium_index(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df_premium_index = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
            price_type='premium_index',
        )
        df = fetcher.calc_fr_from_premium_index(df_premium_index=df_premium_index)
        print(df)

        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_initial_minute(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=60,
            start_time=time.time() - 60 * 60
        )

        self.assertGreater(df.shape[0], 1)
        self.assertLess(df.shape[0], 61)

    def test_fetch_ohlcv_out_of_range(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSDT',
            interval_sec=24 * 60 * 60,
            start_time=time.time() + 60 * 60
        )

        self.assertIsNone(df)
