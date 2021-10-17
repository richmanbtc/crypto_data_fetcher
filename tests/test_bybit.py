import time
import ccxt
import pandas as pd
from unittest import TestCase
from crypto_data_fetcher.bybit import BybitFetcher

class TestBybit(TestCase):
    def test_fetch_ohlcv_initial(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
        )

        self.assertEqual(df['op'].iloc[0], 5740)
        self.assertEqual(df['hi'].iloc[0], 5740)
        self.assertEqual(df['lo'].iloc[0], 5241)
        self.assertEqual(df['cl'].iloc[0], 5601.5)
        self.assertEqual(df['volume'].iloc[0], 14779268)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

        # 未確定足が無いことの確認
        self.assertEqual(df.index.max(), pd.to_datetime((time.time() // (24 * 60 * 60) - 1) * (24 * 60 * 60), unit='s', utc=True))

    def test_fetch_ohlcv_start_time(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
            start_time=pd.to_datetime('2021-01-01 00:00:00Z', utc=True),
        )

        self.assertEqual(df.index[0], pd.to_datetime('2021-01-01 00:00:00Z', utc=True))

    def test_fetch_ohlcv_incremental(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
        )
        last_row = df.iloc[-1]
        df = df.iloc[:-1]

        df = fetcher.fetch_ohlcv(
            df=df,
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
        )
        self.assertTrue(df.iloc[-1].equals(last_row))
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_incremental_empty(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
        )
        before_count = df.shape[0]

        df = fetcher.fetch_ohlcv(
            df=df,
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
        )
        self.assertEqual(df.shape[0], before_count)

    def test_fetch_ohlcv_mark(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
            price_type='mark',
        )

        self.assertEqual(df['op'].iloc[0], 6145.56982421875)
        self.assertEqual(df['hi'].iloc[0], 6145.56982421875)
        self.assertEqual(df['lo'].iloc[0], 5324.22998046875)
        self.assertEqual(df['cl'].iloc[0], 5595.58984375)

        # 2個欠損しているらしい (より細かい足はあるから、bybitの集約の仕様だと思う)
        # 7   2018-11-22 00:00:00+00:00   4487.040039   4487.040039  4487.040039  4487.040039 2 days
        # 249 2019-07-23 00:00:00+00:00
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1 + 2) * 24 * 60 * 60)

    def test_fetch_ohlcv_index(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
            price_type='index',
        )

        self.assertEqual(df['op'].iloc[0], 3872.41)
        self.assertEqual(df['hi'].iloc[0], 3878.63)
        self.assertEqual(df['lo'].iloc[0], 3816.38)
        self.assertEqual(df['cl'].iloc[0], 3850.19)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_premium_index(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
            price_type='premium_index',
        )
        self.assertEqual(df['op'].iloc[0], -0.000346)
        self.assertEqual(df['hi'].iloc[0], 0.002467)
        self.assertEqual(df['lo'].iloc[0], -0.009693)
        self.assertEqual(df['cl'].iloc[0], -0.000026)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_calc_fr_from_premium_index(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df_premium_index = fetcher.fetch_ohlcv(
            market='BTCUSD',
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
            market='BTCUSD',
            interval_sec=60,
            start_time=time.time() - 60 * 60
        )

        self.assertGreater(df.shape[0], 1)
        self.assertLess(df.shape[0], 61)

    def test_fetch_ohlcv_out_of_range(self):
        bybit = ccxt.bybit()
        fetcher = BybitFetcher(ccxt_client=bybit)

        df = fetcher.fetch_ohlcv(
            market='BTCUSD',
            interval_sec=24 * 60 * 60,
            start_time=time.time() + 60 * 60
        )

        self.assertIsNone(df)
