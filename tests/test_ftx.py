import time
import ccxt
import pandas as pd
from unittest import TestCase, mock
from crypto_data_fetcher.ftx import FtxFetcher

class TestFtx(TestCase):
    def test_fetch_ohlcv_initial(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
        )
        print(df)

        self.assertEqual(df['op'].iloc[0], 10564.25)
        self.assertEqual(df['hi'].iloc[0], 11108.75)
        self.assertEqual(df['lo'].iloc[0], 10385.25)
        self.assertEqual(df['cl'].iloc[0], 10765.75)
        self.assertEqual(df['volume'].iloc[0], 117203195.32235)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

        # 未確定足が無いことの確認
        self.assertEqual(df.index.max(), pd.to_datetime((time.time() // (24 * 60 * 60) - 1) * (24 * 60 * 60), unit='s', utc=True))

    def test_fetch_ohlcv_index(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
            price_type='index',
        )
        print(df)

        self.assertEqual(df['op'].iloc[0], 10532.400561321)
        self.assertEqual(df['hi'].iloc[0], 11094.361620957)
        self.assertEqual(df['lo'].iloc[0], 10385.558948835)
        self.assertEqual(df['cl'].iloc[0], 10758.259167146)
        self.assertTrue('volume' not in df.columns)
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

        # 未確定足が無いことの確認
        self.assertEqual(df.index.max(), pd.to_datetime((time.time() // (24 * 60 * 60) - 1) * (24 * 60 * 60), unit='s', utc=True))

    def test_fetch_ohlcv_start_time(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
            start_time=pd.to_datetime('2021-01-01 00:00:00Z', utc=True),
        )

        self.assertEqual(df.index[0], pd.to_datetime('2021-01-01 00:00:00Z', utc=True))

    def test_fetch_ohlcv_incremental(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
        )
        last_row = df.iloc[-1]
        df = df.iloc[:-1]

        df = fetcher.fetch_ohlcv(
            df=df,
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
        )
        self.assertTrue(df.iloc[-1].equals(last_row))
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1) * 24 * 60 * 60)

    def test_fetch_ohlcv_incremental_empty(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
        )
        before_count = df.shape[0]

        df = fetcher.fetch_ohlcv(
            df=df,
            market='BTC-PERP',
            interval_sec=24 * 60 * 60,
        )
        self.assertEqual(df.shape[0], before_count)

    def test_fetch_fr_initial(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_fr(
            market='BTC-PERP',
        )

        self.assertEqual(df['fr'].iloc[0], 1e-05)
        # 4つ欠損があるらしい
        # 146  2019-07-26 04:00:00+00:00  0.000013 0 days 03:00:00
        # 2351 2019-10-26 03:00:00+00:00 -0.001415 0 days 03:00:00
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1 + 4) * 60 * 60)

        # 現在足まで取れていることの確認
        self.assertEqual(df.index.max(), pd.to_datetime(time.time() // (60 * 60) * (60 * 60), unit='s', utc=True))

    def test_fetch_fr_start_time(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_fr(
            market='BTC-PERP',
            start_time=pd.to_datetime('2021-01-01 00:00:00Z', utc=True),
        )

        self.assertEqual(df.index[0], pd.to_datetime('2021-01-01 00:00:00Z', utc=True))

    def test_fetch_fr_incremental(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_fr(
            market='BTC-PERP',
        )
        last_row = df.iloc[-1]
        df = df.iloc[:-1]

        df = fetcher.fetch_fr(
            df=df,
            market='BTC-PERP',
        )
        self.assertTrue(df.iloc[-1].equals(last_row))
        # 4つ欠損があるらしい
        # 146  2019-07-26 04:00:00+00:00  0.000013 0 days 03:00:00
        # 2351 2019-10-26 03:00:00+00:00 -0.001415 0 days 03:00:00
        self.assertEqual(df.index[-1].timestamp() - df.index[0].timestamp(), (df.shape[0] - 1 + 4) * 60 * 60)

    def test_fetch_fr_incremental_empty(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_fr(
            market='BTC-PERP',
        )
        before_count = df.shape[0]

        df = fetcher.fetch_fr(
            df=df,
            market='BTC-PERP',
        )
        self.assertEqual(df.shape[0], before_count)

    def test_fetch_ohlcv_initial_minute(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-PERP',
            interval_sec=60,
            start_time=time.time() - 60 * 60
        )

        self.assertGreater(df.shape[0], 1)
        self.assertLess(df.shape[0], 61)

    def test_fetch_ohlcv_out_of_range(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-20201225',
            interval_sec=24 * 60 * 60,
            start_time=time.time() - 7 * 24 * 60 * 60
        )

        self.assertIsNone(df)

    def test_fetch_fr_out_of_range(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_fr(
            market='BTC-20201225',
            start_time=time.time() - 7 * 24 * 60 * 60
        )

        self.assertIsNone(df)

    def test_fetch_fr_old_future(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_fr(
            market='BNB-20190329',
        )

        self.assertIsNone(df)

    def test_find_total_end_time(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        end_time = fetcher._find_total_end_time(
            market='BTC-20201225',
        )

        self.assertEqual(end_time, 1608865200)

    @mock.patch('time.time', mock.MagicMock(return_value=12345))
    def test_find_total_end_time_spot(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        end_time = fetcher._find_total_end_time(
            market='BTC/USD',
        )

        self.assertEqual(end_time, 12345 - 1)

    @mock.patch('time.time', mock.MagicMock(return_value=12345))
    def test_find_total_end_time_perp(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        end_time = fetcher._find_total_end_time(
            market='BTC-PERP',
        )

        self.assertEqual(end_time, 12345 - 1)

    def test_fetch_ohlcv_old_future(self):
        ftx = ccxt.ftx()
        fetcher = FtxFetcher(ccxt_client=ftx)

        df = fetcher.fetch_ohlcv(
            market='BTC-20201225',
            interval_sec=24 * 60 * 60,
        )

        self.assertEqual(df.index[0], pd.to_datetime('2020-06-08 00:00:00Z', utc=True))

    # 後でなおす
    # def test_fetch_ohlcv_bug(self):
    #     ftx = ccxt.ftx()
    #     fetcher = FtxFetcher(ccxt_client=ftx)
    #
    #     print(fetcher._find_total_end_time(
    #         market='MKR-20200626',
    #     ))
    #
    #     df = fetcher.fetch_ohlcv(
    #         market='MKR-20200626',
    #         interval_sec=300,
    #     )
    #
    #     self.assertEqual(df.index[0], pd.to_datetime('2020-06-08 00:00:00Z', utc=True))
