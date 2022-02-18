import time
import math
import pandas as pd
from .utils import smart_append, create_null_logger, normalize_to_unix


class KrakenFetcher:
    def __init__(self, logger=None, ccxt_client=None):
        self.logger = create_null_logger() if logger is None else logger
        self.ccxt_client = ccxt_client

    def fetch_ohlcv(self, df=None, start_time=None, interval_sec=None, market=None, price_type=None):
        if price_type is not None:
            raise Exception('price_type {} not implemented'.format(price_type))

        if start_time:
            from_time = int(math.floor(normalize_to_unix(start_time)))
        else:
            from_time = 1

        if df is not None and df.shape[0]:
            from_time = int(math.floor(df.index.max().timestamp() + interval_sec))

        dfs = []

        end_time = int(math.floor(time.time()))

        while from_time < end_time:
            data = self.ccxt_client.publicGetOHLC({
                'pair': market,
                'since': from_time - 1, # krakenはfrom_timeを含まない
                'interval': interval_sec // 60,
            })['result'][market]

            if len(data) == 0:
                self.logger.debug('len(data) == 0')
                break

            df2 = pd.DataFrame(data, columns=[
                'timestamp',
                'op',
                'hi',
                'lo',
                'cl',
                'vwap',
                'volume',
                'trade_count',
            ])[['timestamp', 'op', 'hi', 'lo', 'cl', 'volume']]

            df2 = df2[from_time <= df2['timestamp'].astype('int64')]
            if df2.shape[0] == 0:
                self.logger.debug('df2.shape[0] == 0')
                break

            df2['timestamp'] = pd.to_datetime(df2['timestamp'], unit='s', utc=True)

            for col in ['op', 'hi', 'lo', 'cl', 'volume']:
                df2[col] = df2[col].astype('float64')

            from_time = int(df2['timestamp'].max().timestamp()) + 1

            self.logger.debug('end_time {}'.format(end_time))
            dfs.append(df2)

        if len(dfs) == 0:
            return None if df is None else df.copy()
        else:
            df = smart_append(df, pd.concat(dfs).set_index('timestamp'))
            # 最後は未確定足なので削除
            df = df[df.index != df.index.max()]
            return df

