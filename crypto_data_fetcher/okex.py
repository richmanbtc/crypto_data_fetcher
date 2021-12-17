from numbers import Number
from typing import Optional, Union
import time
import pandas as pd
from .utils import smart_append, create_null_logger, normalize_to_unix
from .types import IHasTimestamp


class OkexFetcher:
    def __init__(self, logger=None, ccxt_client=None):
        self.logger = create_null_logger() if logger is None else logger
        self.ccxt_client = ccxt_client

    def fetch_ohlcv(self, df: Optional[pd.DataFrame], start_time: Optional[Union[IHasTimestamp, Number]],
                    interval_sec: int,
                    market: str, price_type: Optional[str]) -> Optional[pd.DataFrame]:
        if price_type is not None:
            raise Exception('price_type {} not implemented'.format(price_type))

        if start_time:
            from_time = int(normalize_to_unix(start_time) * 1000)
        else:
            from_time = 1

        if df is not None and df.shape[0]:
            from_time = int((df.index.max().timestamp() + interval_sec) * 1000)

        dfs = []

        end_time = int(time.time() * 1000 - 1)

        while from_time < end_time:
            data = self.ccxt_client.publicGetMarketHistoryCandles({
                'instId': market,
                'after': end_time,  # end_time未満 (ftxと違い、end_timeを含まない)
                'bar': format_interval_sec(interval_sec),
                # limit: max is 100, default is 100
            })['data']
            if len(data) == 0:
                self.logger.debug('len(data) == 0')
                break

            df2 = pd.DataFrame(data, columns=[
                'timestamp',
                'op',
                'hi',
                'lo',
                'cl',
                'volume',
                'quote_asset_volume',
            ])[['timestamp', 'op', 'hi', 'lo', 'cl', 'volume']]

            df2 = df2[from_time <= df2['timestamp'].astype('int64')]
            if df2.shape[0] == 0:
                self.logger.debug('df2.shape[0] == 0')
                break

            df2['timestamp'] = pd.to_datetime(df2['timestamp'], unit='ms', utc=True)

            for col in ['op', 'hi', 'lo', 'cl', 'volume']:
                df2[col] = df2[col].astype('float64')

            end_time = int(df2['timestamp'].min().timestamp() * 1000)
            self.logger.debug('end_time {}'.format(end_time))
            dfs.append(df2)

        if len(dfs) == 0:
            return None if df is None else df.copy()
        else:
            df = smart_append(df, pd.concat(dfs).set_index('timestamp'))
            # okexは未確定足が返ってこないので削除不要
            return df

    # not implemented
    # def fetch_fr(self, df=None, start_time=None, market=None):
    #     pass


def format_interval_sec(interval_sec):
    interval_min = interval_sec // 60
    if interval_min < 60:
        return '{}m'.format(interval_min)
    if interval_min < 24 * 60:
        return '{}H'.format(interval_min // 60)
    else:
        return '{}D'.format(interval_min // (24 * 60))
