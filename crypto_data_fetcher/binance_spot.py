import pandas as pd
from .utils import smart_append, create_null_logger, normalize_to_unix

class BinanceSpotFetcher:
    def __init__(self, logger=None, ccxt_client=None):
        self.logger = create_null_logger() if logger is None else logger
        self.ccxt_client = ccxt_client

    def fetch_ohlcv(self, df=None, start_time=None, interval_sec=None, market=None, price_type=None):
        if price_type is not None:
            raise Exception('price_type {} not implemented'.format(price_type))

        limit = 1500

        if start_time:
            from_time = int(normalize_to_unix(start_time) * 1000)
        else:
            from_time = 1

        if df is not None and df.shape[0]:
            from_time = int(df.index.max().timestamp() * 1000) + 1

        dfs = []

        while True:
            data = self.ccxt_client.publicGetKlines({
                'symbol': market,
                'startTime': from_time,
                'interval': format_interval_sec(interval_sec),
                'limit': limit
            })
            if len(data) == 0:
                break

            df2 = pd.DataFrame(data, columns=[
                'timestamp',
                'op',
                'hi',
                'lo',
                'cl',
                'volume',
                'close_time',
                'quote_asset_volume',
                'trades',
                'taker_buy_base_asset_volume',
                'taker_buy_quote_asset_volume',
                'ignore',
            ])[['timestamp', 'op', 'hi', 'lo', 'cl', 'volume']]

            # binanceはfrom_time未満が返ることがあるので
            df2 = df2[from_time <= df2['timestamp'].astype('int64')]
            if df2.shape[0] == 0:
                break

            df2['timestamp'] = pd.to_datetime(df2['timestamp'], unit='ms', utc=True)

            for col in ['op', 'hi', 'lo', 'cl', 'volume']:
                df2[col] = df2[col].astype('float64')

            from_time = int(df2['timestamp'].max().timestamp() * 1000) + 1
            dfs.append(df2)

        if len(dfs) == 0:
            return None if df is None else df.copy()
        else:
            df = smart_append(df, pd.concat(dfs).set_index('timestamp'))
            # 最後は未確定足なので削除
            df = df[df.index != df.index.max()]
            return df

    # not implemented
    # def fetch_fr(self, df=None, start_time=None, market=None):
    #     pass

def format_interval_sec(interval_sec):
    interval_min = interval_sec // 60
    if interval_min < 60:
        return '{}m'.format(interval_min)
    if interval_min < 24 * 60:
        return '{}h'.format(interval_min // 60)
    else:
        return '{}d'.format(interval_min // (24 * 60))
