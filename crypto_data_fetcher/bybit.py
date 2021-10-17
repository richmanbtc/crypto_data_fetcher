import pandas as pd
from .utils import smart_append, create_null_logger, normalize_to_unix

class BybitFetcher:
    def __init__(self, logger=None, ccxt_client=None):
        self.logger = create_null_logger() if logger is None else logger
        self.ccxt_client = ccxt_client

    def fetch_ohlcv(self, df=None, start_time=None, interval_sec=None, market=None, price_type=None):
        limit = 200

        if start_time:
            from_time = int(normalize_to_unix(start_time))
        else:
            from_time = 1

        if df is not None and df.shape[0]:
            from_time = int(df.index.max().timestamp()) + 1

        interval = {
            1: 'D',
            7: 'W',
        }.get(interval_sec // (24 * 60 * 60)) or interval_sec // 60

        is_linear = market.endswith('USDT')

        dfs = []

        while True:
            self.logger.debug('{} {}'.format(market, from_time))

            if price_type == 'mark':
                if is_linear:
                    data = self.ccxt_client.publicLinearGetMarkPriceKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
                else:
                    data = self.ccxt_client.v2PublicGetMarkPriceKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
            elif price_type == 'index':
                if is_linear:
                    data = self.ccxt_client.publicLinearGetIndexPriceKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
                else:
                    data = self.ccxt_client.v2PublicGetIndexPriceKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
            elif price_type == 'premium_index':
                if is_linear:
                    data = self.ccxt_client.publicLinearGetPremiumIndexKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
                else:
                    data = self.ccxt_client.v2PublicGetPremiumIndexKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
            else:
                if is_linear:
                    data = self.ccxt_client.publicLinearGetKline({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']
                else:
                    data = self.ccxt_client.v2PublicGetKlineList({
                        'symbol': market,
                        'from': from_time,
                        'interval': interval,
                        'limit': limit
                    })['result']

            if data is None or len(data) <= 1: # 最後を取り除くので最低2個必要
                break

            # self.logger.debug(data)

            df2 = pd.DataFrame(data)
            columns = ['timestamp', 'op', 'hi', 'lo', 'cl']
            if price_type is None:
                columns.append('volume')
            if 'open_time' in df2.columns and 'start_at' in df2.columns:
                df2 = df2.drop(columns=['start_at'])
            df2 = df2.rename(columns={
                'open': 'op',
                'high': 'hi',
                'low': 'lo',
                'close': 'cl',
                'open_time': 'timestamp',
                'start_at': 'timestamp',
            })[columns]
            df2['timestamp'] = pd.to_datetime(df2['timestamp'], unit='s', utc=True)

            # 最後は未確定足なので削除
            df2 = df2[df2['timestamp'] != df2['timestamp'].max()]

            for col in ['op', 'hi', 'lo', 'cl', 'volume']:
                if col in columns:
                    df2[col] = df2[col].astype('float64')

            from_time = int(df2['timestamp'].max().timestamp()) + 1
            dfs.append(df2)

            if len(data) < limit:
                break

        if len(dfs) == 0:
            return None if df is None else df.copy()
        else:
            return smart_append(df, pd.concat(dfs).set_index('timestamp'))

    # df_premium_index has to be minutes data for accurate fr
    def calc_fr_from_premium_index(self, df_premium_index=None):
        df_premium_index = df_premium_index.reset_index()

        df_premium_index['timestamp'] = df_premium_index['timestamp'].dt.floor('8H') + pd.to_timedelta(2 * 8, unit='hour')
        df2 = pd.concat([
            df_premium_index.groupby('timestamp')['cl'].mean()
        ], axis=1)

        interest_rate = (0.0006 - 0.0003) / 3.0
        df2['fr'] = df2['cl'] + (interest_rate  - df2['cl']).clip(-0.0005, 0.0005)
        df2 = df2[['fr']]
        return df2
