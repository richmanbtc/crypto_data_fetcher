import time
import numpy as np
import pandas as pd
import datetime
import urllib.request
from .utils import create_null_logger

class GmoFetcher:
    def __init__(self, logger=create_null_logger(), ccxt_client=None):
        self.logger = logger
        self.ccxt_client = ccxt_client

    def fetch_trades(self, market=None):
        today = datetime.datetime.now().date()

        for year in range(2018, today.year + 1):
            url = 'https://api.coin.z.com/data/trades/{}/{}/'.format(market, year)
            time.sleep(1)
            self.logger.debug(url)
            try:
                res = urllib.request.urlopen(url)
                if res.getcode() == 200:
                    start_year = year
                    break
            except urllib.error.HTTPError:
                pass

        for month in range(1, 13):
            url = 'https://api.coin.z.com/data/trades/{}/{}/{:02}/'.format(market, start_year, month)
            time.sleep(1)
            self.logger.debug(url)
            try:
                res = urllib.request.urlopen(url)
                if res.getcode() == 200:
                    start_month = month
                    break
            except urllib.error.HTTPError:
                pass

        date = datetime.date(start_year, start_month, 1)

        dfs = []
        while date < today:
            url = 'https://api.coin.z.com/data/trades/{}/{}/{:02}/{}{:02}{:02}_{}.csv.gz'.format(
                market,
                date.year,
                date.month,
                date.year,
                date.month,
                date.day,
                market.replace('/', '_'),
            )
            time.sleep(1)
            self.logger.debug(url)
            try:
                df = pd.read_csv(url)
                df = df.rename(columns={
                    'symbol': 'market',
                })
                df['price'] = df['price'].astype('float64')
                df['size'] = df['size'].astype('float64')
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df['side'] = np.where(df['side'] == 'BUY', 1, -1).astype('int8')
                dfs.append(df)
            except urllib.error.HTTPError:
                pass
            date += datetime.timedelta(days=1)

        df = pd.concat(dfs).reset_index(drop=True)

        return df



