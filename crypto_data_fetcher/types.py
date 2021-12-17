from numbers import Number
from typing import Protocol, Union, Optional
import pandas as pd


class IHasTimestamp(Protocol):
    def timestamp(self) -> Number:
        ...


class IDataFetcher(Protocol):
    def fetch_ohlcv(self, df: Optional[pd.DataFrame], start_time: Optional[Union[IHasTimestamp, Number]], interval_sec: int,
                    market: str, price_type: Optional[str]) -> Optional[pd.DataFrame]:
        """ohlcvをpd.DataFrameで取得

        - dfが存在するときはdfに追記。start_timeは無視される
        - start_timeが存在するときはstart_time以降を取得
        - start_timeが存在しないときは最初から取得
        """
        ...
