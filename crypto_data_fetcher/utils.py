from logging import getLogger

def smart_append(df, other):
    if other is None or other.shape[0] == 0:
        return df.copy()
    if df is None:
        df = other.copy()
    else:
        df = df.append(other)
    df.sort_index(inplace=True, kind='mergesort')
    # https://stackoverflow.com/questions/13035764/remove-rows-with-duplicate-indices-pandas-dataframe-and-timeseries
    return df[~df.index.duplicated(keep='last')]

def create_null_logger():
    return getLogger(__name__ + 'null_logger')

def normalize_to_unix(tm):
    if hasattr(tm, 'timestamp'):
        return tm.timestamp()
    else:
        return tm
