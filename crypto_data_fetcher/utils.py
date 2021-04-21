from logging import getLogger

def smart_append(df, other):
    if df is None:
        df = other.copy()
    else:
        df = df.append(other)
    df.sort_index(inplace=True)
    # https://stackoverflow.com/questions/13035764/remove-rows-with-duplicate-indices-pandas-dataframe-and-timeseries
    return df[~df.index.duplicated(keep='last')]

def create_null_logger():
    return getLogger(__name__ + 'null_logger')
