import db_services.readwrite_utils as rw
import pandas as pd
import numpy as np

query = "SELECT * FROM pricedata.token_prices ORDER BY coin_name, utcdate;"
price_data = rw.get_data_from_one_table(query)
df = pd.DataFrame(data=price_data, columns=["symbol", "name", "btc", "usd", "eur", "cny", "utc", "epoch", "source"])

day_ts = 86400
day_intervals = [3, 7, 14, 30, 60, 90, 180, 365, 730, 1095]
di_index = range(1, len(day_intervals))
interval_text = [str(dp) + "d" for dp in day_intervals]
stat_text = ["min","max","open","close","min_post_max","max_post_min","days_to_max","days_to_min","days_to_min_post_max",
               "days_to_max_post_min", "pct_open_max",
             "pct_open_min","pct_open_close","pct_min_post_max","pct_max_post_min","pct_max_close","pct_min_close"]

def initialize_columns(_df, time_intervals, stats):
    for interval in time_intervals:
        for stat in stats:
            _df[str(interval) + "d_" + stat] = np.NaN

def get_max_interval_index(ci, time_intervals):
    max_i = 0
    if ci < 3:
        return ci
    for i in di_index:
        if ci >= 3 and ci < time_intervals[i]:
            max_i = i
            break
        else:
            max_i = len(time_intervals)
    return max_i

def add_stats(_df, time_intervals=day_intervals, stat_names=stat_text, currency='usd'):
    _cursor = 0
    initialize_columns(_df, time_intervals, stat_names)
    coin_indices = df.groupby('symbol',sort=False)['epoch'].count()
    for ci in coin_indices:
        max_index = _cursor + ci
        interval_i = get_max_interval_index(ci, time_intervals)
        if interval_i < min(time_intervals):
            _cursor += ci
            continue
        for i in day_intervals[0:interval_i]:
            df.loc[_cursor:max_index, [str(i) + "d_" + "open"]] = _df[currency][_cursor:max_index].shift(i)
            df.loc[_cursor:max_index, [str(i) + "d_" + "close"]] = _df[currency][_cursor:max_index]
            df.loc[_cursor:max_index, [str(i) + "d_" + "min"]] = _df[currency][_cursor:max_index].rolling(i).min()
            df.loc[_cursor:max_index, [str(i) + "d_" + "max"]] = _df[currency][_cursor:max_index].rolling(i).max()
            df.loc[_cursor:max_index, [str(i) + "d_" + "pct_open_max"] = _df[currency][_cursor:max_index]/df.loc[_cursor:max_index, [str(i) + "d_" + "max"]]
            df.loc[_cursor:max_index, [str(i) + "d_" + "pct_open_min"] = _df[currency][_cursor:max_index]/df.loc[_cursor:max_index, [str(i) + "d_" + "min"]]
            df.loc[_cursor:max_index, [str(i) + "d_" + "pct_open_close"] = _df[currency][_cursor:max_index]/df.loc[]

            _cursor += ci












###Mathematics & Pseudo Code for stats on a given day
###for
###Date-Range = cols[date-dayperiod:date]
###For first col
###Open = x[0], = Close x[0], Max Max(df[Price][D


