import db_services.readwrite_utils as rw
import res.db_data.sql_statement_library as psqll
import pandas as pd
import numpy as np
import log_service.logger_factory as lf
import data_scraping.coin_infosite_service.coinmarketcap as cmc
logging = lf.get_loggly_logger(__name__)
import custom_utils.type_validation as tv
import custom_utils.datautils as du
import time
import redis
lf.launch_logging_service()
cmc_data = cmc.get_coin_list()


_MINUTE_ = 60
_HOUR_ = 24
_DAY_ = 86400
_WEEK_ = 604800
_YEAR_ = 31536000

default_functions = {"max": "df[feature][(df_ix[i] - ts):df_ix[i]].max()",
                 "min": "df[feature][(df_ix[i] - ts):df_ix[i]].min()",
                 "mean": "df[feature][(df_ix[i] - ts):df_ix[i]].mean()",
                 "median": "df[feature][(df_ix[i] - ts):df_ix[i]].median()",
                 "std": "df[feature][(df_ix[i] - ts):df_ix[i]].std()",
                 "var": "df[feature][(df_ix[i] - ts):df_ix[i]].var()",
                 "kurtosis": "df[feature][(df_ix[i] - ts):df_ix[i]].kurtosis"}
default_function_list = default_functions.keys()

def populate_coin_attributes():
    coin_data = cmc_data
    coin_data = {coin["symbol"]: coin for coin in coin_data}
    symbols = coin_data.keys()
    for symbol in symbols:
        for x in ("start_date", "start_date_utc", "amount_premined",
                  "fully_premined", "derivative_token", "parent_blockchain"):
            coin_data[symbol][x] = None
        for x in ('market_cap_usd', 'price_usd', 'last_updated', '24h_volume_usd',
                  'percent_change_7d','24h_volume_usd','rank','total_supply','price_btc','percent_change_24h',
                  'available_supply','percent_change_1h'):
            if coin_data[symbol][x] is not None:
                coin_data[symbol][x] = float(coin_data[symbol][x])
    token_metadata = rw.get_data_from_one_table("Select * from pricedata.static_token_data")
    for entry in token_metadata:
        current_symbol = entry[0]
        if current_symbol in symbols:
            coin_data[current_symbol]["start_date"] = entry[2]
            if isinstance(entry[3], (int, long, float)):
                coin_data[current_symbol]["start_date_utc"] = int(entry[3])
            if isinstance(entry[6], (int, long, float)):
                coin_data[current_symbol]["amount_premined"] = int(entry[6])
            coin_data[current_symbol]["fully_premined"] = entry[7]
            coin_data[current_symbol]["derivative_token"] = entry[15]
            coin_data[current_symbol]["parent_blockchain"] = entry[16]
        else:
            coin_data[current_symbol] = {'derivative_token': entry[15]
                , 'market_cap_usd': None
                , 'price_usd': None
                , 'last_updated': None
                , 'name': entry[1]
                , 'start_date': entry[2]
                , '24h_volume_usd': None
                , 'percent_change_7d': None
                , 'symbol': entry[0]
                , 'start_date_utc': None
                , 'max_supply': None
                , 'rank': None
                , 'percent_change_1h': None
                , 'total_supply': None
                , 'price_btc': None
                , 'fully_premined': entry[7]
                , 'available_supply': None
                , 'amount_premined': None
                , 'percent_change_24h': None
                , 'parent_blockchain': None
                , 'id': entry[8]}
            coin_data[current_symbol]["start_date"] = entry[2]
            if isinstance(entry[3], (int, long, float)):
                coin_data[current_symbol]["start_date_utc"] = int(entry[3])
            if isinstance(entry[6], (int, long, float)):
                coin_data[current_symbol]["amount_premined"] = int(entry[6])
            if isinstance(entry[5],(int, long, float)):
                coin_data[current_symbol]["total_supply"] = int(entry[5])
        if coin_data[current_symbol]['symbol'] == coin_data[current_symbol]['parent_blockchain']:
            coin_data[current_symbol]['parent_blockchain'] = None
            coin_data[current_symbol]['derivative_token'] = False
    return coin_data

coin_data = populate_coin_attributes()

def create_default_df(from_csv=None, custom_measures=None
                      , custom_intervals=None, days_from_present=None
                      , custom_time_range=None, date_format=None):
    if isinstance(from_csv, str):
        df = pd.read_csv(from_csv)
        intervals = [3, 7, 14, 30, 60, 90, 120, 360, 720, 1080]
        df_ix = df.groupby('coin_symbol', sort=False).size().tolist()
        ix = [0]
        count = 0
        for i in range(0, len(df_ix)):
            ix.append(df_ix[i] + count)
            count += df_ix[i]
        return df, ix, intervals

    if isinstance(custom_measures, (tuple, list)):
        measures = custom_measures
    elif(custom_measures != None): raise ValueError("custom_measures "
                                                    "must be specified in list or tuple format")
    else:
        measures = ["7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                    "ewma", "ewma_std", "open", "close", "high", "low",
                    "pct_openclose", "pct_openhi", "pct_openlo", "pct_hilo",
                    "pct_hiclose", "pct_loclose", "pct_lifetime", "idx_high",
                    "idx_low", "time_hi", "time_lo", "days_openhi", "days_openlow",
                    "days_hiclose", "days_loclose", "ewma_open", "ewma_close", "ewma_high", "ewma_low",
                    "ewma_pct_openclose", "ewma_pct_openhi", "ewma_pct_openlo", "ewma_pct_hilo",
                    "ewma_pct_hiclose", "ewma_pct_loclose", "ewma_pct_lifetime", "ewma_idx_high",
                    "ewma_idx_low", "ewma_time_hi", "ewma_time_lo", "days_openhi_ewma", "days_openlow_ewma",
                    "days_hiclose_ewma", "days_loclose_ewma"]

    if isinstance(custom_intervals, (tuple,list)):
        intervals = custom_intervals
    elif(custom_intervals != None): raise ValueError("custom_intervals "
                                                     "must be specified in list or tuple format")
    else:
        intervals = [3, 7, 14, 30, 60, 90, 120, 360, 720, 1080]

    print "made it this frar"
    reads = psqll.crypto_price_data_statements(read=True)
    print reads
    col_query = reads["price_columns"]
    if isinstance(days_from_present,(int, float)) and custom_time_range == None:
        query = psqll.price_history_in_custom_date_range(days=days_from_present,
                                                         custom_format=date_format)
    elif isinstance(custom_time_range, (list,tuple)) and days_from_present == None:
        query = psqll.price_history_in_custom_date_range(custom_range=custom_time_range,
                                                         custom_format=date_format)
    else:
        query = reads["price_history_get_all"]
    columns = [x[0] for x in rw.get_data_from_one_table(col_query)]
    interval_independent_measures = ["ewma", "ewma_std","7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny"]
    df = rw.get_data_from_one_table(query)
    df = pd.DataFrame(data=df, columns=columns)
    df.sort_values(["coin_symbol", "uts"], inplace=True)

    for m in interval_independent_measures:
        df[m] = np.NaN
        df[m] = df[m].astype(object)
    for iv in intervals:
        for m in measures:
            if m in interval_independent_measures:
                continue
            else:
                df[str(iv) + "d_" + m] = np.NaN
                df[str(iv) + "d_" + m] = df[str(iv) + "d_" + m].astype(object)
    df.reset_index(inplace=True)
    df["index"] = df.index.get_values().astype(object)
    df_ix = df.groupby('coin_symbol', sort=False).size().tolist()
    ix = [0]
    count = 0
    for i in range(0,len(df_ix)):
        ix.append(df_ix[i] + count)
        count += df_ix[i]
    return df, ix, intervals

def create_measures(band_multiple=1, custom_measures=None, custom_intervals=None, days_from_present=None
                      , custom_time_range=None, date_format=None, measure_percent_increases=False, pct_increases=None,
                    pct_study_periods=None, add_coin_attributes=False, alpha=0.2):
    df, df_ix, intervals = create_default_df(custom_measures=custom_measures,
                                             custom_intervals=custom_intervals,
                                             days_from_present=days_from_present,
                                             custom_time_range=custom_time_range,
                                             date_format=date_format)
    if measure_percent_increases == True and not (tv.validate_list_element_types(pct_increases, (int, float)) and tv.validate_list_element_types(pct_study_periods, (int, float))):
        raise ValueError("If percent increase measurement is enabled, both increases and increase periods must be specified as lists or tuples or integers or floats ")

    if measure_percent_increases == True and 0 in pct_increases:
        raise ValueError("Cannot measure a 0% increase")

    if add_coin_attributes == True:
        attributes = ('derivative_token', 'market_cap_usd','start_date'
                      , '24h_volume_usd', 'start_date_utc', 'max_supply',
                      'total_supply',  'fully_premined', 'available_supply',
                      'amount_premined', 'parent_blockchain')
        coin_attributes = populate_coin_attributes()

    for i in range(1, len(df_ix)):
        logging.info("Index being measured is %s", df_ix[i])
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_std"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=alpha, min_periods=3).std()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_btc"] = df["price_btc"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_usd"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_eur"] = df["price_eur"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_cny"] = df["price_cny"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        if add_coin_attributes == True:
            symbol = df["coin_symbol"][df_ix[i-1] + 1]
            for attribute in attributes:
                if coin_attributes[symbol][attribute] == None:
                    df.loc[df.index[df_ix[i - 1]:df_ix[i]], attribute] = np.NaN
                else:
                    df.loc[df.index[df_ix[i - 1]:df_ix[i]], attribute] = coin_attributes[symbol][attribute]

    medcols = ["7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny"]
    df[medcols] = df[medcols].replace({0: np.nan})

    for i in range(1, len(df_ix)):
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_usd"] = np.where((df['price_usd'][df_ix[i - 1]:df_ix[i]] > 0) & (df['ewma_std'][df_ix[i - 1]:df_ix[i]] >= 60) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 5), df['7d_med_usd'][df_ix[i - 1]:df_ix[i]], df['price_usd'][df_ix[i - 1]:df_ix[i]])
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_btc"] = np.where((df['price_btc'][df_ix[i - 1]:df_ix[i]] > 0) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 5), df['7d_med_btc'][df_ix[i - 1]:df_ix[i]], df['price_btc'][df_ix[i - 1]:df_ix[i]])
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_cny"] = np.where((df['price_cny'][df_ix[i - 1]:df_ix[i]] > 0) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 5), df['7d_med_cny'][df_ix[i - 1]:df_ix[i]], df['price_cny'][df_ix[i - 1]:df_ix[i]])
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_eur"] = np.where((df['price_eur'][df_ix[i - 1]:df_ix[i]] > 0) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 5), df['7d_med_eur'][df_ix[i - 1]:df_ix[i]], df['price_eur'][df_ix[i - 1]:df_ix[i]])
        fwd = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=alpha, min_periods=1).mean()
        bwd = df["price_usd"][df_ix[i - 1]:df_ix[i]][::-1].ewm(alpha=alpha,min_periods=1).mean()
        c = np.vstack((fwd, bwd[::-1]))
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma"] = np.mean(c, axis=0 )
        fwd = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=alpha, min_periods=1).std()
        bwd = df["price_usd"][df_ix[i - 1]:df_ix[i]][::-1].ewm(alpha=alpha,min_periods=1).std()
        c = np.vstack((fwd, bwd[::-1]))
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_std"] = np.mean(c, axis=0 )
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_top"] = df["ewma"][df_ix[i - 1]:df_ix[i]] + df["ewma_std"][df_ix[i - 1]:df_ix[i]]*band_multiple
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_low"] = df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewma_std"][df_ix[i - 1]:df_ix[i]]*band_multiple
    #   df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_mult"] = (df["ewma"][df_ix[i - 1]:df_ix[i]] + df["ewma_std"][df_ix[i - 1]:df_ix[i]])/df["ewma"][df_ix[i - 1]:df_ix[i]]
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "log_returns"] = np.log(df["price_usd"]) - np.log(df["price_usd"].shift(1))
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "return_variability"] = np.where((df["ewma_low"][df_ix[i - 1]:df_ix[i]] > 0), df["ewma_top"][df_ix[i - 1]:df_ix[i]]/df["ewma_low"][df_ix[i - 1]:df_ix[i]], 0)

    for i in range(1, len(df_ix)):
        logging.info("Index being measured is %s", df_ix[i])
        int_len = df_ix[i] - df_ix[i - 1]
        for iv in intervals:
            if iv > int_len:
                break
            df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_daily_volatility"] = df["log_returns"][df_ix[i - 1]:df_ix[i]].rolling(iv).std()
            df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_mean_volatility"] = df["log_returns"][ df_ix[i - 1]:df_ix[i]].rolling(iv).mean()
            df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_volatility_ratio"] = (1+df[str(iv) + "d_daily_volatility"][df_ix[i - 1]:df_ix[i]])/(1 + np.abs(df[str(iv) + "d_mean_volatility"][ df_ix[i - 1]:df_ix[i]]))

    if measure_percent_increases == True:
        increase_percentages = pct_increases
        _periods = pct_study_periods

        examination_periods = []
        modifier = 0
        for p in _periods:
            end = 0
            if isinstance(modifier, (int, float)):
                p += modifier
                end += modifier
            examination_periods.append((str(p) + "_day_period", du.get_epoch(days=p, start_of_day=True),du.get_epoch(days=end, start_of_day=True)))

        for i in range(1, len(df_ix)):
            if len(df[df_ix[i - 1]:df_ix[i]][df.price_usd > 0.0]) <= 0:
                continue
            coin_opening_price = df[df_ix[i - 1]:df_ix[i]][df.price_usd > 0.0]["price_usd"].iloc[0]
            df.loc[df.index[df_ix[i - 1]:df_ix[i]], "pct_change_since_ico"] = df["price_usd"][df_ix[i - 1]:df_ix[
                i]] / coin_opening_price - 1

        start_time = time.time()
        for i in range(1, len(df_ix)):
            print "at index %s" % df_ix[i - 1]
            now = (time.time() - start_time) / 60
            print "minutes since start %s" % now
            if len(df[df_ix[i - 1]:df_ix[i]][df.price_usd > 0.0]) <= 0:
                continue
            for p in examination_periods:
                if len(df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1]) & (df.price_usd > 0)]) <= 0: continue
                period_opening_price = \
                df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1]) & (df.price_usd > 0)]["price_usd"].iloc[0]
                column_name = str(int((p[2] - p[1]) / 86400)) + "_day_pct_change_from_" + du.convert_epoch(p[1],e2str=True, custom_format="%Y%m%d")
                df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_" + column_name] = \
                    df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1])]["price_usd"] / period_opening_price - 1
                df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_" + column_name] = \
                    df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1])]["ewma"] / period_opening_price - 1
                for pct in increase_percentages:
                    first_pct = \
                    df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])]['ewma_' + column_name].iloc[0]
                    if pct > 0:
                        a = df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])].groupby(
                            (df['ewma_' + column_name] < pct).cumsum()).cumcount(ascending=False)
                        b = (a.shift(1) < a) & (a.shift(-1) < a)
                        if (first_pct > pct):
                            b.iloc[0] = True
                    else:

                        a = df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])].groupby(
                            (df['ewma_' + column_name] > pct).cumsum()).cumcount(ascending=False)
                        b = (a.shift(1) < a) & (a.shift(-1) < a)
                        if first_pct < pct:
                            b.iloc[0] = True
                    if pct > 0:
                        pct_column_name = "open_" + str(int((p[2] - p[1]) / 86400)) \
                                          + "_"  + str(int(pct * 100)) + "_pct_increase"
                    else:
                        pct_column_name = "open_" + str(int((p[2] - p[1]) / 86400)) \
                        + "_" + str(int(abs(pct * 100))) + "_pct_decrease"
                    df.loc[df.index[df_ix[i - 1]:df_ix[i]], pct_column_name] = 0
                    for item in a[b].iteritems():
                        df[pct_column_name].iloc[item[0]] = item[1]
    return df, df_ix, intervals

def create_periods(pct_increases, time_windows):
    column_selections = []
    for window in time_windows:
        for increase in pct_increases:
            if increase > 0:
                column_selections.append("open_"+ str(window) +"_"+ str(int(100*increase))+"_pct_increase")
            else:
                column_selections.append("open_" + str(window) + "_" + str(int(abs(100 * increase))) + "_pct_decrease")
    return tuple(column_selections)

def create_frequency_df(df, pct_increases,time_windows,target_attributes, min_days):
    increase_periods = create_periods(pct_increases, time_windows)
    if len(increase_periods) < 32:
        df_query = ""
        for i in range(0,len(increase_periods)):
            df_query += str(increase_periods[i]) + " > " + str(min_days)
            if i < len(increase_periods) - 1:
                df_query += " | "
        sampling_df = df.query(df_query)
        return sampling_df.loc[:,tuple(target_attributes) + tuple(increase_periods)]
    else:
        return df.loc[:,tuple(target_attributes) + tuple(increase_periods)]

def create_stats_df(df, target_attributes, pct_increases, time_windows, min_period=10):
    data = []
    target_attributes = tuple(x for x in target_attributes if x != 'start_date')
    if isinstance(pct_increases, (tuple, list)) and (time_windows, (tuple,list)):
        for window in time_windows:
            for increase in pct_increases:
                if increase < 0:
                    period = "open_"+str(window)  + "_"+str(int(abs(increase*100)))+"_pct_decrease"
                else:
                    period = "open_"+ str(window) +"_"+ str(int(100*increase))+"_pct_increase"
                sampling_df_columns = tuple(target_attributes)
                sampling_df = df[df[period] > min_period].loc[:,("coin_symbol",) + sampling_df_columns]
                for c in sampling_df_columns:
                    series = sampling_df[c]
                    if ("utc" in c) or ("uts" in c):
                        series = series[series > 0]
                    print(series)
                    data.append((period, window, increase, c, series.mean(), series.median(),series.mad(),series.std(), series.kurt(), series.quantile(.75)))
    else:
        raise ValueError("Valid periods and time windows must be specified")
    return pd.DataFrame(data=data,columns=['period_name','period','increase', 'feature', 'mean','median', 'MAE', 'STD', 'kurtosis', '75th_percentile'])

def create_average_stats(stats_df):
    data = []
    for period in stats_df['period'].unique():
        for feature in stats_df['feature'].unique():
            df = stats_df[(stats_df['period'] == period) & (stats_df['feature'] == feature)]
            data.append(("period",period, feature, df['mean'].mean(), df['mean'].std(),df['mean'].quantile(.75), df['STD'].mean(), df['75th_percentile'].mean(), df['75th_percentile'].std()))
    for increase in stats_df['increase'].unique():
        for feature in stats_df['feature'].unique():
            df = stats_df[(stats_df['increase'] == increase)  & (stats_df['feature'] == feature)]
            data.append(("increase",increase, feature, df['mean'].mean(), df['mean'].std(),df['mean'].quantile(.75), df['STD'].mean(), df['75th_percentile'].mean(), df['75th_percentile'].std()))
    return pd.DataFrame(data=data, columns=['category','magnitude','feature','means_av', 'means_std', 'means_75th_pct', 'std_mean', '75th_pct_mean', '75th_pct_std'])

def get_coin_price_data(dataframe = None,cache_results=True, check_cache_first=False,data_frame_key="df", **kwargs):
    pct_increases, time_windows, pct_study_periods, min_days = (0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 25), (30, 60, 90, 180),(30,60,90,180),10
    df = None
    measures = ('derivative_token', 'market_cap_usd', 'start_date'
                , '24h_volume_usd', 'start_date_utc','total_supply')
    r = redis.Redis(
        host='127.0.0.1',
        port=6379)

    if "pct_increases" in kwargs: pct_increases = kwargs["pct_increases"]
    if "time_windows" in kwargs: time_windows = kwargs["time_windows"]
    if "pct_study_periods" in kwargs: pct_study_periods = kwargs["pct_study_periods"]
    if "min_days" in kwargs: min_days = kwargs["min_days"]
    if "measures" in kwargs: measures = kwargs["measures"]

    if check_cache_first == True:
        try:
            if all([r.exists(key) for key in ("stats_df","summary_df","freq_df")]):
                return pd.read_msgpack(r.get("freq_df")), pd.read_msgpack(r.get("stats_df")), pd.read_msgpack(r.get("summary_df"))
            elif df == None:
                df = pd.read_msgpack(r.get(data_frame_key))
        except Exception as e:
            logging.info("Dataframe retrieval from redis cache failed with error message: %s, computing from database from disk", e)

    if not isinstance(dataframe, pd.DataFrame) and not isinstance(df, pd.DataFrame):
        df, df_ix, intervals = create_measures(days_from_present=31
                                                   , custom_measures= ("7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                    "ewma", "ewma_std")
                                                   , custom_intervals=time_windows
                                                   , pct_increases=pct_increases
                                                   , pct_study_periods=pct_study_periods
                                                   , measure_percent_increases=True
                                                   , add_coin_attributes=True, alpha=0.8)
    elif isinstance(dataframe, pd.DataFrame):
        df = dataframe
    if cache_results == True:
        r.set("df", df.to_msgpack(compress='zlib'))
    freq_df = create_frequency_df(df,pct_increases,pct_study_periods, measures,min_days)
    stats_df = create_stats_df(df, measures, pct_increases, pct_study_periods, min_period=min_days)
    summary_df = create_average_stats(stats_df)
    if cache_results == True:
        r.set("freq_df", freq_df.to_msgpack(compress='zlib'))
        r.set("stats_df", stats_df.to_msgpack(compress='zlib'))
        r.set("summary_df", summary_df.to_msgpack(compress='zlib'))
        r.set("pct_increases", pct_increases)
        r.set("time_windows", time_windows)
        r.set("pct_study_periods", pct_study_periods)
        r.set("min_days", min_days)

    return freq_df, stats_df, summary_df
