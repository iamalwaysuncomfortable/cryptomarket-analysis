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
lf.launch_logging_service()
cmc_data = cmc.get_coin_list()

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

print("module started")
coin_data = populate_coin_attributes()
default_functions = {"max": "df[feature][(df_ix[i] - ts):df_ix[i]].max()",
                 "min": "df[feature][(df_ix[i] - ts):df_ix[i]].min()",
                 "mean": "df[feature][(df_ix[i] - ts):df_ix[i]].mean()",
                 "median": "df[feature][(df_ix[i] - ts):df_ix[i]].median()",
                 "std": "df[feature][(df_ix[i] - ts):df_ix[i]].std()",
                 "var": "df[feature][(df_ix[i] - ts):df_ix[i]].var()",
                 "kurtosis": "df[feature][(df_ix[i] - ts):df_ix[i]].kurtosis"}
default_function_list = default_functions.keys()

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

    reads = psqll.crypto_price_data_statements(read=True)
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
                    pct_study_periods=None, add_coin_attributes=False):
    df, df_ix, intervals = create_default_df(custom_measures=custom_measures,
                                             custom_intervals=custom_intervals,
                                             days_from_present=days_from_present,
                                             custom_time_range=custom_time_range,
                                             date_format=date_format)
    if add_coin_attributes == True:
        attributes = ('derivative_token', 'market_cap_usd','start_date'
                      , '24h_volume_usd', 'start_date_utc', 'max_supply',
                      'total_supply',  'fully_premined', 'available_supply',
                      'amount_premined', 'parent_blockchain')
        coin_attributes = populate_coin_attributes()

    for i in range(1, len(df_ix)):
        logging.info("Index being measured is %s", df_ix[i])
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_std"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.12, min_periods=3).std()
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
        fwd = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.2, min_periods=1).mean()
        bwd = df["price_usd"][df_ix[i - 1]:df_ix[i]][::-1].ewm(alpha=0.2,min_periods=1).mean()
        c = np.vstack((fwd, bwd[::-1]))
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma"] = np.mean(c, axis=0 )
        fwd = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.2, min_periods=1).std()
        bwd = df["price_usd"][df_ix[i - 1]:df_ix[i]][::-1].ewm(alpha=0.2,min_periods=1).std()
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
        if pct_increases == None:
            increase_percentages = [0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 3, 5, 10, 25, 50]
        elif isinstance(pct_increases, (list,tuple)) and tv.validate_list_element_types(pct_increases, (int, float)):
            increase_percentages = pct_increases
        else:
            logging.warn("Invalid percentage increases specified, returning results without measuring pct increases")
            return df, df_ix, intervals

        if pct_study_periods == None:
            _periods = [30, 90, 180]
        elif isinstance(pct_study_periods, (list, tuple)) and tv.validate_list_element_types(pct_increases, (int, float)):
            _periods = pct_study_periods
        else:
            logging.warn("Invalid period of study specified, returning results without measuring pct increases")
            return df, df_ix, intervals

        examination_periods = []
        modifier = 0
        for p in _periods:
            end = 0
            if isinstance(modifier, (int, float)):
                p += modifier
                end += modifier
            examination_periods.append((str(p) + "_day_period", du.get_epoch(days=p, start_of_day=True),
                                        du.get_epoch(days=end, start_of_day=True)))

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
                column_name = str(int((p[2] - p[1]) / 86400)) + "_day_pct_change_from_" + du.convert_epoch(p[1],
                                                                                                           e2str=True,
                                                                                                           custom_format="%Y%m%d")
                df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_" + column_name] = \
                    df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1])]["price_usd"] / period_opening_price - 1
                df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_" + column_name] = \
                    df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1])]["ewma"] / period_opening_price - 1
                for pct in increase_percentages:
                    first_pct = \
                    df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])]['ewma_' + column_name].iloc[0]
                    a = df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])].groupby(
                        (df['ewma_' + column_name] < pct).cumsum()).cumcount(ascending=False)
                    b = (a.shift(1) < a) & (a.shift(-1) < a)
                    if first_pct > pct:
                        b.iloc[0] = True
                    pct_column_name =str(int((p[2] - p[1]) / 86400)) + "_open_" \
                                     + str(pct * 100) + "_pct_increase"
                    df.loc[df.index[df_ix[i - 1]:df_ix[i]], pct_column_name] = 0
                    for item in a[b].iteritems():
                        df[pct_column_name].iloc[item[0]] = item[1]
    return df, df_ix, intervals

# for i in range(1, len(df_ix)):
#     logging.info("Index being measured is %s", df_ix[i])
#     int_len = df_ix[i] - df_ix[i - 1]
#     for iv in intervals:
#         if iv > int_len:
#             break
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "open"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(i
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "close"] = df["price_usd"][df_ix[i - 1]:df_ix[i]]
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "low"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "high"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_openclose"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]] - df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)) /\
#             df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_openhi"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() -
#              df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)) / df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)df
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_openlo"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() -
#              df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv))/df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_hilo"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() - df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()) /\
#             df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_hiclose"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]] - df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()) /\
#             df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_loclose"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["price_usd"][df_ix[i - 1]:df_ix[i]]) /\
#             df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_loclose"] = \
#             (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["price_usd"][df_ix[i - 1]:df_ix[i]]) /\
#             df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "idx_high"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["price_usd"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmax()))
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "idx_low"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["price_usd"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmin()))
#         hi_time = df.loc[df[str(iv) + "d_" + "idx_high"][df_ix[i - 1] + iv:df_ix[i]]]
#         hi_time.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
#         lo_time = df.loc[df[str(iv) + "d_" + "idx_low"][df_ix[i - 1] + iv:df_ix[i]]]
#         lo_time.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
#         df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "time_hi"] = hi_time['uts']
#         df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "time_lo"] = lo_time['uts']
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openhi"] = (df[str(iv) + "d_" + "time_hi"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openlo"] = (df[str(iv) + "d_" + "time_lo"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_hiclose"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "time_hi"])/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_loclose"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "time_lo"])/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_open"] = df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_close"] = df["ewma"][df_ix[i - 1]:df_ix[i]]
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_low"] = df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_high"] = df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "emwa_pct_openclose"] = np.where(
#             df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv) > 0,
#             (df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)) /\
#             df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv),np.NaN)
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_openhi"] = \
#             np.where(df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv) > 0, df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() -
#              df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)) / df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_openlo"] = \
#             (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() -
#              df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv))/df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_hilo"] = \
#             (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() - df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()) /\
#             df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_hiclose"] = \
#             (df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()) /\
#             df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_loclose"] = \
#             (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["ewma"][df_ix[i - 1]:df_ix[i]]) /\
#             df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_loclose"] = \
#             (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["ewma"][df_ix[i - 1]:df_ix[i]]) /\
#             df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_idx_high"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["ewma"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmax()))
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_idx_low"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["ewma"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmin()))
#         hi_time_ewma = df.loc[df[str(iv) + "d_" + "ewma_idx_high"][df_ix[i - 1] + iv:df_ix[i]]]
#         hi_time_ewma.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
#         lo_time_ewma = df.loc[df[str(iv) + "d_" + "ewma_idx_low"][df_ix[i - 1] + iv:df_ix[i]]]
#         lo_time_ewma.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
#         df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "ewma_time_hi"] = hi_time_ewma['uts']
#         df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "ewma_time_lo"] = lo_time_ewma['uts']
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openhi_ewma"] = (df[str(iv) + "d_" + "ewma_time_hi"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openlo_ewma"] = (df[str(iv) + "d_" + "ewma_time_lo"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_hiclose_ewma"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "ewma_time_hi"])/86400
#         df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_loclose_ewma"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "ewma_time_lo"])/86400

def prepare_general_clusters(df, df_ix, ts, x_axis,y_axis, z_axis=None, x_func=None, y_func=None, min_volume = None, min_mcap = None):
    xpoints = []
    ypoints = []
    zpoints = []
    names = []
    symbols = []
    for i in range(1, len(df_ix)):
        if (df_ix[i] - df_ix[i-1]) >= ts:
            symbol = df["coin_symbol"][df_ix[i] - ts / 2]
            if isinstance(min_volume, (int, float)):
                try:
                    volume = float(coin_data[symbol]['24h_volume_usd'])
                except:
                    continue
                if volume == None or min_volume > float(volume): continue
                else: zpoints.append(volume)
            if isinstance(min_mcap, (int, float)):
                mcap = coin_data[symbol]['market_cap_usd']
                if mcap == None or min_mcap > float(mcap): continue
            if df[x_axis][(df_ix[i] - ts)] > 0:
                y_metric = (df[x_axis][(df_ix[i] - ts):df_ix[i]].max() - df[x_axis][(df_ix[i] - ts)])/ \
                           df[x_axis][(df_ix[i] - ts)]
                x_metric = df[y_axis][(df_ix[i] - ts):df_ix[i]].max()
                xpoints.append(x_metric)
                ypoints.append(y_metric)
                names.append(df["coin_name"][df_ix[i] - ts / 2])
                symbols.append(symbol)
            else:
                continue
    return xpoints, ypoints, zpoints, names, symbols

def prepare_cluster_data(df, df_ix, feature_list, function_list, static_feature_list=None,static_feature_types=None,
                              names = None, ts_=None, constraints=None, coin_data=coin_data, skip_nan=False):
    ts = ts_
    if len(feature_list) != len(function_list): raise ValueError("List of feature and functions must be equal")
    if not (isinstance(static_feature_list, (tuple, list)) and
            all(isinstance(sf, (str, unicode)) for sf in static_feature_list) or static_feature_list == None):
        raise ValueError("List of static features must be in string format or NoneType")

    if static_feature_list != None: point_container = {sf:[] for sf in static_feature_list}
    else: point_container = {}

    #####Initialize cotainer for cluster data#####
    j, c = 0, 0
    for feature in feature_list:
        if function_list[j] in default_function_list:
            if isinstance(names, (tuple, list)) and isinstance(names[j], (str, unicode)):
                point_container[names[j]] = []
            else:
                point_container[feature + "_" + function_list[j]] = []
        else:
            if isinstance(names, (tuple, list)) and isinstance(names[j], (str, unicode)):
                point_container[names[j]] = []
            else:
                point_container["custom_" + feature + "_" + str(c)] = []
            c += 1
        j += 1
    del j
    del c

    point_container["names"] = []
    point_container["symbols"] = []
    def collect_features(feature_list, function_list, static_feature_list, static_feature_types,
                         point_container, symbol, ts, names):
        include = True
        j, c, results = 0, 0, []

        if static_feature_list != None:
            for sf in static_feature_list:
                f = 0
                try:
                    if static_feature_types != None and len(static_feature_types) == len(static_feature_list):
                        result = static_feature_types[f](coin_data[symbol][sf])
                    else:
                        result = coin_data[symbol][sf]
                except Exception as e:
                    logging.warn("Feature %s not found for symbol %s", sf, symbol)
                    result = np.nan
                    if skip_nan == True:
                        include = False
                        break
                f += 1
                results.append((sf, result))

        for feature in feature_list:
            if function_list[j] in default_function_list:
                result = eval(default_functions[function_list[j]])
                if skip_nan == True and (np.isnan(result) or np.isinf(result) or result == None):
                    logging.warn("result given for function %s for feature %s gave result of %s skipping coin %s",
                                 function_list[j], feature, result, symbol)
                    include = False
                    break
                if isinstance(names, (tuple, list)) and isinstance(names[j], (str, unicode)):
                    results.append((names[j], result))
                else:
                    results.append((feature + "_" + function_list[j], result))
            else:
                try:
                    result = eval(function_list[j])
                except Exception as e:
                    result = np.nan
                    logging.warn("function %s evaluation failed with error %s for symbol %s", function_list[j],
                                 e, symbol)
                if isinstance(names, (tuple, list)) and isinstance(names[j], (str, unicode)):
                    results.append((names[j], result))
                else:
                    results.append(("custom_" + feature + "_" + str(c), result))
                if skip_nan == True and (np.isnan(result) or np.isinf(result) or result == None):
                    include = False
                    break
                c += 1
            j += 1

        if include == True:
            #####Write results if skip condition not reached#####
            for result in results:
                point_container[result[0]].append(result[1])
            point_container["symbols"].append(symbol)
            point_container["names"].append(df["coin_name"][df_ix[i]-1])
        print results

    for i in range(1, len(df_ix)):
        if ts == None: ts = df_ix[i] - df_ix[i-1]
        if ts > 0 and (df_ix[i] - df_ix[i-1]) >= ts:

            symbol = df["coin_symbol"][df_ix[i] - ts / 2]
            if isinstance(constraints, (list, tuple, set)) and all(len(constraint) == 3 for constraint in constraints):
                include = True
                for c in constraints:
                    datapoint = c[0]
                    operation = c[1]
                    operation = operation.lower()
                    constraint = c[2]
                    try:
                        if isinstance(constraint, (int, float, np.float, np.float16, np.float32, np.float64, np.float80,
                                                   np.float96, np.float128, np.float256)):
                            measure = float(coin_data[symbol][datapoint])
                            if operation == ">" and measure <= constraint: include = False
                            if operation == ">=" and measure < constraint: include = False
                            if operation == "<" and measure >= constraint: include = False
                            if operation == "<=" and measure > constraint: include = False
                            if operation == "==" and measure != constraint: include = False
                        else:
                            measure = coin_data[symbol][datapoint]
                            if operation != "==": raise ValueError("If constraint is non float, only equalities are allowed")
                            if measure != constraint: include = False
                    except Exception as e:
                        #logging.warn("Exception caught in constraint measurement, text was %s", e)
                        include = False
            elif constraints == None:
                include = True
            else:
                #logging.warn("starting collection for symbol %s", symbol)
                include = False

            if include == True:
                collect_features(feature_list, function_list, static_feature_list, static_feature_types,
                                 point_container, symbol, ts, names)

    return point_container
