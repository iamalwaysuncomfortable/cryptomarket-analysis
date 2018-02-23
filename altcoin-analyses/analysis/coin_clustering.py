import db_services.readwrite_utils as rw
import res.db_data.sql_statement_library as psqll
import pandas as pd
import numpy as np
import log_service.logger_factory as lf
import pylab as plt
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

def create_default_df():
    reads = psqll.crypto_price_data_statements(read=True)
    query, col_query = reads["price_history_get_all"], reads["price_columns"]
    columns = [x[0] for x in rw.get_data_from_one_table(col_query)]
    measures = ["7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                "ewma","ewma_std","open", "close", "high", "low",
                "pct_openclose", "pct_openhi","pct_openlo","pct_hilo",
                "pct_hiclose", "pct_loclose", "pct_lifetime", "idx_high",
                "idx_low","time_hi","time_lo", "days_openhi", "days_openlow",
                "days_hiclose", "days_loclose","ewma_open", "ewma_close", "ewma_high", "ewma_low",
                "ewma_pct_openclose", "ewma_pct_openhi","ewma_pct_openlo","ewma_pct_hilo",
                "ewma_pct_hiclose", "ewma_pct_loclose", "ewma_pct_lifetime", "ewma_idx_high",
                "ewma_idx_low","ewma_time_hi","ewma_time_lo", "days_openhi_ewma", "days_openlow_ewma",
                "days_hiclose_ewma", "days_loclose_ewma" ]
    interval_independent_measures = ["ewma", "ewma_std","7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny"]
    intervals = [3, 7, 14, 30, 60, 90, 120, 360, 720, 1080]
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
    return df, ix, intervals, measures

df, df_ix, intervals, measures  = create_default_df()
#def create_measures(band_multiple=1):
band_multiple = 1
for i in range(1, len(df_ix)):
    logging.info("Index being measured is %s", df_ix[i])
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_std"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.12, min_periods=3).std()
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_btc"] = df["price_btc"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_usd"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_eur"] = df["price_eur"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_cny"] = df["price_cny"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_usd"] = np.where((df['ewma_std'][df_ix[i - 1]:df_ix[i]] >= 60) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 3), df['7d_med_usd'][df_ix[i - 1]:df_ix[i]], df['price_usd'][df_ix[i - 1]:df_ix[i]])
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_btc"] = np.where((df['ewma_std'][df_ix[i - 1]:df_ix[i]] >= 60) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 3), df['7d_med_btc'][df_ix[i - 1]:df_ix[i]], df['price_btc'][df_ix[i - 1]:df_ix[i]])
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_cny"] = np.where((df['ewma_std'][df_ix[i - 1]:df_ix[i]] >= 60) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 3), df['7d_med_cny'][df_ix[i - 1]:df_ix[i]], df['price_cny'][df_ix[i - 1]:df_ix[i]])
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_eur"] = np.where((df['ewma_std'][df_ix[i - 1]:df_ix[i]] >= 60) & (np.abs(df['price_usd'][df_ix[i - 1]:df_ix[i]] / df['7d_med_usd'][df_ix[i - 1]:df_ix[i]]) > 3), df['7d_med_eur'][df_ix[i - 1]:df_ix[i]], df['price_eur'][df_ix[i - 1]:df_ix[i]])
    fwd = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.12, min_periods=3).mean()
    bwd = df["price_usd"][df_ix[i - 1]:df_ix[i]][::-1].ewm(alpha=0.12,min_periods=3).mean()
    c = np.vstack((fwd, bwd[::-1]))
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma"] = np.mean( c, axis=0 )
    fwd = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.12, min_periods=3).std()
    bwd = df["price_usd"][df_ix[i - 1]:df_ix[i]][::-1].ewm(alpha=0.12,min_periods=3).std()
    c = np.vstack((fwd, bwd[::-1]))
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_std"] = np.mean( c, axis=0 )
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_top"] = df["ewma"][df_ix[i - 1]:df_ix[i]] + df["ewma_std"][df_ix[i - 1]:df_ix[i]]*band_multiple
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_low"] = df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewma_std"][df_ix[i - 1]:df_ix[i]]*band_multiple
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_mult"] = (df["ewma"][df_ix[i - 1]:df_ix[i]] + df["ewma_std"][df_ix[i - 1]:df_ix[i]])/df["ewma"][df_ix[i - 1]:df_ix[i]]
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "log_returns"] = np.log(df["price_usd"]) - np.log(df["price_usd"].shift(1))
    int_len = df_ix[i] - df_ix[i-1]
    for iv in intervals:
        if iv > int_len:
            break
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_daily_volatility"] = df["log_returns"][df_ix[i - 1]:df_ix[i]].rolling(iv).std()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_mean_volatility"] = df["log_returns"][ df_ix[i - 1]:df_ix[i]].rolling(iv).std()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "open"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "close"] = df["price_usd"][df_ix[i - 1]:df_ix[i]]
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "low"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "high"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_openclose"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]] - df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)) /\
            df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_openhi"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() -
             df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)) / df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_openlo"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() -
             df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv))/df["price_usd"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_hilo"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() - df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()) /\
            df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_hiclose"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]] - df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()) /\
            df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_loclose"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["price_usd"][df_ix[i - 1]:df_ix[i]]) /\
            df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "pct_loclose"] = \
            (df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["price_usd"][df_ix[i - 1]:df_ix[i]]) /\
            df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "idx_high"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["price_usd"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmax()))
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "idx_low"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["price_usd"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmin()))
        hi_time = df.loc[df[str(iv) + "d_" + "idx_high"][df_ix[i - 1] + iv:df_ix[i]]]
        hi_time.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
        lo_time = df.loc[df[str(iv) + "d_" + "idx_low"][df_ix[i - 1] + iv:df_ix[i]]]
        lo_time.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
        df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "time_hi"] = hi_time['uts']
        df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "time_lo"] = lo_time['uts']
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openhi"] = (df[str(iv) + "d_" + "time_hi"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openlo"] = (df[str(iv) + "d_" + "time_lo"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_hiclose"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "time_hi"])/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_loclose"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "time_lo"])/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_open"] = df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_close"] = df["ewma"][df_ix[i - 1]:df_ix[i]]
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_low"] = df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_high"] = df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "emwa_pct_openclose"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)) /\
            df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_openhi"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() -
             df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)) / df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_openlo"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() -
             df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv))/df["ewma"][df_ix[i - 1]:df_ix[i]].shift(iv)
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_hilo"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max() - df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()) /\
            df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_hiclose"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()) /\
            df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).max()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_loclose"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["ewma"][df_ix[i - 1]:df_ix[i]]) /\
            df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_pct_loclose"] = \
            (df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min() - df["ewma"][df_ix[i - 1]:df_ix[i]]) /\
            df["ewma"][df_ix[i - 1]:df_ix[i]].rolling(iv).min()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_idx_high"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["ewma"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmax()))
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "ewma_idx_low"] = (df["index"][df_ix[i - 1]:df_ix[i]].shift(iv) +pd.rolling_apply(df["ewma"][df_ix[i - 1]:df_ix[i]], iv, lambda x: pd.Series(x).idxmin()))
        hi_time_ewma = df.loc[df[str(iv) + "d_" + "ewma_idx_high"][df_ix[i - 1] + iv:df_ix[i]]]
        hi_time_ewma.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
        lo_time_ewma = df.loc[df[str(iv) + "d_" + "ewma_idx_low"][df_ix[i - 1] + iv:df_ix[i]]]
        lo_time_ewma.index = np.arange(df_ix[i - 1] + iv,df_ix[i])
        df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "ewma_time_hi"] = hi_time_ewma['uts']
        df.loc[df.index[df_ix[i - 1] + iv:df_ix[i]], str(iv) + "d_" + "ewma_time_lo"] = lo_time_ewma['uts']
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openhi_ewma"] = (df[str(iv) + "d_" + "ewma_time_hi"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_openlo_ewma"] = (df[str(iv) + "d_" + "ewma_time_lo"] - df["uts"][df_ix[i - 1]:df_ix[i]].shift(iv))/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_hiclose_ewma"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "ewma_time_hi"])/86400
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], str(iv) + "d_" + "days_loclose_ewma"] = (df["uts"][df_ix[i - 1]:df_ix[i]] - df[str(iv) + "d_" + "ewma_time_lo"])/86400


def plot_data(df, plot_type, i, d=7):
    if plot_type == "ewma_vs_price":
        plt.plot(df["utcdate"][df_ix[i - 1]:df_ix[i]], df["ewma"][df_ix[i - 1]:df_ix[i]], "--r", df["utcdate"][df_ix[i - 1]:df_ix[i]], df["price_usd"][df_ix[i - 1]:df_ix[i]], "--b",
                 df["utcdate"][df_ix[i - 1]:df_ix[i]], df["ewma_top"][df_ix[i - 1]:df_ix[i]], "--g", df["utcdate"][df_ix[i - 1]:df_ix[i]], df["ewma_low"][df_ix[i - 1]:df_ix[i]], "--g")
        plt.show()
    if plot_type == "ewma_std":
        plt.plot(df["utcdate"][df_ix[i - 1]:df_ix[i]], df["ewma_std"][df_ix[i - 1]:df_ix[i]], "--r")
        plt.show()
    if plot_type == "ewma_mult":
        plt.plot(df["utcdate"][df_ix[i - 1]:df_ix[i]], df["ewma_mult"][df_ix[i - 1]:df_ix[i]], "--r")
        plt.show()
    if plot_type == "daily_volatility":
        plt.plot(df["utcdate"][df_ix[i - 1]:df_ix[i]], df[str(d) + "d_daily_volatility"][df_ix[i - 1]:df_ix[i]], "--r")
        plt.show()

plot_data(df, "daily_volatility", 1)

