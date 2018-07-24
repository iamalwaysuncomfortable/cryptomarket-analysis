import db_services.readwrite_utils as rw
import res.db_data.sql_statement_library as psqll
import pandas as pd
import numpy as np
import log_service.logger_factory as lf
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
                "days_hiclose", "days_loclose"]
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
np.where((df['ewma_std'] >= 60) & (np.abs(df['price_usd']/df['7d_med_usd']) < 3) , df['7d_med_usd'], df['price_usd'])
def create_measures(band_multiple=1):
    for i in range(1, len(df_ix)):
        logging.info("Index being measured is %s", df_ix[i])
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_std"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.12, min_periods=3).std()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_btc"] = df["price_btc"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_usd"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_eur"] = df["price_eur"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "7d_med_cny"] = df["price_cny"][df_ix[i - 1]:df_ix[i]].rolling(7).median()
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], 'price_btc'][df['ewma_std'][df_ix[i - 1]:df_ix[i]] > 100] = df['7d_med_btc'][df_ix[i - 1]:df_ix[i]][df['ewma_std'] > 100]
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], 'price_usd'][df['ewma_std'][df_ix[i - 1]:df_ix[i]] > 100] = df[df_ix[i - 1]:df_ix[i]][df['ewma_std'] > 100]['7d_med_usd']
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], 'price_eur'][df['ewma_std'][df_ix[i - 1]:df_ix[i]] > 100] = df[df_ix[i - 1]:df_ix[i]][df['ewma_std'] > 100]['7d_med_eur']
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], 'price_cny'][df['ewma_std'][df_ix[i - 1]:df_ix[i]] > 100] = df[df_ix[i - 1]:df_ix[i]][df['ewma_std'] > 100]['7d_med_cny']

        df.loc[df.index[df_ix[i - 1]:df_ix[i]], ['price_btc', 'price_usd', 'price_eur', 'price_cny']][df['ewma_std'][df_ix[i - 1]:df_ix[i]] > 100] = df[['7d_med_btc', '7d_med_usd', '7d_med_eur', '7d_med_cny']][df['ewma_std'] > 100][df_ix[i - 1]:df_ix[i]]
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].ewm(alpha=0.95, min_periods=3).mean()

        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_top"] = df["ewma"][df_ix[i - 1]:df_ix[i]] + df["ewm_std"][df_ix[i - 1]:df_ix[i]]*band_multiple
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_low"] = df["ewma"][df_ix[i - 1]:df_ix[i]] - df["ewm_std"][df_ix[i - 1]:df_ix[i]]*band_multiple
        int_len = df_ix[i] - df_ix[i-1]
        for iv in intervals:
            if iv > int_len:
                break
            df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewm_std"] = df["price_usd"][df_ix[i - 1]:df_ix[i]].rolling(iv).stddev()
	    df.loc[df.index[0:252], "price_usd"] = np.where((df['ewma_std'][0:252] >= 60) & (np.abs(df['price_usd'][0:252]/df['7d_med_usd'][0:252]) > 3) , df['7d_med_usd'][0:252], df['price_usd'][0:252])
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

