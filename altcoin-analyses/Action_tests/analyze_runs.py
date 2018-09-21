import analysis.pre_processing.price_clusters_pp_fork as pcp
import custom_utils.datautils as du
import custom_utils.type_validation as tv
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from res.env_config import set_master_config
set_master_config()
from log_service.logger_factory import get_loggly_logger, launch_logging_service
logging = get_loggly_logger(__name__, level="INFO")
launch_logging_service()


increase_percentages = [0.1,0.2,0.3,0.5,0.75,1,2,3,5,10,25,50]
_periods=[30, 90, 180]
measures = ("7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                    "ewma", "ewma_std")

df, df_ix, intervals = pcp.create_measures(days_from_present=91
                                           , custom_measures=measures
                                           , custom_intervals=(3, 7, 14, 30, 60, 90)
                                           , pct_increases=increase_percentages
                                           , pct_study_periods=_periods
                                           , measure_percent_increases=True
                                           , add_coin_attributes=True)

df["derivative_token"] = df["derivative_token"].astype("float64")
df["fully_premined"] = df["fully_premined"].astype("float64")
#increase_periods = ('90_open_300_pct_increase', '30_open_1000_pct_increase')
target_attributes = ("total_supply", "start_date_utc","uts","24h_volume_usd","derivative_token", "market_cap_usd")
log_scale_columns = ("total_supply")

pct_increases, time_windows = (300,500,1000), (30,90)

def create_periods(pct_increases, time_windows):
    column_selections = []
    for window in time_windows:
        for increase in pct_increases:
            column_selections.append(str(window) + "_open_"+str(increase)+"_pct_increase")
    return tuple(column_selections)

increase_periods = create_periods(pct_increases,time_windows)

def plot_price_increase_histograms(increase_periods,target_attributes, log_scale_columns=None, min_period=10, bins=30):
    columns, rows = len(target_attributes) + 1, len(increase_periods)
    i = 1
    fig = plt.figure()
    for period in increase_periods:
        sampling_df_columns = tuple(target_attributes) + (period,)
        sampling_df = df[df[period] > min_period].loc[:,("coin_symbol",) + sampling_df_columns]
        for c in sampling_df_columns:
            ax = fig.add_subplot(rows, columns, i)
            series = sampling_df[c][~pd.isnull(sampling_df[c])]
            if ("utc" in c) or ("uts" in c):
                series = series[series > 0]
                series = mdates.epoch2num(series)
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m.%y'))
            if c in log_scale_columns:
                series = series[series > 0]
                series = np.log10(series)
                c = c + " (Log10)"
            if i <= columns:
                if "pct_increase" in c:
                    ax.set_title("Run Length")
                else:
                    ax.set_title(c)
            if i%len(sampling_df_columns) == 1:
                ax.set_ylabel(period[:-13], rotation=0,size="small")
                ax.yaxis.set_label_coords(-.3,0.5)
            ax.hist(series, bins=bins)
            i += 1
    plt.show()

def create_stats(target_attributes, pct_increases,time_windows, min_period=10):
    data = []
    if isinstance(pct_increases, (tuple, list)) and (time_windows, (tuple,list)):
        for window in time_windows:
            for increase in pct_increases:
                period = str(window) + "_open_"+str(increase)+"_pct_increase"
                sampling_df_columns = tuple(target_attributes)
                sampling_df = df[df[period] > min_period].loc[:,("coin_symbol",) + sampling_df_columns]
                for c in sampling_df_columns:
                    series = sampling_df[c]
                    if ("utc" in c) or ("uts" in c):
                        series = series[series > 0]
                    data.append((period, window, increase, c, series.mean(), series.mad(),series.std(), series.kurt(), series.quantile(.75)))
    else:
        raise ValueError("Valid periods and time windows must be specified")
    return pd.DataFrame(data=data,columns=['period_name','period','increase', 'feature', 'mean', 'MAE', 'STD', 'kurtosis', '75th_percentile'])

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


plot_price_increase_histograms(increase_periods, target_attributes, log_scale_columns)
stats_df = create_stats(target_attributes, pct_increases, time_windows)
sum_df = create_average_stats(stats_df)



#Useful snippets
df[df['90_open_300_pct_increase'] > 1].loc[:,('coin_symbol', 'coin_name','utcdate','90_open_300_pct_increase')]
df[df['90_open_300_pct_increase'] > 1].loc[:,('coin_symbol','utcdate','90_open_300_pct_increase')]
#df[df['coin_name'] == cn].loc[:,('coin_name','utcdate','ewma_90_day_pct_change_from_20180602')]
#df[df['coin_name'] == 'ReeCoin'].loc[:,('coin_name','utcdate','ewma_90_day_pct_change_from_20180608')]
#i = 620
#pct = 25
#a = df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])].groupby((df['ewma_90_day_pct_change_from_20180602'] > pct).cumsum()).cumcount(ascending=False)
#(df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])]['ewma_90_day_pct_change_from_20180602'] > pct).cumsum()