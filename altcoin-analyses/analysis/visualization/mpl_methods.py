import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

def simple_3d_scatter(x, y, z, draw=True):
    fig = plt.figure()
    ax = Axes3D(fig)
    ax.scatter(x, y, z)
    if draw == True:
        plt.show()

def simple_2d_scatter(x,y, draw=True):
    plt.scatter(x, y)
    if draw==True:
        plt.show()

def plot_price_increase_histograms(df, increase_periods,target_attributes, log_scale_columns=None, min_period=10, bins=30, draw=True):
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
    if draw==True:
        plt.show()