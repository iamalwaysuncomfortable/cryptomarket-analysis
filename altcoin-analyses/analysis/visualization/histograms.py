import numpy as np
#import analysis.pre_processing.price_clusters_pp as pcp
import analysis.pre_processing.price_clusters_pp_fork as pcp
from custom_utils.type_validation import validate_type
from matplotlib import colors as mcolors
import db_services.readwrite_utils as rw
import matplotlib.pyplot as plt
from itertools import cycle
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()
#colors = mcolors.XKCD_COLORS.values()
colors = cycle('bgrcmykbgrcmykbgrcmykbgrcmyk')
coin_data = pcp.coin_data.copy()

def create_histograms_on_filtered_condition(series):
    pass


def create_histograms(data, clusters, labels, target_chars, coin_chars=coin_data, log10_scales=None, min_cluster_size=4):

    '''
    Create aggregate histograms from existing clusters

    :param clusters(list, np.ndarray): Array of clusters
    :param coin_chars (dict): Dictionary containing characteristics of interest in the coin
    :param target_chars (str, unicode): The characteristics to target, should be a list of strings
    :param log10_scales (bool): Whether or not to log scale the particular parameter
    :return: None
    '''

    validate_type(target_chars, (list, tuple))
    if log10_scales is not None:
        validate_type(log10_scales, (list, tuple))
        if len(target_chars) != len(log10_scales):
            raise ValueError("If log scales are specified, 1 boolean value indicating whether or not a log "
                             "should be taken must be specified per specified target characteristic")
        for target in target_chars: validate_type(target, (str, unicode))
        for scale in log10_scales: validate_type(scale, bool)

    X = data
    cluster_info = {}
    if isinstance(X, (list, tuple)):
        X = np.array(X)

    columns = len(target_chars)
    rows = 0
    fig = plt.figure()

    for cluster in clusters:
        if len(X[labels == cluster]) > min_cluster_size:
            rows += 1
            cluster_info[cluster] = {target:[] for target in target_chars}

    selected_clusters = cluster_info.keys()

    for x in range(len(X)):
        if labels[x] in selected_clusters:
            for target in target_chars:
                cluster_info[labels[x]][target].append(coin_chars[target][x])

    i = 1
    for cluster in cluster_info.keys():
        c = 0
        for target in target_chars:
            ax = fig.add_subplot(rows, columns, i)
            if i <= columns:
                if log10_scales != None and log10_scales[c] == True:
                    ax.set_title(target + "(Log Scale)")
                else:
                    ax.set_title(target)
            data = cluster_info[cluster][target]
            if log10_scales == None or log10_scales[c] == False:
                print(cluster, target)
                ax.hist(data)
            elif log10_scales[c] == True:
                data = np.log10(np.float64(data))
                data = data[~np.isnan(data) & ~np.isinf(data)]
                ax.hist(data)
            i += 1
            c += 1
    plt.show()

def plot_data(df, df_ix, plot_type, i, d=7, x_axis="uts"):
    randval = mcolors.XKCD_COLORS.values()
    randlen = len(randval) - 1
    np.random.seed(0)
    if plot_type == "ewma_vs_price":
        plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df["ewma"][df_ix[i - 1]:df_ix[i]], "--r", df[x_axis][df_ix[i - 1]:df_ix[i]], df["price_usd"][df_ix[i - 1]:df_ix[i]], "--b",df[x_axis][df_ix[i - 1]:df_ix[i]], df["ewma_top"][df_ix[i - 1]:df_ix[i]], "--g", df[x_axis][df_ix[i - 1]:df_ix[i]], df["ewma_low"][df_ix[i - 1]:df_ix[i]], "--g")
        plt.show()
    elif plot_type == "ewma_std":
        plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df["ewma_std"][df_ix[i - 1]:df_ix[i]], "--r")
        plt.show()
    elif plot_type == "ewma_mult":
        plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df["ewma_mult"][df_ix[i - 1]:df_ix[i]], "--r")
        plt.show()
    elif plot_type == "daily_volatility":
        plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df[str(d) + "d_daily_volatility"][df_ix[i - 1]:df_ix[i]], "--r",
                 df[x_axis][df_ix[i - 1]:df_ix[i]], df[str(d) + "d_mean_volatility"][df_ix[i - 1]:df_ix[i]], "--b")
        plt.show()
    elif plot_type == "volatility_ratio":
        plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df[str(d) + "d_volatility_ratio"][df_ix[i - 1]:df_ix[i]], "--r")
        plt.show()
    elif plot_type == "return_variability":
        plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df["return_variability"][df_ix[i - 1]:df_ix[i]], linestyle="-", color="Red")
        plt.show()
    else:
        if isinstance(plot_type, (str, unicode)):
            plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df[plot_type][df_ix[i - 1]:df_ix[i]], linestyle="-", color="Red")
            plt.show()
        elif isinstance(plot_type, (list, tuple, set)):
            for pt in plot_type:
                if isinstance(pt, (str, unicode)):
                    plt.plot(df[x_axis][df_ix[i - 1]:df_ix[i]], df[plot_type][df_ix[i - 1]:df_ix[i]], linestyle="-",
                             color=randval[np.random.randint(randval)])
                    plt.show()
        else:
            logging.warn("No valid input for plot_type was specified")
