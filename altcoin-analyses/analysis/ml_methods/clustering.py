import numpy as np
import analysis.pre_processing.price_clusters_pp as pcp
from sklearn.cluster import AffinityPropagation, MeanShift, DBSCAN, estimate_bandwidth, AgglomerativeClustering
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

for item in rw.get_data_from_one_table("SELECT * FROM pricedata.static_token_data"):
    try:
        coin_data[item[0]]['start_date_utc'] = item[3]
        coin_data[item[0]]['derivative_token'] = item[15]
        coin_data[item[0]]['parent_blockchain'] = item[16]
    except:
        logging.info("Coin %s does not exist in current coin_list", item[1])

def Cluster(data, algorithm, plot_results=False, **kwargs):
    X = data
    if isinstance(algorithm, (str, unicode)):
        algorithm = algorithm.lower()
    else:
        raise ValueError("Algorithm specified must be in string or unicode format")

    if algorithm == "meanshift":
        bandwidth = 10
        if "bandwidth" in kwargs: bandwidth = kwargs["bandwidth"]
        alg = MeanShift(bandwidth=bandwidth)
        alg.fit(X)
        if plot_results == True:
            plt.ion()
            cluster_centers, labels, n_clusters_ = alg.cluster_centers_, alg.labels_, len(np.unique(alg.labels_))
            for k, col in zip(range(n_clusters_), colors):
                my_members = labels == k
                cluster_center = cluster_centers[k]
                plt.plot(X[my_members, 0], X[my_members, 1], col + ".")
                plt.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
                         markeredgecolor='k', markersize=4)
    elif algorithm == "dbscan":
        eps, min_samples = 50, 5
        if "eps" in kwargs: eps = kwargs["eps"]
        if min_samples in kwargs: min_samples = kwargs["min_samples"]
        alg = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
        if plot_results == True:
            core_samples_mask = np.zeros_like(alg.labels_, dtype=bool)
#            core_samples_mask[alg.core_sample_indices_]
            labels = alg.labels_
            n_clusters_, unique_labels = len(set(labels)) - (1 if -1 in labels else 0), set(labels)
            plt.close('all')
            plt.figure(1)
            plt.clf()
            plt.ion()
            for k, col in zip(unique_labels, colors):
                if k == -1:
                    # Black used for noise.
                    col = [0, 0, 0, 1]
                class_member_mask = (labels == k)
                xy = X[class_member_mask & core_samples_mask]
                plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col), markeredgecolor='k', markersize=14)
                xy = X[class_member_mask & ~core_samples_mask]
                plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col), markeredgecolor='k', markersize=6)
    elif algorithm == "agglomerativeclustering" or algorithm == "agglomerative":
        n_clusters, linkage = 30, 'ward'
        if "n_clusters" in kwargs: n_clusters = kwargs['n_clusters']
        if "linkage" in kwargs: linkage = kwargs['linkage']
        alg = AgglomerativeClustering(n_clusters=n_clusters, linkage=linkage).fit(X)
        if plot_results == True:
            label = alg.labels_
            plt.close('all')
            plt.figure(1)
            plt.clf()
            plt.ion()
            for l in np.unique(label):
                if len(X[label == l]) > 3:
                    print(l)
                    plt.scatter(X[label == l, 1], X[label == l, 0], color=plt.cm.jet(np.float(l) / np.max(label + 1)),
                                s=20,
                                edgecolor='k')
    elif algorithm == "affinitypropogation":
        preference = -50
        if "preference" in kwargs: preference = kwargs["preference"]
        alg = AffinityPropagation(preference=preference).fit(X)
        if plot_results == True:
            cluster_centers_indices = alg.cluster_centers_indices_
            labels = alg.labels_
            n_clusters_ = len(cluster_centers_indices)
            plt.close('all')
            plt.figure(1)
            plt.clf()
            plt.ion()
            for k, col in zip(range(n_clusters_), colors):
                class_members = labels == k
                cluster_center = X[cluster_centers_indices[k]]
                plt.plot(X[class_members, 0], X[class_members, 1], col + '.')
                plt.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
                         markeredgecolor='k', markersize=14)
                for x in X[class_members]:
                    plt.plot([cluster_center[0], x[0]], [cluster_center[1], x[1]], col)
    else:
        raise ValueError("No Valid Clustering Algorithm Specified")

    if plot_results == True:
        plt.draw()
    return alg

def cluster_histograms(data, clusters, labels, target_chars, coin_chars=coin_data, log10_scales=None, min_cluster_size=4):

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

def plot_clusters(xpoints, ypoints, zpoints = None, names = None):
    if zpoints == None:
        f, diagram = plt.subplots(1)
        for i in range(len(xpoints)):
            diagram.plot(xpoints[i], ypoints[i], 'ro')
            if isinstance(names, (list, tuple)) and len(names) == len(xpoints):
                diagram.annotate(names[i], (xpoints[i], ypoints[i]), fontsize=12)
    else:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(xpoints, ypoints, zpoints)
    plt.show()