import analysis.pre_processing.price_clusters_pp as pcp
import analysis.ml_methods.clustering as cl
import numpy as np
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()



df, df_ix, intervals = pcp.create_measures()

q = pcp.prepare_cluster_data(df, df_ix, ("ewma", "ewma", "return_variability"),
                             ("max", "(df[feature][(df_ix[i] - ts):df_ix[i]].max() - df[feature][(df_ix[i] - ts)])/"
                                     "df[feature][(df_ix[i] - ts)]", "mean"),
                             names=(None, "ewma_max_gain", None),
                             static_feature_list=("24h_volume_usd","total_supply"),static_feature_types=(float, float),
                             ts_=30, coin_data=pcp.coin_data,skip_nan=True)

X = [[q['ewma_max_gain'][i],q['ewma_max'][i],q['24h_volume_usd'][i], q['total_supply'][i]] for i in range(len(q['names']))]

#X = [[q['ewma_max'][i],q['ewma_max_gain'][i],q['24h_volume_usd'][i], q['total_supply'][i]] for i in range(len(q['names']))]
#X = np.array(X)
mgr = [0,0.3,0.6,1,3,5.5,10,20,40,80,150,300]
labels = range(len(mgr))
for x in X:
    for i in range(1, len(mgr) + 1):
        if i < len(mgr) and (x[0] >= mgr[i-i] and x[0] < mgr[i]):
            x.append(labels[i-1])
            break
        if i == len(mgr) and x[0] > mgr[i-1]:
            x.append(labels[i-1])

X = np.array(X)

cl.cluster_histograms(X, np.unique(X[:,4]),X[:,4],("ewma_max", "ewma_max_gain", "24h_volume_usd","total_supply", 'return_variability_mean'), coin_chars=q,log10_scales=(True, False, True, True, False))

