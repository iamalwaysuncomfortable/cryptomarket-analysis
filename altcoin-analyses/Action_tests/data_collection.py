import time
import analysis.pre_processing.price_clusters_pp as pcp
import custom_utils.datautils as du
from res.env_config import set_master_config
set_master_config()
from log_service.logger_factory import get_loggly_logger, launch_logging_service
logging = get_loggly_logger(__name__, level="INFO")
launch_logging_service()

#I.retrieve_data_since_last_update()
#df, df_ix, intervals = pcp.create_measures(custom_measures="ewma")
#df, df_ix, intervals = pcp.create_measures()
df, df_ix, intervals = pcp.create_measures(days_from_present=91, custom_measures=("7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                    "ewma", "ewma_std"), custom_intervals=(3, 7, 14, 30, 60, 90))

increase_percentages = [0.1,0.2,0.3,0.5,0.75,1,2,3,5,10,25,50]
_periods=[30, 90, 180]
examination_periods = []
modifier = 0
for p in _periods:
    end = 0
    if isinstance(modifier, (int,float)):
        p += modifier
        end += modifier
    examination_periods.append((str(p) + "_day_period",du.get_epoch(days=p, start_of_day=True),du.get_epoch(days=end, start_of_day=True)))



###CUSTOM INDEXING CODE###
#ix = df.groupby('coin_symbol', sort=False).size().tolist()
#df_ix = [0]
#count = 0
#for i in range(0, len(ix)):
#   df_ix.append(ix[i] + count)
#    count += ix[i]
########################
#df['']

for i in range(1, len(df_ix)):
    if len(df[df_ix[i - 1]:df_ix[i]][df.price_usd > 0.0]) <= 0:
        continue
    coin_opening_price = df[df_ix[i - 1]:df_ix[i]][df.price_usd > 0.0]["price_usd"].iloc[0]
    df.loc[df.index[df_ix[i - 1]:df_ix[i]], "pct_change_since_ico"] = df["price_usd"][df_ix[i - 1]:df_ix[i]]/coin_opening_price - 1


start_time = time.time()
for i in range(1, len(df_ix)):
    print "at index %s" % df_ix[i-1]
    now = (time.time() - start_time)/60
    print "minutes since start %s" % now
    if len(df[df_ix[i - 1]:df_ix[i]][df.price_usd > 0.0]) <= 0:
        continue
    for p in examination_periods:
        if len(df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1]) & (df.price_usd > 0)]) <= 0: continue
        period_opening_price = df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1]) & (df.price_usd > 0)]["price_usd"].iloc[0]
        column_name =  str(int((p[2] - p[1])/86400)) + "_day_pct_change_from_" + du.convert_epoch(p[1],e2str=True,custom_format="%Y%m%d")
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "price_" + column_name] = \
            df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1])]["price_usd"] / period_opening_price - 1
        df.loc[df.index[df_ix[i - 1]:df_ix[i]], "ewma_" + column_name] = \
        df[df_ix[i - 1]:df_ix[i]][(df.uts < p[2]) & (df.uts > p[1])]["ewma"] / period_opening_price - 1
        for pct in increase_percentages:
            first_pct = df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])]['ewma_' + column_name].iloc[0]
            a = df[df_ix[i-1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])].groupby((df['ewma_' + column_name] < pct).cumsum()).cumcount(ascending=False)
            b = (a.shift(1) < a) & (a.shift(-1) < a)
            if first_pct > pct:
                b.iloc[0] = True
            pct_column_name = str(pct*100) +"_pct_" + str(int((p[2] - p[1])/86400)) + "_day_run_from_"+du.convert_epoch(p[1],e2str=True,custom_format="%Y%m%d")
            df.loc[df.index[df_ix[i - 1]:df_ix[i]], pct_column_name] = 0
            for item in a[b].iteritems():
                df[pct_column_name].iloc[item[0]] = item[1]

#Useful snippets
#df[df['2500_pct_90_day_run_from_20180602'] > 1].loc[:,('coin_name','utcdate','2500_pct_90_day_run_from_20180602')]
#df[df['coin_name'] == cn].loc[:,('coin_name','utcdate','ewma_90_day_pct_change_from_20180602')]
#i = 620
#pct = 25
#a = df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])].groupby((df['ewma_90_day_pct_change_from_20180602'] > pct).cumsum()).cumcount(ascending=False)
#(df[df_ix[i - 1]:df_ix[i]][(df['uts'] > p[1]) & (df['uts'] < p[2])]['ewma_90_day_pct_change_from_20180602'] > pct).cumsum()