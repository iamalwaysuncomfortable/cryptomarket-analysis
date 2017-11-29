#Launch logger
import log_service.logger_factory as lf
lf.launch_logging_service()

##Configure environment
import res.env_config as __config__
__config__.set_master_config()

import pandas as pd
import data_scraping.datautils as du
import data_scraping.scrapingutils as su
import re
import time
import datetime
import numpy as np
import analysis.regression as reg
import data_scraping.gitservice.interface as I
from data_scraping.gitservice.data_writing import db_exists
import os
print(os.environ.keys())


##Declare initial variables
cmp_num_columns = ['price_usd', 'market_cap_usd', 'price_usd', '24h_volume_usd', 'price_btc', 'rank', 'total_supply',
                   'percent_change_1h', 'percent_change_24h', 'percent_change_7d']
cmp_txt_columns = ["id", "name", "symbol"]
measure_filters = {"rank":[0, 300], "total_supply":[2e5,1.2e11], "price_usd":[0., 3e3]}

#Price Stats
price_data = su.get_coinlist("https://api.coinmarketcap.com/v1/ticker/")
price_data = du.pre_process_types(price_data,cmp_num_columns, cmp_txt_columns)
price_data = price_data.set_index("symbol")

#Prooftype/Hashtype/Premined Status
algo_data = su.get_techinfo("https://www.cryptocompare.com/api/data/coinlist/")
algo_data = algo_data.loc[price_data.index.values]
combined_data = pd.concat([price_data,algo_data], axis=1)
combined_data["platform"] = "unique"

#Determination of Derivative or Unique Blockchain Status
derivative_coinlist = su.get_derivative_coin_data()
for coin in derivative_coinlist.keys():
    if len(coin) == 16:
        coin_data = combined_data[combined_data.name.str.contains(re.escape(coin[0:12]))]
    else:
        coin_data = combined_data[combined_data.name == coin]
    if coin_data.empty is False:
        derivative_coin_symbol = coin_data.index[0]
        basis_coin_symbol = derivative_coinlist[coin]
        if basis_coin_symbol is not "":
            basis_coin_algorithm = combined_data.ix[basis_coin_symbol]["algorithm"]
            combined_data.set_value(derivative_coin_symbol, "platform", basis_coin_symbol)
            combined_data.set_value(derivative_coin_symbol, "algorithm", str(basis_coin_algorithm) + "-d")

#Include static categorical data
categorical_data, replacement_data = su.get_static_data()
socdev_data = su.get_social_and_dev_data()

combined_data = combined_data.join(categorical_data)
combined_data = combined_data.join(socdev_data)

#Monitor platform adoptions (i.e. networks that can create derivative tokens)
platform_adoption_count = combined_data["platform"].value_counts()
platform_adoption_data = combined_data.groupby('platform').agg([np.sum, np.mean])
combined_data["derivative_token_count"] = 0.0
combined_data["derivative_token_mcap"] = 0.0
combined_data["24h_volume_derivative_tokens"] = 0.0
combined_data["av_price_derivative_tokens"] = 0.0
for symbol, count in platform_adoption_count.iteritems():
    if symbol != "unique":
        combined_data.set_value(symbol, "derivative_token_count", count)
        combined_data.set_value(symbol, "derivative_token_mcap", platform_adoption_data.loc[symbol]["market_cap_usd"]["sum"])
        combined_data.set_value(symbol, "24h_volume_derivative_tokens", platform_adoption_data.loc[symbol]["24h_volume_usd"]["sum"])
        combined_data.set_value(symbol, "av_price_derivative_tokens", platform_adoption_data.loc[symbol]["price_usd"]["mean"])


# Integrate dev stats pulled from Github API
def collect_github_stats(df):
    I.retrieve_data_since_last_update_from_github()
    monthly_dev_stats = I.collect_all_stats_in_time_range(du.get_time(months=1, utc_string=True), du.get_time(now=True, utc_string=True))
    print("monthly dev stats are")
    print(type(monthly_dev_stats))
    print(monthly_dev_stats)
    fields = monthly_dev_stats.itervalues().next()
    for key in fields['stats'].keys(): df[key + "_last_30_days"] = np.nan
    for key in fields['lifetime_stats'].keys():
        if not key in df.columns: df[key + "_all_time"] = np.nan
        else: df.rename(index=str, columns={key: key + "_all_time"})
    df["total_repos"], df["active_repos"] = np.nan, np.nan
    for k, v in monthly_dev_stats.iteritems():
        symbol = v["coin_symbol"]
        if symbol in combined_data.index:
            for stat_name, data in v['stats'].iteritems():
                combined_data.set_value(symbol, stat_name + "_last_30_days",data)
            for stat_name, data in v['lifetime_stats'].iteritems():
                combined_data.set_value(symbol, stat_name + "_all_time", data)
            combined_data.set_value(symbol, "total_repos", v["total_repos"])
            combined_data.set_value(symbol, "active_repos", v["active_repos"])

def execute_db_dependent_feature_collection():
    if not db_exists():
        return
    collect_github_stats(combined_data)

execute_db_dependent_feature_collection()


#Perform Regression to determine how currencies in related classes perform
reg_data = du.filter_df(combined_data, m_filters=measure_filters, d_exclude=True)
coef, intercept, sd, residues, ci = reg.fit_linear(np.log(reg_data["total_supply"].values), np.log(reg_data["price_usd"].values))
combined_data["stddevs"] = 0.0
combined_data["nearest_high_price"] = 0.0

for symbol, row in combined_data.iterrows():
    stddevs = (np.log(row[10]) - (np.log(row[12])*coef + intercept))/sd
    combined_data.set_value(symbol, "stddevs", stddevs)

#Find nearest reasonable price
reg_data = du.filter_df(reg_data, m_filters={"price_usd":[0.,1e3]})
for symbol, row in combined_data.iterrows():
    if row[12] > 1e9:
        weight = 0.33
    else:
        est_price = np.exp((np.log(row[12]) * coef + intercept))
        weight = 1.0 / (1.0 + np.exp(-10.0 * est_price))
    combined_data.set_value(symbol, "nearest_high_price",
                            reg_data[(reg_data["total_supply"] > (row[12] - weight*row[12])) & (reg_data["total_supply"] < (row[12] + weight*row[12]))]['price_usd'].max())


ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H')

combined_data.to_csv("coin_data-" + st + "hr.csv" )
du.dict_tocsv("price_vs_supply_regressiondata" + st + "hr.csv",["coefficient", "intercept", "error_std_dev", "rss", "99%_ci"],{"coefficient":coef, "intercept":intercept, "error_std_dev":sd, "rss":residues, "99%_ci":ci})
