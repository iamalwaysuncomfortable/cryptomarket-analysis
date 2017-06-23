import pandas as pd
import webdata
import re

##Columns to change from unicode fo float and str respectively
cmp_num_columns = ['price_usd', 'market_cap_usd', 'price_usd', '24h_volume_usd', 'price_btc', 'rank', 'total_supply',
                   'percent_change_1h', 'percent_change_24h', 'percent_change_7d']
cmp_txt_columns = ["id", "name", "symbol"]

##Data Cleaning Utility Methods
def pre_process_types(df, num_columns = None, txt_columns = None):
    processed_data = df
    if num_columns is not None:
        for column in num_columns:
            processed_data[column] = pd.to_numeric(df[column])
    if txt_columns is not None:
        for column in txt_columns:
            processed_data[column] = df[column].astype(str)
    return processed_data

def clean_outliers(df, max_price, max_supply):
    cleaned_data = df[(df.price_usd < max_price) & (df.total_supply < max_supply)]
    return cleaned_data

##Action Code (Refactor into a main module soon)

#Price Stats
number_of_coins = {"limit":200}
price_data = webdata.get_coin_data("https://api.coinmarketcap.com/v1/ticker/", number_of_coins)
price_data = pre_process_types(price_data,cmp_num_columns, cmp_txt_columns)
price_data = price_data.set_index("symbol")

#Prooftype/Hashtype/Premined Status
algo_data = webdata.get_crypto_compare_data("https://www.cryptocompare.com/api/data/coinlist/")
algo_data = algo_data.loc[price_data.index.values]
combined_data = pd.concat([price_data,algo_data], axis=1)
combined_data["platform"] = "unique"

#Determination of Derivative or Unique Blockchain Status
derivative_coinlist = webdata.get_derivative_coin_data()
for entry in derivative_coinlist.keys():
    if len(entry) == 16:
        coin_data = combined_data[combined_data.name.str.contains(re.escape(entry[0:12]))]
    else:
        coin_data = combined_data[combined_data.name == entry]
    if coin_data.empty is False:
        derivative_coin_symbol = coin_data.index[0]
        basis_coin_symbol = derivative_coinlist[entry]
        basis_coin_algorithm = combined_data.ix[basis_coin_symbol]["algorithm"]
        combined_data.set_value(derivative_coin_symbol, "platform", basis_coin_symbol)
        combined_data.set_value(derivative_coin_symbol, "algorithm", basis_coin_algorithm + "-d")

combined_data.to_csv("coin_data.csv")