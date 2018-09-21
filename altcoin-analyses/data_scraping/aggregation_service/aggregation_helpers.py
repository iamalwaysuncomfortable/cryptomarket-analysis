import data_scraping.coin_infosite_service.tokenmarket as tm
import data_scraping.coin_infosite_service.coinmarketcap as cmc
import data_scraping.coin_infosite_service.cryptocompare as cc
import data_scraping.coin_infosite_service.etherscan as es
import custom_utils.decorators.memoize as mem
import db_services.readwrite_utils as rw
from collections import Counter

def return_derivative_token_list(extra_symbols=None):
    cmp_derivatives_list, cmp_platform_data = cmc.get_derivative_token_list(extra_symbols=extra_symbols)
    tm_derivatives_list, tm_platform_data = tm.get_derivative_token_list()
    master_derivatives_list = tm_derivatives_list.copy()
    for key in cmp_derivatives_list.keys():
        if key not in master_derivatives_list:
            master_derivatives_list[key] = cmp_derivatives_list[key]

    platform_counts = {}
    counts = [master_derivatives_list[key]['parent_platform_symbol'] for key in master_derivatives_list.keys()]
    for k, v in Counter(counts).items():
        platform_counts[k] = v
    #platform_counts['ETH'] = es.get_number_of_erc20_token_contracts()
    return master_derivatives_list, platform_counts

@mem.memoize_with_timeout
def cache_online_data(timeout, desired_output, extra_symbols=None):
    cache_online_data.cache['timeout'] = timeout
    if desired_output == "SDC":
        cc_coinlist = cc.get_coin_list()['Data']
        derivative_list, platform_counts = return_derivative_token_list(extra_symbols=extra_symbols)
        return cc_coinlist, derivative_list, platform_counts
    if desired_output == "COIN_LIST":
        coin_symbols = cc.get_coin_list()['Data']
        return coin_symbols

@mem.memoize_with_timeout
def make_db_call(timeout, sql_call):
    make_db_call.cache['timeout'] = timeout
    symbol_query = sql_call
    return rw.get_data_from_one_table(symbol_query)
