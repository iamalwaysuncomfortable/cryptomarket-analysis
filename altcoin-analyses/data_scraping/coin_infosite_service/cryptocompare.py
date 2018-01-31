import res.env_config as ec
import log_service.logger_factory as lf
import data_scraping.HTTP_helpers as HTTPh
logging = lf.get_loggly_logger(__name__)
ec.set_specific_config("cryptocurrencychart")

def check_limit():
    call = "https://min-api.cryptocompare.com/stats/rate/hour/limit"
    r = HTTPh.general_api_call(call, return_json=True)
    callsleft = int(r["CallsLeft"]["Histo"])
    return callsleft

def get_coin_list(order_by_rank=False):
    call = "https://min-api.cryptocompare.com/data/all/coinlist"
    r, status_code = HTTPh.general_api_call(call, return_json=True, return_status=True)
    if status_code == 200:
        if order_by_rank==True:
            return sorted(r['Data'].values(), key=lambda k:int(k['SortOrder']))
        return r
    else:
        logging.warn("Non 200 status returned, returning error response")
        return r

def get_histo_hour(epoch, from_sym, to_sym="USD", limit="2000", hour_resolution="1", exchange="CCCAGG"):
    call = "https://min-api.cryptocompare.com/data/histohour?fsym="+from_sym+"&tsym="+to_sym+"&limit="+str(limit)+\
           "&aggregate="+str(hour_resolution)+"&toTs="+str(epoch)+"&e="+exchange
    result = HTTPh.general_api_call(call, return_json=True)
    return result

def get_full_snapshot_by_id(coin_id):
    call = "https://www.cryptocompare.com/api/data/coinsnapshotfullbyid/?id=" + str(coin_id)
    result = HTTPh.general_api_call(call, return_json=True)
    return result

def get_price_data(_epoch, coin_symbol, pairs):
    if isinstance(_epoch, (int, float, str, unicode)):
        epoch = _epoch
        if isinstance(epoch, (int, float)):
            epoch = str(int(epoch))
    else:
        raise ValueError("UTC time must be in int, float, unicode or string format")
    if not isinstance(coin_symbol, (str, unicode)):
        raise ValueError("Coin symbol must be in str or unicode format")
    if isinstance(pairs, (str, unicode, list, tuple)):
        pair_statement = pairs
        if isinstance(list, tuple):
            pair_statement = ""
            for element in pairs:
                if not isinstance(element, (str, unicode)):
                    raise ValueError("All elements in list must be string or unicode")
                pair_statement += element + ","
            pair_statement = pair_statement[0:len(pair_statement)-1]
    else:
        raise ValueError("pair inputs must be string format or a list/tuple of strings")

    call = "https://min-api.cryptocompare.com/data/pricehistorical?fsym=" +\
           coin_symbol + "&tsyms="+pair_statement+"&ts=" + str(epoch)
    result = HTTPh.general_api_call(call, return_json=True)
    return result

