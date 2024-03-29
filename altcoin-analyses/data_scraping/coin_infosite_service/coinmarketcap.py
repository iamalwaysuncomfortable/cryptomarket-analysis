from bs4 import BeautifulSoup
import redis
import custom_utils.HTTP_helpers as HTTPh
import log_service.logger_factory as lf
import custom_utils.datautils as du
import json
logging = lf.get_loggly_logger(__name__)

def get_name_symbol_lookup_table(lookup_by_name=False, lookup_by_symbol=False, extra_symbols=None):
    master_list = get_coin_list()
    result_list = []

    if lookup_by_name == True:
        result_list.append({entry['name']:entry['symbol'] for entry in master_list})
        if isinstance(extra_symbols, dict):
            for symbol, name in extra_symbols.iteritems():
                result_list[0][name] = symbol
    if lookup_by_symbol == True:
        result_list.append({entry['symbol']:entry['name'] for entry in master_list})
        if isinstance(extra_symbols, dict):
            index = 0
            if len(result_list) == 2:
                index = 1
            for symbol, name in extra_symbols.iteritems():
                result_list[index][symbol] = name
    elif not any((lookup_by_name, lookup_by_symbol)):
        raise ValueError("at least one option must be specified, cannot have all options False")



    if len(result_list) > 1:
        return tuple(result_list)
    if len(result_list) == 1:
        return result_list[0]

def get_derivative_token_list(platform_stats=False, extra_symbols=None):
    page = HTTPh.general_api_call('https://coinmarketcap.com/tokens/views/all/')
    results = []
    symbol_lookup_table = get_name_symbol_lookup_table(lookup_by_symbol=True, extra_symbols=extra_symbols)
    coin_dict = {}
    platform_dict = {}
    if page.status_code == 200:
        contents = page.content
        soup = BeautifulSoup(contents, 'html.parser')
        derivative_coin_list = soup.tbody.find_all("tr")
        for coin in derivative_coin_list:
            coin_symbol = coin.find("a").contents[0]
            platform_symbol = coin["data-platformsymbol"]
            coin_dict[coin_symbol] = {'symbol':coin_symbol, 'name':symbol_lookup_table[coin_symbol],
                                      'parent_platform_symbol':platform_symbol,
                                      'parent_platform_name':symbol_lookup_table[platform_symbol], 'description':u""}
            if platform_symbol not in platform_dict:
                platform_dict[platform_symbol] = [coin_dict[coin_symbol]]
            else:
                platform_dict[platform_symbol].append(coin_dict[coin_symbol])
    else:
        msg = "Status code of %s returned, data not collected" %str(page.status_code)
        logging.warn(msg)
        HTTPh.throw_HTTP_error(msg)
    if platform_stats==True:
        platform_dict = {key:{"data":platform_dict[key], "number_derivative":len(platform_dict[key])}
                         for key in platform_dict.keys()}
        return coin_dict, platform_dict
    else:
        return coin_dict, platform_dict

def get_coin_list(cache_time=3600):
    try:
        r = redis.Redis(
            host='127.0.0.1',
            port=6379)
        if r.exists("cmp_list") and r.exists("date"):
            date = r.get("date")
            if ((du.get_epoch(current=True) - float(date)) > cache_time):
                print ("Fetching Re-Up")
                call = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
                coin_list = HTTPh.general_api_call(call, return_json=True)
                r.set("cmp_list",json.dumps(coin_list))
                r.set("date", du.get_epoch(current=True))
            else:
                print "get from cache"
                coin_list = json.loads(r.get('cmp_list'))
        else:
            call = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
            coin_list = HTTPh.general_api_call(call, return_json=True)
            r.set("cmp_list", coin_list)
            r.set("date", du.get_epoch(current=True))
    except Exception as e:
        print "redis error"
        error = "Retrieving data from redis cache failed with error message %s, contacting coinmarketcap for information" % e
        print(error)
        #logging.exception(error, e)
        call = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
        coin_list = HTTPh.general_api_call(call, return_json=True)
    return coin_list