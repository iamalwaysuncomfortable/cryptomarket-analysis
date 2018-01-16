import log_service.logger_factory as lf
from data_scraping import scrapingutils as su
from db_services.db import make_single_query
import data_scraping.reddit_service.environment_config as ec
logging = lf.get_loggly_logger(__name__)

ignore_list = [x[0].lower() for x in make_single_query("SELECT * FROM redditdata.general_crypto_reddits", 0, all=True)]

class SearchManager(object):
    def __init__(self, coin_list, error_list, search_function, analysis_function=None, storage_function=None):
        self.coin_list = coin_list
        self.error_list = error_list
        self.search_function = search_function
        self.analysis_function = analysis_function
        self.storage_function = storage_function
        if not callable(search_function) or not (callable(storage_function) or storage_function==None):
            raise AttributeError("Search_function must be callable and storage_function must be None or callable")

    def reset_data(self, coin_list, error_list):
        self.coin_list = coin_list  
        self.error_list = error_list

    def search(self, *args, **kwargs):
        self.search_function(self.coin_list, self.error_list, *args, **kwargs)

    def store_results(self, *args, **kwargs):
        if self.storage_function == None or not callable(self.storage_function):
            logging.warn("No valid storage function defined, nothing will be stored")
        else:
            self.storage_function(self.coin_list, *args, **kwargs)

    def get_results(self):
        return self.coin_list

    def get_errors(self):
        return self.error_list

def get_canonical_coinlist(start = 0, result_limit=0):
    api_call = "https://api.coinmarketcap.com/v1/ticker/?start=" + str(start) + "&limit=" + str(result_limit)
    coin_results = su.get_coinlist(api_call)
    coin_results = coin_results[["name", "id", "symbol"]]
    coin_results["sr_data"] = None
    return coin_results

def get_tuned_search_manager(num_coins=None, coin_list=None):
    if coin_list is None:
        if isinstance(num_coins, int):
            coin_list = get_canonical_coinlist(result_limit=num_coins)
        else:
            coin_list = get_canonical_coinlist()
        return SearchManager(coin_list, {"indexes": [], "coin_name": [], "errors": []},
                             search_function=subreddit_search)
    else:
        return SearchManager(coin_list, {"indexes":[], "coin_name":[], "errors":[]}, search_function=subreddit_search)

#error_list = {"indexes":[], "coin_name":[], "errors":[]}
def subreddit_search(coin_list, error_list, search_column="name", header=ec.get_header_from_envars(),
                     start=None, end=None):
    data = coin_list
    coin_column = data[search_column]
    if (isinstance(start, int) and isinstance(end, int)):
        if end > start: search_indices = range(start, end)
        else: raise ValueError("End index must be greater than start index")
    else:
        search_indices = data[data['sr_data'].isnull()].index.tolist()

    for i in search_indices:
        url = "https://www.reddit.com/search.json?q="+coin_column[i]+"&t=all&type=sr"
        try:
            response = su.get_http(url, headers=header)
            if response.status_code == 200:
                logging.info("Response code for coin %s was 200 - url was %s", coin_column[i], url)
                response = response.json()
                sr_list = []
                for result in response['data']['children']:
                    sr_name, title, desc = result['data']['display_name_prefixed'].lower(), \
                                           result['data']['title'].lower(), result['data']['public_description'].lower()
                    if sr_name not in ignore_list: sr_list.append([sr_name, title, desc])
                data.set_value(i, "sr_data", sr_list)
            else:
                logging.warn("Non 200 status code detected for coin %s, url %s - code %s - message %s",
                             coin_column[i], url, response.status_code, response.content)
        except Exception as e:
            error_msg = "Error Message: %s, proceeding to next" % str(e)
            logging.warn(error_msg)
            error_list["indexes"].append(i)
            error_list["coin_name"].append(coin_column[i])
            error_list["errors"].append(error_msg)
    data['normalized_title'] = data.name.map(lambda x: x if type(x) != unicode else (x.lower()).replace(' ', ''))


