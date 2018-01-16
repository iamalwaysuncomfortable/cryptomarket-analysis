import res.env_config as ec
import data_scraping.scrapingutils as su
import data_scraping.datautils as du
import log_service.logger_factory as lf
import os
lf.launch_logging_service()
logging = lf.get_loggly_logger(__name__)
ec.set_specific_config("cryptocurrencychart")

uts_time_zero = du.convert_time_format('1970-01-01T00:00:00Z', str2dt=True)

def ccc_api_call(call):
    r = su.get_http(call, headers={"Key": os.environ["CC_CHART_KEY"], "Secret": os.environ["CC_CHART_SECRET"]})
    if r.status_code == 200:
        logging.info("Response from %s OK", call)
        return r
    else:
        logging.warn("Response from %s BAD with response code of %s", call, r.status_code)
        logging.warn("Response content was %s", r)
        return r

def try_json_conversion(response):
    try:
        logging.info("Returning response in JSON format")
        json = response.json()
        return json
    except:
        logging.warn("No Json Encoding detected in response, returning original response format")
        return response

def get_single_coin_history_in_daterange(coin_id, start, end):
    call = "https://www.cryptocurrencychart.com/api/coin/history/"+str(coin_id)+"/"+start+"/"+end+"/price/usd"
    r = ccc_api_call(call)
    result = try_json_conversion(r)
    return result

def get_coin_ids():
    url = "https://docs.google.com/spreadsheet/ccc?11DjT1F-YRLE-6VvcZs9R_N8vtomMfOaMoj3wCYjKu0M&output=csv"
    df = su.read_csv(url)

class PriceCollector(object):
    def __init__(self, id_list):
        self.coin_data = {}
        self.id_list = id_list

    def collect_full_price_history_of_coin(self, coin_id):
        coin_name = self.id_list[coin_id]['name']
        current_date = du.get_time(now=True, utc_string=True, custom_format='%Y-%m-%d')
        one_year_prior = du.get_time(input_time=current_date, months=12, utc_string=True, custom_format='%Y-%m-%d')
        price_data = get_single_coin_history_in_daterange(coin_id, current_date, one_year_prior)

def push_batch_time_data_one_coin(data):
    data_to_write = []
    table_names = ("token_prices",)
    table_subset = []
    input_data = (data,)
    i = 0
    for values in input_data:
        if values:
            sql = "INSERT INTO pricedata.token_prices ("
            table_subset.append(table_names[i])
            data_to_write.append((sql, input_data[i]))
        i += 1
    logging.info("github data: %s being written to db", table_subset)
    db.write_data(data_to_write)

def get_epoch(date):
    date = date + "T00:00:00Z"
    date = du.convert_time_format(date, str2dt=True)
    epoch = (date - uts_time_zero).total_seconds()
    return int(epoch)

def get_coin_list():
    call = "https://www.cryptocurrencychart.com/api/coin/list"
    r = ccc_api_call(call)
    result = try_json_conversion(r)
    return result
