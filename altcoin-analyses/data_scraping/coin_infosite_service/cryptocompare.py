import res.env_config as ec
import data_scraping.datautils as du
import threading
import log_service.logger_factory as lf
import data_scraping.HTTP_helpers as HTTPh
import db_services.readwrite_utils as rw
import res.db_data.sql_statement_library as psqll
logging = lf.get_loggly_logger(__name__)
ec.set_specific_config("cryptocurrencychart")

write_statements, read_statements = psqll.crypto_compare_statements()

def validate_existing_price_data():
    data = rw.get_data_from_one_table(read_statements["get_all"])
    get_coin_list()
    return data

class CollectPrices(object):
    def __init__(self, api_limit=50, queue_record_limit=20):
        self.coin_data = {}
        self.record_queue = []
        self.api_limit = api_limit
        self.queue_record_limit = queue_record_limit
        self.semaphore = threading.BoundedSemaphore(1)

    def __record_typecheck__(self, record):
        expected_types = ((str, unicode), (str, unicode), int, )

    def process_queue(self, task=None, record=None):
        if task == None:
            logging.warn("Task was blank, task must be specified, nothing will be done")
            return
        elif task == 'add' and self.__record_typecheck__(record) == True:
            self.record_queue.append(record)
        elif task == 'write_to_db':
            ##Write task to DB, if db writing function returns successfully, delete record set
            result = rw.push_batch_data(self.record_queue, write_statements["price"])
            if result == "success":
                logging.info("Data written successfully, resetting record queue")
                self.record_queue = []
            else:
                logging.warn("Data not written successfully, record queue not modified")


    def write_data_in_queue(self, record_limit=100):
        if len(self.record_queue) % 100 == 0:
            with self.semaphore:
                ###Write info
                logging.info("beggining data write")
                self.process_queue(task="write_to_db")


    def collect_full_price_history_of_coin(self, symbol, name):
        limit = check_limit()
        if limit <= 50:
            logging.info("")
            logging.warn("API limit is at %s function will wait to execute until limit resets", str(limit))
            return

        if not symbol in self.coin_data:
            self.coin_data[symbol] = []
            t0 = du.get_time(now=True, custom_format='%Y-%m-%d', utc_string=True) + 'T00:00:01Z'
            epoch = int((du.convert_time_format(t0, str2dt=True) - du.get_time(
                input_time='1970-01-01T00:00:00Z')).total_seconds())
        else:
            epoch = min(self.coin_data[symbol], key=lambda k: k[4])[4]
        utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')
        call = "https://min-api.cryptocompare.com/data/pricehistorical?fsym="+str(symbol)+"&tsyms=USD&ts="+str(epoch)

        keep_calling = True
        loops = 0
        while keep_calling:
            loops += 1
            if loops % 20 == 0:
                remaining = check_limit()
                logging.info("API Limit Currently At %s", str(remaining))
                if remaining <= self.api_limit:
                    logging.warn("CryptoCompare API limit below %s, function exiting", str(remaining))
                    keep_calling = False

            data = HTTPh.general_api_call(call)
            data = HTTPh.try_json_conversion(data)
            price = data[symbol]["USD"]

            if price <= 0:
                keep_calling = False

            record = (symbol, name, price, utcdate, epoch, "cryptocompare.com")
            self.coin_data[symbol].append(record)
            self.process_queue(task='add', record=record)
            epoch = epoch - 86400
            utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')



def check_limit():
    call = "https://min-api.cryptocompare.com/stats/rate/hour/limit"
    r = HTTPh.general_api_call(call)
    r = HTTPh.try_json_conversion(r)
    callsleft = int(r["CallsLeft"]["Histo"])

def get_coin_list(order_by_rank=False):
    call = "https://min-api.cryptocompare.com/data/all/coinlist"
    r = HTTPh.general_api_call(call)
    result = HTTPh.try_json_conversion(r)
    if order_by_rank==True:
        return sorted(r['Data'].values(), key=lambda k:int(k['SortOrder']))
    return result
