import res.db_data.sql_statement_library as psqll
import db_services.readwrite_utils as rw
import data_scraping.datautils as du
import data_scraping.coin_infosite_service.cryptocompare as cc
import aggregation_helpers as ah
import log_service.logger_factory as lf
import time
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

categorical_sql_reads, categorical_sql_writes = psqll.crypto_categorical_data_statements(read=True, write=True)
price_sql_reads, price_sql_writes = psqll.crypto_price_data_statements(read=True, write=True)


class StaticDataCollector(object):
    def __init__(self, timeout=3600):
        self.cc_coin_list, self.derivative_token_list, self.platform_counts = ah.cache_online_data(timeout, "SDC")
        self.unrecorded_coins = self.check_unrecorded_records()
        self.coin_data_queue = []
        self.coin_data_dict = {}

    def try_int_conversion(self, data):
        try:
            int_converted = int(data)
            return int_converted
        except Exception as e:
            logging.warn("Could not convert %s to int, None value will be returned exception was %s", data, e)
            return None

    def fetch_static_data_on_coin(self, symbol):
        cc_id = self.cc_coin_list[symbol]['Id']
        r = cc.get_full_snapshot_by_id(cc_id)
        self.coin_data_dict[symbol] = r['Data']['General']
        coin_symbol = r['Data']['General']["Symbol"]
        coin_name = r['Data']['General']["Name"]
        start_date = r['Data']['General']["StartDate"]
        try:
            start_date_utc = int(du.convert_epoch(start_date, str2e=True, custom_format="%d/%m/%Y"))
        except Exception as e:
            logging.warn("start date %s could not be converted to uts, error was %s", start_date, e)
            start_date_utc = None
        algorithm = r['Data']['General']['Algorithm']
        total_supply = self.try_int_conversion(r['Data']['General']["TotalCoinSupply"])
        amount_premined = self.try_int_conversion(r['Data']['General']['PreviousTotalCoinsMined'])
        if not total_supply == None and not amount_premined == None and \
                        total_supply > 1000 and (total_supply - 10 <= amount_premined <= total_supply + 10):
            fully_premined = True
        else: fully_premined=False
        coinmarketcap_id = r['Data']['General']['Id']
        proof_type = r['Data']['General']["ProofType"]
        description = r['Data']['General']["Description"]
        features = r['Data']['General']["Features"]
        technology = r['Data']['General']["Technology"]
        website = r['Data']['General']["Website"]
        twitter = r['Data']['General']["Twitter"]
        derivative_token=None
        parent_blockchain=None
        if coin_symbol in self.derivative_token_list:
            derivative_token=True
            parent_blockchain=self.derivative_token_list[coin_symbol]['parent_platform_symbol']
            algorithm = "derivative_coin"
            proof_type = "derivative_coin"
        write_record = (coin_symbol, coin_name, start_date, start_date_utc, algorithm, total_supply, amount_premined,
                        fully_premined, coinmarketcap_id, proof_type, description, features, technology, website,
                        twitter, derivative_token,parent_blockchain)
        self.coin_data_queue.append(write_record)
        time.sleep(0.1)

    def check_unrecorded_records(self):
        sql = categorical_sql_reads['token_symbols']
        #TODO: Add Validation Step
        existing_symbols = set([item[0] for item in rw.get_data_from_one_table(sql)])
        cc_symbols = set(self.cc_coin_list.keys())
        return list(cc_symbols.difference(existing_symbols))

    def fetch_static_data_on_all_coins(self):
        unrecorded_records = self.check_unrecorded_records()
        for record in unrecorded_records:
            self.fetch_static_data_on_coin(record)
            if len(self.coin_data_queue) % 10 == 0:
                try:
                    status = self.write_data(self.coin_data_queue)
                    if status == "success":
                        self.coin_data_queue = []
                except Exception as e:
                    logging.warn("Error in write function, message: %s", e)

    def write_data(self, data):
        status = rw.push_batch_data([data], [categorical_sql_writes['write_cc_record']])
        return status

class CollectPrices(object):
    def __init__(self, api_limit=50, queue_record_limit=20):
        self.coin_data = {}
        self.record_queue = []
        self.api_limit = api_limit
        self.queue_record_limit = queue_record_limit
        self.coin_list = ah.cache_online_data(86400, "COIN_LIST")

    def process_queue(self, task=None, record=None):
        if task == None:
            logging.warn("Task was blank, task must be specified, nothing will be done")
            return
        if task == 'add':
            self.record_queue.append(record)

        if task == 'write_to_db' or (task == 'add' and len(self.record_queue) % 20 == 0):
            ##Write task to DB, if db writing function returns successfully, delete record set
            result = rw.push_batch_data([self.record_queue], [price_sql_writes["price"]])
            if result == "success":
                logging.info("Data written successfully, resetting record queue")
                self.record_queue = []
            else:
                logging.warn("Data not written successfully, record queue not modified")

    def validate_completeness_of_single_coin_price_history(self, coin_symbol):
        query = psqll.price_history_one_token(coin_symbol, all_fields=True)
        results = sorted(rw.get_data_from_one_table(query), key=lambda x:x[8], reverse=True)
        final_price = results[-1][3]
        if final_price >= 0:
            return final_price, results
        else:
            return final_price, "complete"
        #TODO: Add step to ensure day level resolution
        #for i in range(1, len(results)):
        #   if (results[i-1][0] - results[0][0]) != 86400:
        #        discontinuous_indices.append((i-1, i))

    def collect_all_coin_history(self):
        symbols_to_scrape = self.check_unrecorded_records()
        for symbol in symbols_to_scrape:
            try:
                self.collect_full_price_history_of_coin(symbol, self.coin_list[symbol]['CoinName'])
            except Exception as e:
                logging.warn("Loop Failed to complete, error message was: %s", e)
                self.process_queue(task="write_to_db")

    def check_unrecorded_records(self):
        sql = price_sql_reads['token_symbols']
        existing_symbols = set(x[0] for x in rw.get_data_from_one_table(sql))
        coin_symbols = set(self.coin_list.keys())
        symbols_to_scrape = list(coin_symbols.difference(existing_symbols))
        for symbol in existing_symbols:
            final_price, result = self.validate_completeness_of_single_coin_price_history(symbol)
            logging.info("final price from symbol %s is %s", symbol, final_price)
            if final_price > 0:
                self.coin_data[symbol] = result
                symbols_to_scrape.append(symbol)
        return symbols_to_scrape

    def collect_full_price_history_of_coin(self, symbol, name):
        limit = cc.check_limit()
        if limit <= 50:
            logging.warn("API limit is at %s function will wait to execute until limit resets", str(limit))
            return

        if not symbol in self.coin_data:
            self.coin_data[symbol] = []
            t0 = du.get_time(now=True, custom_format='%Y-%m-%d', utc_string=True) + 'T00:00:01Z'
            epoch = int((du.convert_time_format(t0, str2dt=True) - du.get_time(
                input_time='1970-01-01T00:00:00Z')).total_seconds())
        else:
            epoch = min(self.coin_data[symbol], key=lambda k: k[7])[7]
        utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')
        keep_calling = True
        loops = 0
        while keep_calling:
            loops += 1
            if loops % 10 == 0:
                remaining = cc.check_limit()
                logging.info("API Limit Currently At %s", str(remaining))
                if remaining <= self.api_limit:
                    logging.warn("CryptoCompare API limit below %s, function exiting", str(remaining))
                    keep_calling = False

            data = cc.get_price_data(epoch, symbol, "BTC,USD,EUR,CNY")
            try:
                price_BTC = data[symbol]["BTC"]
                price_USD = data[symbol]["USD"]
                price_EUR = data[symbol]["EUR"]
                price_CNY = data[symbol]["CNY"]
            except Exception as e:
                logging.warn("Price capturing failed, data returned was %s, error message was %s", str(data), e)
                keep_calling=False
                continue


            if price_USD <= 0:
                keep_calling = False

            record = (symbol, name, price_BTC,price_USD, price_EUR, price_CNY, utcdate, epoch, "cryptocompare.com")
            self.coin_data[symbol].append(record)
            self.process_queue(task='add', record=record)
            epoch = epoch - 86400
            utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')
            time.sleep(0.1)