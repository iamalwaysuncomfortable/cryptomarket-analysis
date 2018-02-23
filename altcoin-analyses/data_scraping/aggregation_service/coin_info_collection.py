import res.db_data.sql_statement_library as psqll
import db_services.readwrite_utils as rw
import data_scraping.datautils as du
import data_scraping.coin_infosite_service.cryptocompare as cc
import data_scraping.coin_infosite_service.coinmarketcap as cmc
import aggregation_helpers as ah
import log_service.logger_factory as lf
import time
import gc
from collections import Counter
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

categorical_sql_reads, categorical_sql_writes = psqll.crypto_categorical_data_statements(read=True, write=True)
price_sql_reads, price_sql_writes = psqll.crypto_price_data_statements(read=True, write=True)

fiat_pairs = {"USD":"US Dollar", "EUR":"EURO", "CNY":"Chinese RMB"}

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
    def __init__(self, api_limit=50, initialize_data_containers=True):
        self.write_queue = {}
        self.validation_data = {}
        self.api_limit = api_limit
        self.coin_list = ah.cache_online_data(86400, "COIN_LIST")
        self.launch_dates = {x[0]:int(x[1]) for x in rw.get_data_from_one_table(price_sql_reads['start_dates']) if int(x[1]) > 0}
        if initialize_data_containers == True:
            self.initialize_data_containers("price_history", "pair_history")

    def initialize_data_containers(self, *args):
        for arg in args:
            if not isinstance(arg, (str, unicode)):
                msg = "Error arg %s was of type %s " % (str(arg), type(arg))
                logging.warn(msg)
                raise TypeError(msg)
            if arg not in self.write_queue:
                self.write_queue[arg] = []
            if arg not in self.validation_data:
                self.validation_data[arg] = {}

    def process_queue(self, queue_name, task=None, record=None, initialize_if_not_exits=False, write_threshold=1000,
                      force_write=False):
        def write_to_db():
            ##Write task to DB, if db writing function returns successfully, delete record
            logging.debug("sql statement is %s", price_sql_writes[queue_name])
            logging.debug("length of write queue is %s", len(self.write_queue))
            if queue_name == "pair_history":
                logging.debug("first element in write_queue is %s", self.write_queue[queue_name][-1])
            result = rw.push_batch_data([self.write_queue[queue_name]], [price_sql_writes[queue_name]])
            if result == "success":
                logging.info("Data written successfully, resetting record queue")
                del self.write_queue[queue_name]
                self.write_queue[queue_name] = []
                gc.collect()
            else:
                logging.warn("Data not written successfully, record queue not modified")

        if task == None:
            logging.warn("Task was blank, task must be specified, nothing will be done")
            return
        if queue_name not in self.write_queue:
            if initialize_if_not_exits == True:
                self.initialize_data_containers(queue_name)
            else:
                msg = "queue name specified is not initialize, please initialize queue before processing"
                logging.warn(msg)
                raise ReferenceError(msg)
        if task == 'add':
            self.write_queue[queue_name].append(record)
            if force_write == True:
                write_to_db()
                return
        if ((task == 'write_to_db' and (force_write == True or len(self.write_queue[queue_name]) > write_threshold))) or \
                (task == 'add' and isinstance(write_threshold, (float, int)) and write_threshold != -1 and
                 len(self.write_queue[queue_name]) > write_threshold):
            write_to_db()


    def validate_completeness_of_single_record_history(self, record, validation_type):
        if validation_type == "price":
            query = psqll.price_history_one_token(record, all_fields=True)
            results = sorted(rw.get_data_from_one_table(query), key=lambda x: x[8], reverse=True)
            if len(results) > 0:
                final_price = results[-1][3]
                if final_price > 0:
                    return False, results
                else:
                    return True, results
            else:
                return False, results
        if validation_type == "pair":
            query = psqll.pair_history_one_pair(record[0], record[1])
            results = sorted(rw.get_data_from_one_table(query), key=lambda x: x[8], reverse=True)
            if len(results) > 0:
                is_all_zeros = any(results[-1][6:10])
                return is_all_zeros, results
            else:
                return False, results


        #TODO: Add step to day level resolution was recorded

    def get_coin_names(self, *args):
        results = []
        for arg in args:
            if arg in fiat_pairs:
                name = fiat_pairs[arg]
                results.append(name)
            else:
                name = self.coin_list[arg]['CoinName']
                results.append(name)
        if len(results) == 1:
            return results[0]
        else:
            return tuple(results)

    def get_collection_start_date(self, key, table, epoch_location, format=None):
        if not key in self.validation_data[table]:
            if table == "price_history":
                t0 = du.get_time(now=True, custom_format='%Y-%m-%d', utc_string=True) + 'T00:00:01Z'
            elif table == "pair_history":
                t0 = du.get_time(now=True, utc_string=True)
            else:
                raise ValueError("Invalid table specified")
            reference_epoch = int((du.convert_time_format(t0, str2dt=True) - du.get_time(
                input_time='1970-01-01T00:00:00Z')).total_seconds())
        else:
            logging.info("Data for %s token already exists but, is not complete, resuming", key)
            reference_epoch = self.validation_data[table][key]["last_recorded_epoch"]
            logging.info("Collection will resume at epoch %s", reference_epoch)
        return reference_epoch

    def collect_single_pair_history(self, from_pair, to_pair, exchange="CCCAGG", hour_delta=1, records="2000",
                                    write_theshold=80000):
        if from_pair in self.launch_dates:
            launch_epoch = self.launch_dates[from_pair]
        else:
            launch_epoch = -1

        pair_key = from_pair + to_pair
        from_name, to_name = self.get_coin_names(from_pair, to_pair)
        reference_epoch = self.get_collection_start_date(pair_key, "pair_history", 0)
        keep_calling, loops = True, 0
        unresponsive_call_count = 0
        while keep_calling:
            if loops % 30 == 0:
                try:
                    limit = cc.check_limit()
                    logging.info("API limit is at %s", limit)
                    if limit < self.api_limit:
                        logging.warn('API limit is %s function will wait to execute until limit resets', limit)
                        return
                except:
                    logging.error("Response from limit endpoint bad", exc_info=True)
            loops += 1
            data = cc.get_histo_hour(reference_epoch, from_pair, to_pair, records, hour_delta, exchange)
            try:
                pair_data = data["Data"]
            except Exception as e:
                logging.warn("Pair capture failed, data returned was %s, error message was %s", str(data), e)
                keep_calling = False
                continue
            if len(pair_data) > 0:
                logging.debug("Data-full respose returned from cc api")
                logging.debug("Epoch tried was %s, launch_epoch was %s, will set epoch to %s",
                              reference_epoch, launch_epoch, data["TimeFrom"])
                for p in reversed(pair_data):
                    utc_time = du.convert_epoch(p["time"], e2str=True)
                    record = (p["time"], utc_time, from_pair, to_pair, from_name, to_name, p["low"], p["high"],
                              p["open"], p["close"], False)
                    self.process_queue("pair_history","add",record, write_threshold=-1)
                reference_epoch = data["TimeFrom"] - 3600
                unresponsive_call_count = 0
            else:
                logging.debug("Blank response returned Epoch tried was %s, launch_epoch was %s", reference_epoch, launch_epoch)
                logging.debug("Unresponsive hour count is %s", unresponsive_call_count)
                logging.debug("Loop count is %s length of write queue is %s", loops, len(self.write_queue['pair_history']))
                if launch_epoch < 0:
                    keep_calling = False
                if len(self.write_queue['pair_history']) == 0 and loops == 1:
                    logging.debug("No information returned in the first response")
                    keep_calling = False
                    continue
                elif reference_epoch > launch_epoch:
                    if unresponsive_call_count <= 24:
                        logging.debug("Stepping back %s seconds", 3600)
                        unresponsive_call_count += 1
                        reference_epoch -= 3600
                    if 168 >= unresponsive_call_count > 24:
                        logging.debug("Stepping back %s seconds", 3600*24)
                        reference_epoch -= 3600*24
                        unresponsive_call_count += 24
                    if 672 >= unresponsive_call_count > 168:
                        logging.debug("Stepping back %s seconds", 3600*168)
                        reference_epoch -= 3600*168
                        unresponsive_call_count += 168
                    if unresponsive_call_count > 672:
                        logging.debug("Stepping back %s seconds", 3600*672)
                        reference_epoch -= 3600*672
                    continue
                elif reference_epoch <= launch_epoch:
                    keep_calling = False
                    logging.debug("Backstepping limit reached")
                    if loops == 1 or len(self.write_queue) == 0:
                        continue

                i = 0
                check_length = sum(1 for x in self.write_queue['pair_history'] if x[2] == from_pair)
                logging.debug("check length is %s", check_length)
                for x in reversed(self.write_queue["pair_history"][-check_length:]):
                    i += 1
                    if x[2] == from_pair:
                        if x[6] ==0 and x[7] == 0 and x[8] == 0 and x[9] == 0:
                            pass
                        else:
                            logging.debug("pair_history length being deleted is %s", i)
                            logging.debug("Reversed history is %s",
                                          reversed(self.write_queue["pair_history"][-check_length:]))

                            #logging.debug("pair history being deleted is %s", self.write_queue["pair_history"][-i:][::-1])
                            #time.sleep(30)
                            del self.write_queue["pair_history"][-i:]
                            record = (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], True)
                            self.process_queue("pair_history", "add", record, write_threshold=-1)
                            break
                self.process_queue('pair_history',task="write_to_db", force_write=True)

    def collect_history(self, data_type, to_pair=None, to_latest=False):
        symbols_to_scrape = self.check_unrecorded_records(data_type, to_pair)
        logging.debug(symbols_to_scrape)
        for symbol in symbols_to_scrape:
            try:
                if data_type == "price":
                    self.collect_full_price_history_of_coin(symbol, self.coin_list[symbol]['CoinName'])
                if data_type == "pair":
                    self.collect_single_pair_history(symbol[0], symbol[1])
            except Exception as e:
                logging.error("Loop Failed to complete, error message was: %s", exc_info=True)
                self.process_queue(data_type + "_history", task="write_to_db")

    def check_unrecorded_records(self, validation_type, to_pair=None, current_to_most_recent=False):
        del self.validation_data[validation_type + "_history"]
        gc.collect()
        self.initialize_data_containers(validation_type+"_history")
        if validation_type == "price":
            for x in rw.get_data_from_one_table(price_sql_reads["min_price_dates"]):
                sym, epoch, price = x[0], int(x[1]), float(x[2])
                self.validation_data['price_history'][sym] = {"last_recorded_epoch": epoch, "symbol":sym,
                                                             "price_usd": price}
            finished_symbols = set(v['symbol'] for v in self.validation_data['price_history'].values() if v['price_usd'] == 0)
            coin_symbols = set(x for x in set(self.coin_list.keys()))
            symbols_to_scrape = list(coin_symbols.difference(finished_symbols))
            symbols_to_scrape = [v[0] for v in sorted([(x, self.coin_list[x]['SortOrder']) for x in symbols_to_scrape],
                                                      key=lambda k: int(k[1]))]
        elif validation_type == "pair" and isinstance(to_pair, (str, unicode)):
            pairs_with_start_dates = rw.get_data_from_one_table(psqll.pair_histories_start_date_truth())
            finished_symbols = set([])
            if len(pairs_with_start_dates) > 0:
                for x in pairs_with_start_dates:
                    sym, epoch, is_start_date, to_sym = x[0], int(x[1]), float(x[2]), x[3]
                    self.validation_data['pair_history'][sym] = {"last_recorded_epoch": epoch, "symbol":sym,
                                                                 "is_start_date": is_start_date, "to_sym":to_sym}
                finished_symbols = set((v["symbol"], to_pair) for v in self.validation_data['pair_history'].values() if
                                       v["is_start_date"]==True and v["to_sym"] == to_pair)
            coin_symbols = set((x, to_pair) for x in set(self.coin_list.keys()))
            symbols_to_scrape = list(coin_symbols.difference(finished_symbols))
            symbols_to_scrape = [v[0] for v in sorted([(x, self.coin_list[x[0]]['SortOrder']) for x in symbols_to_scrape],
                                                      key=lambda k: int(k[1]))]
        else:
            raise TypeError("Unsupported record type passed")
        logging.debug("finished symbols are %s", finished_symbols)
        return symbols_to_scrape

    def collect_full_price_history_of_coin(self, symbol, name):
        epoch = self.get_collection_start_date(symbol,"price_history",7)
        utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')
        keep_calling = True
        loops = 0
        while keep_calling:
            loops += 1
            if loops % 10 == 0:
                try:
                    remaining = cc.check_limit()
                    logging.info("API Limit Currently At %s", str(remaining))
                    if remaining <= self.api_limit:
                        logging.warn("CryptoCompare API limit below %s, function exiting", str(remaining))
                        keep_calling = False
                except:
                    logging.error("Response from limit endpoint bad", exc_info=True)

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
            self.process_queue("price_history", task='add', record=record, write_threshold=1000)
            epoch = epoch - 86400
            utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')

def collect_coin_rank():
    coins = cmc.get_coin_list()
    rankings = [(x['symbol'], x['market_cap_usd']) if x['market_cap_usd'] > 0 else (x['symbol'], 0) for x in coins]
    sql = "INSERT INTO pricedata.coin_ranks (symbol, marketcap) VALUES (%s, %s) ON CONFLICT (symbol) DO UPDATE SET (marketcap) = (EXCLUDED.marketcap)"
    result = rw.push_batch_data([rankings], [sql])
    return result

