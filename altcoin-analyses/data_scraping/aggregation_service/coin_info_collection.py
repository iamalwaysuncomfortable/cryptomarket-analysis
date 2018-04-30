import gc
import time

import aggregation_helpers as ah
import custom_utils.datautils as du
import data_scraping.coin_infosite_service.coinmarketcap as cmc
import data_scraping.coin_infosite_service.cryptocompare as cc
import db_services.readwrite_utils as rw
import log_service.logger_factory as lf
import res.db_data.sql_statement_library as psqll
from custom_utils.type_validation import validate_type

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

    """Collect price information from cryptocompare.com

    Args:
        api_limit (int): Hourly api_limit for cryptocompare.com once reached will cause class to cease record collection 
        initialize_data_containers (bool): Initialize data containers for day-day price and open/low/close/high data
         for altcoin/USD data
    
    Attributes:
        write_queue (dict): Dictionary containing one list object for each price data collection type, each entry
        a list object fits its corresponding format in the database it is written to
        validation_data (dict): Dictionary containing specific information needed to determine if records in database
        are up to date
        api_limit (int): Hourly api_limit for cryptocompare.com once reached will cause class to cease record collection 
        coin_list (dict): List of coins and their surrounding data
    """
    def __init__(self, api_limit=50, initialize_data_containers=True):
        self.write_queue = {}
        self.validation_data = {}
        self.api_limit = api_limit
        self.coin_list = ah.cache_online_data(86400, "COIN_LIST")
        self.launch_dates = {x[0]:int(x[1]) for x in rw.get_data_from_one_table(price_sql_reads['start_dates']) if int(x[1]) > 0}
        if initialize_data_containers == True:
            self.initialize_data_containers("price_history", "pair_history")

    def __gc_internal_storage__(self, internal_attribute, target_field, msg=None):

        """Gabrage Collect Record Queue or Validation Data"""

        if internal_attribute == "write_queue":
            del self.write_queue[target_field]
            gc.collect()
            self.write_queue[target_field] = []
        elif internal_attribute == "validation_data":
            del self.validation_data[target_field]
            gc.collect()
            self.write_queue[target_field] = []
        if isinstance(msg, (str, unicode)):
            logging.info(msg)

    def initialize_data_containers(self, *args):

        """Initialize or re-initialize internal write queue and validation dictionaries for various historical price
        collections types. Sets the entries for each collection method to blank
        
        Args:
            *args (str): Names of price data collection methods (day over day price, open/high/low/close, etc.)
        """

        for arg in args:
            validate_type(arg, (str, unicode))
            if arg not in self.write_queue:
                self.write_queue[arg] = []
            if arg not in self.validation_data:
                self.validation_data[arg] = {}

    def process_queue(self, queue_name, task=None, record=None, write_threshold=100, force_write=False, upsert=False):

        """Add new records to internal class list which stores records prior to writing them to database
        behavior is either 
        Args:
            queue_name (str, unicode): Which queue to process (currently "pair_history" or "price_history")
            task (str, unicode): Task to perform (add to queue or write to qeueue
            record (tuple): Record to add to queue for later writing to database
            write_threshold (int): Number of records in queue before write operation takes place
            force_write (bool): Force write existing records in queue to database
        """

        def write_to_db():

            logging.debug("sql statement is %s length of write queue is %s", price_sql_writes[queue_name], len(self.write_queue))
            if upsert == True:
                result = rw.push_batch_data([self.write_queue[queue_name]], [price_sql_writes[queue_name + "_update"]])
            else:
                result = rw.push_batch_data([self.write_queue[queue_name]], [price_sql_writes[queue_name]])

            if result == "success":
                self.__gc_internal_storage__("write_queue",queue_name, "Data written successfully, resetting write queue")
            else:
                logging.warn("Data not written successfully, record queue not modified")

        if task == None:
            logging.warn("Task was blank, task must be specified, nothing will be done")
            return
        if queue_name not in self.write_queue:
            raise ReferenceError("queue name specified is not initialize, please initialize queue before processing")
        if task == 'add':
            self.write_queue[queue_name].append(record)
        if ((task == 'write_to_db' and (force_write == True or len(self.write_queue[queue_name]) > write_threshold))) or \
                (task == 'add' and (force_write == True or (isinstance(write_threshold, (float, int))
                and write_threshold != -1 and len(self.write_queue[queue_name]) > write_threshold))):
            write_to_db()

    def get_coin_names(self, *args):
        """Get list of coin names from passed coin symbols"""
        return [fiat_pairs[arg] if arg in fiat_pairs else self.coin_list[arg]['CoinName'] for arg in args]

    def get_collection_start_date(self, symbol, table, current_to_most_recent=False):
        """
        Method determines what date collection should start at. It will retrieve the date from the validation 
        dictionary constructed by the check_unrecorded_records method for the given crypto. 
        If the key does not exist, it will return the current date.
        
        Args:
            symbol (str, unicode): Symbol of the 
            table (str, unicode): Table 
            current_to_most_recent (bool)
        Returns:
            reference_epoch (float): Epoch time representing the time on which to start collection
        """

        if not symbol in self.validation_data[table]:
            if table == "price_history":
                t0 = du.get_time(now=True, custom_format='%Y-%m-%d', utc_string=True) + 'T00:00:01Z'
            elif table == "pair_history":
                t0 = du.get_time(now=True, utc_string=True)
                if current_to_most_recent == True:
                    return -1
            else:
                raise ValueError("Invalid table specified")
            reference_epoch = int((du.convert_time_format(t0, str2dt=True) - du.get_time(
                input_time='1970-01-01T00:00:00Z')).total_seconds())
        else:
            logging.info("Data for %s token already exists but, is not complete, resuming", symbol)
            reference_epoch = self.validation_data[table][symbol]["last_recorded_epoch"]
            logging.info("Collection will resume at epoch %s", reference_epoch)
        return reference_epoch

    def collect_single_pair_history(self, from_pair, to_pair, exchange="CCCAGG", hour_delta=1, records="2000",
                                    write_theshold=80000, current_to_most_recent=False, upsert=False, from_date=None):
        collect_to_front = current_to_most_recent
        if from_pair in self.launch_dates:
            launch_epoch = self.launch_dates[from_pair]
        else:
            launch_epoch = -1

        pair_key = from_pair + to_pair
        from_name, to_name = self.get_coin_names(from_pair, to_pair)
        reference_epoch = self.get_collection_start_date(pair_key, "pair_history", current_to_most_recent=collect_to_front)
        step_size = records
        logging.debug("Collect to front is %s, reference epoch is %s", collect_to_front, reference_epoch)
        if collect_to_front == True:
            goal_epoch = reference_epoch
            logging.debug("Collect to front is %s", collect_to_front)
            now = int(du.get_epoch(current=True))
            reference_epoch = now
            if goal_epoch == -1:
                return
            else:
                step_size = str(int((now - goal_epoch)/3600 + 1))
        else:
            goal_epoch = launch_epoch
        keep_calling, loops = True, 0
        unresponsive_call_count = 0
        logging.debug("Collect to front is %s, reference epoch is %s, goal epoch is %s", collect_to_front, reference_epoch, goal_epoch)
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
            if current_to_most_recent == True:
                data = cc.get_histo_hour(reference_epoch, from_pair, to_pair, step_size, hour_delta, exchange)
                logging.info("data is %s", data)
            else:
                data = cc.get_histo_hour(reference_epoch, from_pair, to_pair, step_size, hour_delta, exchange)
            try:
                pair_data = data["Data"]
            except Exception as e:
                logging.warn("Pair capture failed, data returned was %s, error message was %s", str(data), e)
                keep_calling = False
                continue
            if len(pair_data) > 0:
                logging.debug("Data-full respose returned from cc api")
                logging.debug("Epoch tried was %s, goal_epoch was %s, step_size was %s, will set epoch to %s, collect_to_front %s",
                              reference_epoch, goal_epoch, step_size, data["TimeFrom"] - 3600, collect_to_front)
                for p in reversed(pair_data):
                    utc_time = du.convert_epoch(p["time"], e2str=True)
                    record = (p["time"], utc_time, from_pair, to_pair, from_name, to_name, p["low"], p["high"],
                              p["open"], p["close"], False)
                    self.process_queue("pair_history","add",record, write_threshold=-1)
                reference_epoch = data["TimeFrom"] - 3600
                if collect_to_front == True and (reference_epoch < goal_epoch):
                    keep_calling = False
                    self.process_queue("pair_history","write_to_db",write_threshold=write_theshold)
                unresponsive_call_count = 0
            else:
                if collect_to_front == True:
                    keep_calling = False
                    continue
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

    def check_unrecorded_records(self, validation_type, to_pair="USD", current_to_most_recent=False):

        """
        Check database records for records which are not complete and store data about completeness in the validation
        data dictionary and return a list of coin symbols which have incomplete records. Default behavior is to 
        determine if records have been collected to the beginning of the coin. If current_to_most_recent parameter is 
        set to true, function will find the most recent record date and compare that against the current date (in epoch
        time) and will consider coins incomplete if their last recorded date is not equal to the current date.
        Args:
            validation_type (str, unicode): Which price history to collect (currently "pair_history" or "price_history")
            to_pair (str, unicode): If collecting pair_data, which pair to translate price into
            current_to_most_recent (bool): Whether to move forward or backward in time
        Returns:
            symbols_to_scrape (list[(str, unicode)]): List of coin_symbols which have unfinished records
        """

        logging.debug("Validation Type is %s, to_pair is %s, ctmr %s", validation_type, to_pair, current_to_most_recent)

        self.__gc_internal_storage__("validation_data", validation_type + "_history")
        self.initialize_data_containers(validation_type+"_history")

        completion_conditions = {("price", True): 'du.get_epoch(current=True) - v["last_recorded_epoch"] < 86400',
                                 ("price", False):'v["price_usd"] == 0',
                                 ("pair", True):'du.get_epoch(current=True) - v["last_recorded_epoch"] < 3600',
                                 ("pair", False):'v["is_start_date"]==True and v["to_sym"] == to_pair'}

        sql_statements = {("price", True): price_sql_reads["max_price_dates"],
                          ("price", False): price_sql_reads["min_price_dates"],
                          ("pair", True): price_sql_reads["max_pair_dates"],
                         ("pair", False): psqll.pair_histories_start_date_truth()}

        pairs = rw.get_data_from_one_table(sql_statements[(validation_type, current_to_most_recent)])

        if len(pairs) == 0:
            return set()

        for x in pairs:
            if validation_type == "price":
                sym, epoch, price = x[0], int(x[1]), float(x[2])
                self.validation_data['price_history'][sym] = {"last_recorded_epoch": epoch, "symbol":sym,
                                                             "price_usd": price}
            elif validation_type == "pair" and current_to_most_recent == False:
                sym, epoch, is_start_date, to_sym = x[0], int(x[1]), x[2], x[3]
                self.validation_data['pair_history'][sym + to_pair] = {"last_recorded_epoch": epoch, "symbol":sym,
                                                             "is_start_date": is_start_date, "to_sym":to_sym}
            elif validation_type == "pair" and current_to_most_recent == True:
                sym, epoch, to_sym = x[0], int(x[1]), x[2]
                self.validation_data['pair_history'][sym + to_pair] = {"last_recorded_epoch": epoch, "symbol":sym,
                                                             "to_sym":to_sym}

        finished_symbols = set(v['symbol'] for v in self.validation_data[validation_type+ '_history'].values()
                               if eval(completion_conditions[(validation_type, current_to_most_recent)]))
        coin_symbols = set(x for x in set(self.coin_list.keys()))
        symbols_to_scrape = list(coin_symbols.difference(finished_symbols))
        symbols_to_scrape = [v[0] for v in sorted([(x, self.coin_list[x]['SortOrder']) for x in symbols_to_scrape],
                                                  key=lambda k: int(k[1]))]
        logging.debug("finished symbols are: %s - unfinished symbols are: %s", finished_symbols, symbols_to_scrape)
        return symbols_to_scrape

    def collect_full_price_history_of_coin(self, symbol, name, current_to_most_recent=False, upsert=False, from_date=None):

        """
        Makes successive calls to cryptocompare API until final record is reach at 1 day (86400s) intervals. Default 
        behavior is to step backwards until the beginning of the coin's history and periodically write those records to 
        the database. If current_to_most_recent is set to True behavior will be to step foward until the most current 
        day's price is captured.
        
        Args:
            symbol (str, unicode): Symbol of coin to collect price info on
            name (str, unicode): Name of coin
            current_to_most_recent (bool): If True, algorithim will collect forward
        """

        now = du.get_epoch(current=True)
        if isinstance(from_date, (int, float)):
            epoch = from_date
        else:
            epoch = self.get_collection_start_date(symbol,"price_history")
        utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')
        keep_calling = True
        loops = 0
        while keep_calling:
            loops += 1
            if loops % 40 == 0:
                try:
                    if cc.check_limit() <= self.api_limit:
                        logging.warn("CryptoCompare API limit below %s, function exiting", self.api_limit)
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
                keep_calling = False
                continue

            record = (symbol, name, price_BTC,price_USD, price_EUR, price_CNY, utcdate, epoch, "cryptocompare.com")

            if current_to_most_recent == True:
                if (now - epoch) < 86400:
                    keep_calling = False
                    if loops == 1:
                        break
                epoch = epoch + 86400
            else:
                if price_USD <= 0:
                    keep_calling = False
                epoch = epoch - 86400

            self.process_queue("price_history", task='add', record=record, write_threshold=100, upsert=upsert)
            utcdate = du.convert_epoch(epoch, e2str=True, custom_format='%Y-%m-%d')

    def collect_history(self, data_type, to_pair=None, current_to_most_recent=False, upsert=False, symbols=None, from_date=None):

        """Initialize collection of crypto price histories
        
        Args:
            data_type (str, unicode): Specification of historical price data to collect from cryptocompare
            to_pair (str, unicode): Destination pair to get prices in
            current_to_most_recent (bool): Collect forward or backwards, default false
        """

        if isinstance(symbols, (tuple, list)) and all(isinstance(sym, (str, unicode)) for sym in symbols):
            symbols_to_scrape = symbols
        elif symbols == None:
            symbols_to_scrape = self.check_unrecorded_records(data_type, to_pair,
                                                              current_to_most_recent=current_to_most_recent)
        else:
            raise ValueError("Symbols if specified must be in a list of unicode or str representations of coin symbols")

        for symbol in symbols_to_scrape:
            try:
                if data_type == "price":
                    self.collect_full_price_history_of_coin(symbol, self.coin_list[symbol]['CoinName'],
                                                            current_to_most_recent=current_to_most_recent, upsert=upsert,
                                                            from_date=from_date)
                if data_type == "pair":
                    self.collect_single_pair_history(symbol[0], symbol[1], current_to_most_recent=current_to_most_recent,
                                                     upsert=upsert, from_date=from_date)
            except Exception as e:
                logging.error("Loop Failed to complete, error message was: %s", exc_info=True)
                self.process_queue(data_type + "_history", task="write_to_db", upsert=upsert)

        self.process_queue(data_type + "_history", task="write_to_db", force_write=True, upsert=upsert)

def collect_coin_rank():

    """Get current rankings of coin by marketcap from coinmarketcap.com"""

    coins = cmc.get_coin_list()
    rankings = [(x['symbol'], x['market_cap_usd']) if x['market_cap_usd'] > 0 else (x['symbol'], 0) for x in coins]
    sql = "INSERT INTO pricedata.coin_ranks (symbol, marketcap) VALUES (%s, %s) ON CONFLICT (symbol) DO UPDATE SET" \
          " (marketcap) = (EXCLUDED.marketcap)"

    result = rw.push_batch_data([rankings], [sql])
    return result

