import sys
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

if len(set(sys.argv).intersection({"dev"})) == 1:
    import data_scraping.gitservice.interface2 as I2
if len(set(sys.argv).intersection({"pair","price", "attributes"})) == 1:
    import data_scraping.aggregation_service.coin_info_collection as cic

def collect_dev_data(since_last_update=False, all_time=False):
    if since_last_update == True:
        I2.retrieve_data_since_last_update()
    if all_time == True:
        I2.retrieve_alltime_data_from_github()

def collect_pricing_data(data_type, temporal_direction="future"):
    to_most_recent = True
    if temporal_direction == "past":
        to_most_recent = False
    CP = cic.CollectPrices()
    CP.collect_history(data_type=data_type, current_to_most_recent=to_most_recent)

def execute_functions(args):
    possible_args = {"price", "pair","dev","attributes"}
    given_args = possible_args.intersection(set(args))
    if len(given_args) == 1:
        task = given_args.pop()
    else:
        if len(args) == 1:
            print("No Arguments were supplied")
            print("One Argument Must Be Specified and be ONLY ONE Of The Following: %s " % list(possible_args))
        else:
            print("Invalid Arguments Specified. Arguments Must Be ONLY ONE Of The Following: %s " % list(possible_args))
            args.pop(0)
            print("Args Supplied Were: %s" % args )
        return
        #raise ValueError(error)

    if task in ("price","pair"):
        if '-past' in args:
            collect_pricing_data(task, temporal_direction='past')
        else:
            collect_pricing_data(task)
    elif task == "dev":
        if '-all' in args:
            collect_dev_data(all_time=True)
        else:
            collect_dev_data(since_last_update=True)
    elif task == "attributes":
        CCA = cic.StaticDataCollector(extra_symbols={"VEN":"Vechain [Token]"})
        CCA.fetch_static_data_on_all_coins()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        doc = "\n Usage: python scraping_utils.py [-past] price/pair | [-all] dev | attributes \n" \
             "\n"\
             " Commands:\n" \
             "   dev: collect dev statistics from github accounts specified \n" \
             "      default behavior collects info from last record date to present\n"\
             "   pair: collect open/hi/lo/close records for all specified coins in\n" \
              "      (format: [COIN] -> USD/EUR/CNY/BTC pairs) from cryptocompare.com\n" \
             "      default behavior is to collect from last record to most current\n"\
             "   price: collect price records (format: [COIN] -> USD/EUR/CNY/BTC pairs)\n" \
              "      from cryptocompare.com for all specified crypto coins\n" \
             "      default behavior is to collect from last record to most current\n" \
              "   attributes: collect information on coins for crypto coins\n" \
                "\n"\
              " Options:\n" \
              "  [-past]: get records from current record to beginning of price/pair records\n" \
              "  [-all]: repeat full collection of all specified records from github.com \n"
        print doc
    else:
        execute_functions(sys.argv)