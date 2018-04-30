import data_scraping.aggregation_service.coin_info_collection as ci
import data_scraping.gitservice.interface as I
import gc
from sys import argv
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

if __name__ == "main":
    arg_queue = []
    for arg in argv[1:]:
        if arg in ("pricedata", "pairdata", "githubdata", "-hist"):
            arg_queue.append(arg)
            if len(arg_queue) > 0 and arg[0] != "-":
                logging.warn("Current iteration of this software does not use multi-threading. Any data collection "
                             "tasks passed will operate in series. It is currently recommended to start up a new"
                             " interpreter instance for each process if it is desired to run processes in parallel")

    i = 0
    for arg in arg_queue:
        if arg == "pricedata":
            PriceCollectionObject = ci.CollectPrices()
            if len(arg_queue) > (i + 1) and arg_queue[i + 1] == "-hist":
                PriceCollectionObject.collect_history("price", "USD", current_to_most_recent=False)
            else:
                PriceCollectionObject.collect_history("price", "USD", current_to_most_recent=True)
            del PriceCollectionObject
            gc.collect()
        elif arg == "pairdata":
            PriceCollectionObject = ci.CollectPrices()
            if len(arg_queue) > (i + 1) and arg_queue[i + 1] == "-hist":
                PriceCollectionObject.collect_history("pair", "USD", current_to_most_recent=False)
            else:
                PriceCollectionObject.collect_history("pair", "USD", current_to_most_recent=True)
            del PriceCollectionObject
            gc.collect()
        elif arg == "githubdata":
            I.retrieve_data_since_last_update_from_github()
            pass
        i += 1




