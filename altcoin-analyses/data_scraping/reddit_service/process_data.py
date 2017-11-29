import types
import data_scraping.datautils as du
import data_scraping.reddit_service.db_interface as rdbi
import db_services.db as db
import log_service.logger_factory as lf
from api_communication import SubredditStatCollector
logging = lf.get_loggly_logger(__name__)

TOP_VALUES = ("day", "week", "month", "year", "all")

class RedditDataProcessor(object):
    @staticmethod
    def __validate_and_initialize_inputs__(period, subreddit_list, client_id = None,
                                           client_secret = None, user = None, pw = None, initialize_new_object=True):
        credentials = (client_id, client_secret, user, pw)
        if period in TOP_VALUES and isinstance(subreddit_list, (list, tuple, set)) and \
                all(isinstance(x, (unicode, str)) for x in subreddit_list):
            if initialize_new_object == True:
                if all(credentials):
                        ssc = SubredditStatCollector(period, subreddit_list, client_id=client_id,
                                                     client_secret=client_secret,user=user, pw=pw,
                                                     use_default_envars=False)
                        return ssc, ssc.get_stats_from_next_subreddit()
                else:
                    ssc = SubredditStatCollector(period, subreddit_list)
                    return ssc, ssc.get_stats_from_next_subreddit()
            else:
                return period, subreddit_list
        else:
            if period not in TOP_VALUES:
                error_msg = "period must be specified as day, week, month, year, or all"
            elif not isinstance(subreddit_list, (list, tuple, set)):
                error_msg = "input specified must be list, tuple, or set type"
            else:
                error_msg = "all inputs in subreddit list must be string or unicode"
            logging.error(error_msg)
            raise ValueError(error_msg)

    @staticmethod
    def fetch_subreddit_data_greedily(period, subreddit_list, stat_collector=None):
        if stat_collector != None and isinstance(stat_collector, SubredditStatCollector):
            stats_collector, stats_generator = stat_collector, stat_collector.get_stats_from_next_subreddit()
        else:
            stats_collector, stats_generator = \
                RedditDataProcessor.__validate_and_initialize_inputs__(period, subreddit_list)
        numerical_stats = []
        nl_stats = []
        while True:
            try:
                sr_data = stats_generator.next()
                numerical_stats.append(sr_data["numerical_stats"])
                for submission in sr_data['natural_language_data']['submissions']:
                    nl_stats.append(submission)
            except StopIteration:
                logging.info("End of results reached, exiting data collection")
                break
        return numerical_stats, nl_stats, stats_collector.subreddit_list

    def __init__(self, period, subreddit_list, stats_collector=None, client_id=None,
                 client_secret=None, user=None, pw=None):
        if stats_collector != None and isinstance(stats_collector, SubredditStatCollector):
            self.stat_collector, self.stat_generator = stats_collector, stats_collector.get_stats_from_next_subreddit()
        else:
            self.stat_collector, self.stat_generator = \
                self.__validate_and_initialize_inputs__(period, subreddit_list, client_id, client_secret, user, pw)
        self.nl_stats = []
        self.numerical_stats = []

    def get_unprocessed_subreddits(self):
        if isinstance(self.stat_collector, SubredditStatCollector):
            return self.stat_collector.subreddit_list
        else:
            logging.error("No valid subreddit statistics object initialized", exc_info=True)
            raise AttributeError("No valid subreddit statistics object initialized")

    def get_processed_data(self):
        return self.numerical_stats, self.nl_stats

    def fetch_and_write_subreddit_data_to_db(self):
        if isinstance(self.stat_collector, SubredditStatCollector) and\
            isinstance(self.stat_generator, types.GeneratorType):
            with db.return_connection() as conn:
                while True:
                    try:
                        sr_data = self.stat_generator.next()
                        rdbi.push_record_with_existing_conn(conn, subreddit_stats=sr_data['numerical_stats'],
                        submission_data=sr_data['natural_language_data']['submissions'])
                    except StopIteration:
                        logging.info("End of results reached, exiting data collection")
                        break
        else:
            error_msg = "stat collection objects not initialized or initialized incorrectly"
            logging.error(error_msg, exc_info=True)
            raise AttributeError(error_msg)

    def fetch_and_store_subreddit_data_statefully(self):
        if isinstance(self.stat_collector, SubredditStatCollector) and\
            isinstance(self.stat_generator, types.GeneratorType):
            while True:
                try:
                    sr_data = self.stat_generator.next()
                    self.numerical_stats.append(sr_data['numerical_stats'])
                    for submission in sr_data['natural_language_data']['submissions']:
                        self.nl_stats.append(submission)
                except StopIteration:
                    logging.info("End of results reached, exiting data collection")
                    break
        else:
            error_msg = "stat collection objects not initialized or initialized incorrectly, re-initialization can"\
            "be done via the add_new_subbredit_list method"
            logging.error(error_msg, exc_info=True)
            raise AttributeError(error_msg)

    def add_new_subreddit_list(self, period, subreddit_list):
            period, subreddit_list = self.__validate_and_initialize_inputs__(period, subreddit_list,
                                                                             initialize_new_object=False)
            self.stat_collector.initialize_new_subreddit_list(period, subreddit_list)
            self.stat_generator = self.stat_collector.get_stats_from_next_subreddit()
            self.nl_stats = []
            self.numerical_stats = []