from res.env_config import set_master_config
set_master_config()
from custom_utils .decorators.memoize import memoize_with_timeout
import data_scraping.gitservice.collect_stats2 as cs
import data_scraping.gitservice.process_data as prd
from data_scraping.gitservice.data_writing import push_github_data_to_postgres, get_github_data_from_postgres, \
    get_stored_queries, get_db_timestamp
import custom_utils.datautils as du

from log_service.logger_factory import get_loggly_logger, log_exceptions_from_entry_function, launch_logging_service

logging = get_loggly_logger(__name__, level="INFO")
launch_logging_service()

@memoize_with_timeout
def get_org_data():
    return cs.create_collection_objects(return_org_data=True)

get_org_data.cache["timeout"] = 7200

def get_and_store_records_from_API(start=None, existing_data=None):
    org_data = existing_data
    if existing_data == None:
        logging.debug("No Existing Data Specified, collecting data default list of organizations")
        org_data = get_org_data()
        logging.info("%s github accounts collected, starting data collection on their repos", len(org_data))
    stats_collector = cs.create_collection_objects(org_data, start, return_stats_generator=True)
    for result in stats_collector:
        stars, forks, issues, pullrequests, repo_stats, commits = prd.stage_data(result, db_format=True)
        push_github_data_to_postgres(stars, forks, issues, pullrequests, repo_stats, commits=commits)
    org_stats = prd.stage_org_totals(org_data, du.get_time(now=True, utc_string=True))
    push_github_data_to_postgres(org_stats=org_stats)

def check_for_overlimit_tasks(since=None):
    if since:
        tasks = get_stored_queries(since)
    else:
        tasks = get_stored_queries()
    return tasks

def execute_overlimit_task(task_number):
    task_data = get_stored_queries(task_number)
    if task_data:
        get_and_store_records_from_API(existing_data=task_data)

def retrieve_data_from_last_stored_updates():
    get_and_store_records_from_API()

def retrieve_data_from_github_from_date(start_date):
    task = check_for_overlimit_tasks(start_date)
    if task:
        get_and_store_records_from_API(start_date, task)
    else:
        get_and_store_records_from_API(start_date)

@log_exceptions_from_entry_function(logging)
def retrieve_data_from_date(start_date):
    start_date_delta = du.get_time(days=1, input_time=start_date, utc_string=True)
    retrieve_data_from_github_from_date(start_date_delta)

@log_exceptions_from_entry_function(logging)
def retrieve_alltime_data_from_github():
    start_date = "2007-10-01T00:00:00Z"
    retrieve_data_from_github_from_date(start_date)

@log_exceptions_from_entry_function(logging)
def retrieve_data_since_last_update():
    start = None
    org_data = get_org_data()
    stats_collector = cs.create_collection_objects(org_data, start, return_stats_generator=True)
    for result in stats_collector:
        stars, forks, issues, pullrequests, repo_stats, commits = prd.stage_data(result, db_format=True)
        push_github_data_to_postgres(stars, forks, issues, pullrequests, repo_stats, commits=commits)

def collect_all_stats_in_time_range(start, end):
    data = get_github_data_from_postgres(True, True, True, True, True, True, True, True, 0, start, end)
    logging.debug("data length from database %s", len(data))
    stats = prd.count_github_org_stats(data, start, end)
    logging.debug("length of processed stats are %s", len(data))
    return stats

@log_exceptions_from_entry_function(logging)
def collect_org_stats_in_time_range(start, end, org):
    data = get_github_data_from_postgres(True, True, True, True, True, True, True, True, 0, start, end, org)
    stats = prd.count_github_org_stats(data, start, end)
    return stats