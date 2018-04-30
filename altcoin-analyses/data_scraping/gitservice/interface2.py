from res.env_config import set_master_config
set_master_config()

import threading
from time import sleep
from log_service.logger_factory import get_loggly_logger, log_exceptions_from_entry_function, launch_logging_service
import data_scraping.gitservice.collect_stats2 as cs
import data_scraping.gitservice.process_data as prd
from data_scraping.gitservice.data_writing import push_github_data_to_postgres, get_github_data_from_postgres, \
    get_stored_queries, get_db_timestamp, update_db_timestamp, clean_stored_queries
import custom_utils.datautils as du


logging = get_loggly_logger(__name__)
launch_logging_service()

def get_and_store_records_from_API(start=None, existing_data=None):
    data_storage = []
    def write_stored_info():
        while True:
            if len(data_storage) > 1:
                for item in data_storage:
                    push_github_data_to_postgres(stars=item[0], forks=item[1], issues=item[2], pullrequests=item[3],
                                                 repo_stats=item[4],commits=item[5])

    logging.info("Getting repo and org stats from %s", start)
    stats_collector, org_data = cs.create_collection_objects(existing_data, start, return_org_data=True)
    org_stats = prd.stage_org_totals(org_data, du.get_time(now=True, utc_string=True))
    for result in stats_collector:
        stars, forks, issues, pullrequests, repo_stats, commits = prd.stage_data(result, db_format=True)
        push_github_data_to_postgres(stars, forks, issues, pullrequests, repo_stats, commits=commits)

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

def retrieve_data_from_github_from_date(start_date):
    task = check_for_overlimit_tasks(start_date)
    if task:
        get_and_store_records_from_API(start_date, task)
    else:
        get_and_store_records_from_API(start_date)

@log_exceptions_from_entry_function(logging)
def retrieve_data_from_date(start_date):
    retrieve_data_since_last_update_from_github(start_date)

@log_exceptions_from_entry_function(logging)
def retrieve_alltime_data_from_github():
    start_date = "2007-10-01T00:00:00Z"
    retrieve_data_from_github_from_date(start_date)

@log_exceptions_from_entry_function(logging)
def retrieve_data_since_last_update_from_github():
    start_date = get_db_timestamp()
    start_date_delta = du.get_time(days=1, input_time=start_date, utc_string=True)
    retrieve_data_from_github_from_date(start_date_delta)

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