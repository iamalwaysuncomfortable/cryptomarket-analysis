from log_service.logger_factory import get_loggly_logger, log_exceptions_from_entry_function
from data_scraping.gitservice.collect_stats import collect_repo_data, get_org_repos
from data_scraping.gitservice.process_data import stage_data, stage_org_totals, count_github_org_stats
from data_scraping.gitservice.data_writing import push_github_data_to_postgres, get_github_data_from_postgres, \
    get_stored_queries, get_db_timestamp, update_db_timestamp, clean_stored_queries
from data_scraping.datautils import get_time, convert_time_format

logging = get_loggly_logger(__name__)

def get_and_store_records_from_API(start=None, existing_data=None):
    logging.debug("Getting org data & storing it")
    org_data = get_org_repos()
    org_stats = stage_org_totals(org_data, get_time(now=True, utc_string=True))
    logging.debug("Getting repo stats from %s", start)
    data, errors, since = collect_repo_data(start, existing_data)
    stars, forks, issues, pullrequests, repo_stats, commits = stage_data(data, db_format=True)
    push_github_data_to_postgres(stars, forks, issues, pullrequests, repo_stats, org_stats, commits)
    clean_stored_queries(since)
    update_db_timestamp(since)

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
    start_date_delta = get_time(weeks=1, input_time=start_date, utc_string=True)
    retrieve_data_from_github_from_date(start_date_delta)

def collect_all_stats_in_time_range(start, end):
    data = get_github_data_from_postgres(True, True, True, True, True, True, True, True, 0, start, end)
    logging.debug("data length from database %s", len(data))
    stats = count_github_org_stats(data, start, end)
    logging.debug("length of processed stats are %s", len(data))
    return stats

@log_exceptions_from_entry_function(logging)
def collect_org_stats_in_time_range(start, end, org):
    data = get_github_data_from_postgres(True, True, True, True, True, True, True, True, 0, start, end, org)
    stats = count_github_org_stats(data, start, end)
    return stats