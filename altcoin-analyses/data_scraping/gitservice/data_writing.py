import json
from datetime import datetime

import custom_utils.datautils as du
import log_service.logger_factory as lf
from data_scraping.gitservice.db_interface import get_github_analytics_data, get_remaining_queries, \
    write_remaining_orgs, edit_last_updated_timestamp, get_last_updated_timestamp, clean_temp_data, \
    push_github_analytics_data, get_time_stats
from db_services.db import check_existence

##Setup Logger
logging = lf.get_loggly_logger(__name__)

###Information-to-disk methods
def error_processor(calling_func_name, errors):
    'Write errors to disk'
    logging.error(errors)
    if isinstance(errors, dict):
        du.write_log(calling_func_name + "_errors_" + str(datetime.now()).replace(" ", "_"),errors, file_extension=".json")
    else:
        du.write_log(calling_func_name + "_errors_" + str(datetime.now()).replace(" ", "_"), errors)

def db_exists():
    return check_existence()

def remaining_query_dump(since, remaining_repos):
    'If query exceeds limit, write remaining repos to search to disk'
    since = du.convert_time_format(since, dt2str=True)
    remaining_repos["records_start_date"] = since
    output = json.dumps(remaining_repos)
    write_remaining_orgs(since, output)

def clean_stored_queries(since):
    since = du.convert_time_format(since, dt2str=True)
    clean_temp_data(since)

def get_stored_queries(since=None):
    if since:
        since = du.convert_time_format(since, dt2str=True)
    stored_queries = get_remaining_queries(since)
    if not stored_queries["remainingqueries"]:
        return stored_queries["remainingqueries"]
    return stored_queries["remainingqueries"][0][1]

def push_github_data_to_postgres(stars = None, forks = None, issues = None, pullrequests = None, repo_stats = None, org_stats=None, commits=None):
    push_github_analytics_data(stars, forks, issues, pullrequests, repo_stats, org_stats, commits)

def get_github_data_from_postgres(stars=False, forks=False, pullrequests=False, issues=False, stats=False,
                                  orgtotals=False, commits=False, all_records=True, num_records=0, start=None, end=None, org=None,
                                  repo=None):
    data = get_github_analytics_data(stars, forks, pullrequests, issues, stats, orgtotals, commits, all_records, num_records,
                                     start, end, org,repo)
    return data

def update_db_timestamp(since):
    du.convert_time_format(since, dt2str=True)
    edit_last_updated_timestamp(since)

def get_db_timestamp():
    timestamp = get_last_updated_timestamp()
    return timestamp[0][0]

def update_last_updated_timestamp(since):
    since_dt = du.convert_time_format(since, str2dt=True)
    edit_last_updated_timestamp(since_dt)

def get_time_series_data(stars=True, forks=True, issues=True, pullrequests=True, commits=True, start=None, end=None, owner=None,repo=None, utf_format=True):
    data = get_time_stats(stars, forks, issues,pullrequests, commits, start=start, end=end, org=owner, repo=repo, utf_format=utf_format)
    return data