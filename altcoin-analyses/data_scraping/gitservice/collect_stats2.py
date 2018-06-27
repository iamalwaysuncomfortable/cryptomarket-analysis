from datetime import datetime
import pandas as pd
import pytz
import git_queries as gq
import log_service.logger_factory as lf
import api_communication as gitapi
from requests import HTTPError
from api_communication import try_gql_query, get_gsheet_data
from custom_utils import datautils as du
from custom_utils.errors.github_errors import GithubPaginationError, LimitExceededDuringPaginationError, \
    GithubAPIBadQueryError
from data_writing import remaining_query_dump, error_processor
from db_interface import get_latest_dates_of_database_update
logging = lf.get_loggly_logger(__name__)

def create_collection_objects(existing_data=None, since=None, return_org_data=False, return_stats_generator=False):
    _since=since
    if existing_data == None:
        multiple_repo_list, single_repo_list = get_repo_lists(multiple_only=True), get_repo_lists(single_only=True)
        org_data = get_org_repos(single_repo_list, multiple_repo_list)
    else:
        if "records_start_date" in existing_data.keys(): _since = existing_data["records_start_date"]
        org_data = existing_data
    if return_org_data == True and return_stats_generator == True:
        return collect_repo_data(org_data, _since), org_data
    elif return_stats_generator == True and return_org_data == False and bool(existing_data):
        return collect_repo_data(org_data, _since)
    elif return_org_data == True and return_stats_generator == False:
        return org_data
    else:
        raise ValueError("No Valid Outputs Specified")

###Get Authoritative List of Github Repos Associated with a Coin
def get_repo_lists(single_only=False, multiple_only=False):
    'get list of coin github accounts, sort by single repo or multiple repo accounts'
    repo_list = get_gsheet_data("https://docs.google.com/spreadsheet/ccc?key=1la_UVV4c3YLnvTesLTEq-F1wxL9Rtd2sZkdyq9OBMB4&output=csv")
    repo_list = repo_list[~repo_list.duplicated("owner") & pd.notnull(repo_list["owner"])]
    if single_only == True: return repo_list[repo_list["single_repo"] == True]
    if multiple_only == True: return repo_list[repo_list["single_repo"] == False]
    return repo_list

def get_org_repos(single_repo_list, multiple_repo_list):
    'Get all github repositories under a coin github account'
    skipped_repos = []
    repo_data, repo_count = {}, 0

    for k, v in single_repo_list.iterrows():
        org,repo = v["owner"], v["main_repo"]
        query, alias = gq.simple_repo_query(org, repo, full_query=True)
        org_data = {org:{"coin_name": v["id"], "coin_symbol": k, "repositories": {"totalCount": 1, "edges":[]}, "login": v["owner"]}}
        status, result = try_gql_query(query)
        if status == "success":
            org_data[org]["repositories"]["edges"].append(result['data'][alias])
            repo_data.update(org_data)
        else:
            skipped_repos.append((org, [result, query, status]))
            logging.error("Bad API Query result: %s - query: %s - HTTP status code: %s",
                          result, query, status, exc_info=True)
            continue

    for index, acc in multiple_repo_list.iterrows():
        try:
            repos = gitapi.get_all_repos_in_account_http(acc["account_type"], acc["owner"], 3)
        except HTTPError as e:
            logging.warn("HTTP status error, error data was %s", e)
            skipped_repos.append((acc["owner"],e))
            continue
        except Exception as e:
            logging.warn("Collection of Repo failed, error is %s, continuing", e)
            skipped_repos.append((acc["owner"], e))
            continue
        repo_data[acc["owner"].lower()] = {"repositories": {"totalCount": len(repos), "edges":[]}, "login": acc["owner"],
                                               "coin_name": multiple_repo_list[multiple_repo_list["owner"] ==  acc["owner"]].iloc[0]["id"],
                                               "coin_symbol": multiple_repo_list[multiple_repo_list["owner"] == acc["owner"]].index[0]}
        for r in repos:
            repo_data[acc["owner"].lower()]["repositories"]["edges"].append(
                {"name":r["name"], "updatedAt":r["updated_at"],
                 "owner":{"login":r["owner"]['login']}, "defaultBranchRef":{"name":r["default_branch"]}})

    logging.info("%s github accounts collected with %s total repos", len(repo_data.keys()), repo_count)
    logging.warn("Skipped repos were %s, full error data was %s", [sr[0] for sr in skipped_repos], skipped_repos)
    return repo_data

###Get repo Statistics
def page_backwards(org_data, owner, repo, since, num):
    alias, depth = gq.generate_alias(repo), 0
    if isinstance(since, datetime):
        since = unicode(since.strftime("%Y-%m-%dT%H:%M:%SZ"))
    else:
        since = unicode(since)
    'If records exist prior to requested date cannot be accessed by a single query, page backwards using cursors'
    logging.debug("pagination beginning for github account: %s - repo: %s", owner, repo)

    page_state = {"stars": True, "forks": True, "commits": True, "openissues": True, "closedissues": True,
                  "openrequests": True, "mergedrequests": True}
    cursors = {}
    since_delta_7w = du.get_time(input_time=since, weeks=7)
    since_delta_7w = unicode(du.convert_time_format(since_delta_7w, dt2str=True))

    #Get cursors from last element in list
    def get_cursors(data, _page_state):
        cursors = {}
        for key, t_value in _page_state.iteritems():
            if t_value and key == "commits":
                _page_state[key] = False
                if len(data["data"][alias]["commits"]["target"]["history"]["edges"]) >= num:
                    first_item = data["data"][alias]["commits"]["target"]["history"]["edges"][-1]["node"][
                        "committedDate"]
                    if first_item > since:
                        cursors[key] = data["data"][alias]["commits"]["target"]["history"]["edges"][-1]["cursor"]
                        cursors["default_branch"] = data["data"][alias]["defaultBranchRef"]["name"]
                        _page_state[key] = True

            elif key != "commits" and t_value:
                _page_state[key] = False
                if len(data["data"][alias][key]["edges"]) >= num:
                    if key == "closedissues":
                        timelines = [edge['node']['timeline']['nodes'] for edge in
                                     data["data"][alias]["closedissues"]["edges"]]
                        for timeline in timelines:
                            closed_dates = [event["createdAt"] for event in timeline if bool(event)]
                            if closed_dates and max(closed_dates) > since_delta_7w:
                                cursors[key] = data["data"][alias][key]["edges"][0]["cursor"]
                                _page_state[key] = True

                    else:
                        if key == "stars":
                            first_item = data["data"][alias][key]["edges"][0]["starredAt"]
                        elif key == "mergedrequests":
                            first_item = max([x['node']['mergedAt'] for x in data["data"][alias][key]["edges"]])
                            if first_item > since_delta_7w:
                                cursors[key] = data["data"][alias][key]["edges"][0]["cursor"]
                                _page_state[key] = True
                        else:
                            first_item = data["data"][alias][key]["edges"][0]["node"]["createdAt"]
                        if first_item > since and key !="mergedrequests":
                            cursors[key] = data["data"][alias][key]["edges"][0]["cursor"]
                            _page_state[key] = True

        logging.debug("%s : %s page_states are: %s at depth: %s", owner, alias, _page_state, depth)
        return cursors

    def send_query_and_combine_data(data, _page_state, _depth, _cursors):
        query = gq.pagination_query(owner, repo, _cursors, num, page_state["stars"], page_state["forks"],
                page_state["commits"], page_state["openissues"], page_state["closedissues"], page_state["openrequests"],
                page_state["mergedrequests"])
        status, paged_result = try_gql_query(query, limitcheck=True)
        if status == "success":
            for key, value in _page_state.iteritems():
                if key == "commits" and value:
                    data["data"][alias]["commits"]["target"]["history"]["edges"] += \
                        paged_result["data"][alias]["commits"]["target"]["history"]["edges"]
                elif key != "commits" and value:
                    data["data"][alias][key]["edges"] = paged_result["data"][alias][key]["edges"] + \
                                                        data["data"][alias][key]["edges"]
            return paged_result
        if status == "limit exceeded":
            status, remaining, limit_exceeded, resetAt = paged_result
            raise LimitExceededDuringPaginationError(resetAt, paged_result, query, data, _cursors, _depth)
        elif status == "error":
            logging.error('pagination error for org: %s repo: %s at depth % message: %s query was: %s',
                          owner, alias, _depth, paged_result[1], query)
            logging.exception("pagination error stacktrace: ")
            raise GithubPaginationError(paged_result[0], paged_result[1], paged_result, query, _cursors, _depth)

    while not all(t_value == False for t_value in page_state.values()):
        if depth == 0:
            cursors = get_cursors(org_data, page_state)
        if bool(cursors):
            paged_data = send_query_and_combine_data(org_data, page_state, depth, cursors)
            cursors = get_cursors(paged_data, page_state)
            depth += 1

    return org_data

def collect_repo_data(data, since):
    'Collect statistics on cryptocoin github accounts'
    limit_exceeded, resetAt, unsearched_set, result_set, error_log, until = (False, "", {}, {}, [], du.get_time(now=True))
    result_set["records_end_date"] = du.convert_time_format(until, dt2str=True)
    if since == None:
        repo_update_dates = get_latest_dates_of_database_update()
        result_set["records_start_date"] = since
    else:
        result_set["records_start_date"] = du.convert_time_format(since, dt2str=True)

    #Get data on accounts with more than one repo
    for alias, org in data.items():
        if alias == "records_start_date" or alias == "records_end_date": continue
        org_name, org_alias = org["login"], alias
        repos_to_search, repos, org_stats, org_errors = ([], org['repositories']['edges'], {org_name: {}}, {org_name: {}})
        for repo in repos:
            updated_at = du.convert_time_format(repo['updatedAt'], str2dt=True)
            try:
                repo['defaultBranchRef']['name']
            except:
                continue
            if since == None:
                if (repo['owner']['login'], repo['name']) in repo_update_dates:
                    last_repo_update = pytz.utc.localize(repo_update_dates[(repo['owner']['login'], repo['name'])])
                    print(last_repo_update)
                    print type(last_repo_update)
                    print(updated_at)
                    print type(updated_at)
                    if updated_at > last_repo_update:
                        repos_to_search.append(repo)
                else:
                        repos_to_search.append(repo)
            else:
                if updated_at > since:
                    repos_to_search.append(repo)
        if not repos_to_search:
            continue

        if limit_exceeded == True and len(repos_to_search) > 0:
            unsearched_set[org_alias] = {"repositories":{"edges":repos_to_search}, "login":org_name}
        else:
            idx = 0
            for repo in repos_to_search:
                parent_org_name, repo_name = (repo['owner']['login'], repo['name'])
                default_branch = repo['defaultBranchRef']['name']
                query, repo_alias = gq.repo_query(parent_org_name, repo_name, 50, 50, 50, 50, 50, full_query=True, create_alias=True, default_branch=default_branch)
                status, result = try_gql_query(query, limitcheck=True)
                if status == "success":
                    try:
                        if since == None:
                            if (parent_org_name, repo_name) in repo_update_dates:
                                last_updated_date = pytz.utc.localize(repo_update_dates[(parent_org_name, repo_name)])
                                result = page_backwards(result, parent_org_name, repo_name, last_updated_date, 50)
                            else:
                                result = page_backwards(result, parent_org_name, repo_name, "2007-10-01T00:00:00Z", 50)
                        else:
                            result = page_backwards(result, parent_org_name, repo_name, since, 50)
                        org_stats[org_name].update(result['data'])
                    except LimitExceededDuringPaginationError as e:
                        logging.exception("Limit Exceeded Error, github account: %s - repo %s ",
                                          parent_org_name, repo_name)
                        remaining_repos = [repos_to_search[x] for x in range(idx, len(repos_to_search))]
                        resetAt, limit_exceeded = (e.resetAt, True)
                        unsearched_set[org_alias] = {"repositories":{"edges":remaining_repos}, "login":org_name}
                    except GithubPaginationError as e:
                        logging.exception("Pagination Error github account: %s - repo: %s ", parent_org_name, repo_name)
                        org_errors[org_name].update({repo_name:{"data":e.data, "error_message":e.message}})
                        continue
                    except Exception as e:
                        logging.exception("Exception encountered while paginating data")
                        org_errors[org_name].update({repo_name: {"data": e, "error_message": e.message}})
                        continue
                elif status == "limit_exceeded":
                    remaining_repos = [repos_to_search[x] for x in range(idx, len(repos_to_search))]
                    unsearched_set[org_alias] = {"repositories": {"edges": remaining_repos}, "login":org_name}
                    resetAt = result[3]
                    limit_exceeded = True
                    break
                elif status == "error":
                    logging.warn("Received error in graphql call for github account: %s - repo: %s with data",
                                 org_name, repo_name, result)
                    org_errors[org_name].update({repo_name:{"data":result, "error_message":result[1]}})
                idx += 1
        if bool(org_errors[org_name]):
            error_log.append(org_errors)
        if bool(org_stats[org_name]):
            result_set.update(org_stats)
        if len(result_set) > 5 and limit_exceeded == False:
            error_processor(collect_repo_data.__name__, error_log)
            yield result_set
            del result_set
            result_set = {}
            if since == None:
                result_set["records_start_date"] = None
            else:
                result_set["records_start_date"] = du.convert_time_format(since, dt2str=True)
            result_set["records_end_date"] = du.convert_time_format(until, dt2str=True)
    if limit_exceeded == True:
        logging.warn("Limit exceeded, exiting api data collection, api resets at %s", resetAt)
        remaining_query_dump(since, unsearched_set)
        error_processor(collect_repo_data.__name__, error_log)
        yield result_set
    else:
        error_processor(collect_repo_data.__name__, error_log)
        yield result_set
