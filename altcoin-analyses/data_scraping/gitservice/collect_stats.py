from datetime import datetime

import pandas as pd
from dateutil import parser

import git_queries as gq
import log_service.logger_factory as lf
from api_communication import try_gql_query, get_gsheet_data
from custom_utils import datautils as du
from custom_utils.errors.github_errors import GithubPaginationError, LimitExceededDuringPaginationError, \
    GithubAPIBadQueryError
from data_writing import remaining_query_dump, error_processor

###Get logger
logging = lf.get_loggly_logger(__name__)

###Analytics data collection methods
def get_repo_lists(single_only=False, multiple_only=False):
    'get list of coin github accounts, sort by single repo or multiple repo accounts'
    repo_list = get_gsheet_data("https://docs.google.com/spreadsheet/ccc?key=1la_UVV4c3YLnvTesLTEq-F1wxL9Rtd2sZkdyq9OBMB4&output=csv")
    repo_list = repo_list[~repo_list.duplicated("owner") & pd.notnull(repo_list["owner"])]
    if single_only == True: return repo_list[repo_list["single_repo"] == True]
    if multiple_only == True: return repo_list[repo_list["single_repo"] == False]
    return repo_list

def paginate_repos(input_data, org_list, depth=0):
    'Use graphql to parse the repos within each organization'
    query, cursors = "", {}
    orgs_to_paginate = [(name, data) for name, data in input_data['data'].items()
                        if len(data['repositories']['edges']) == 100]
    if not orgs_to_paginate: return input_data

    #Build query graphql query to paginate orgs
    for org in orgs_to_paginate:
        name, org_data = org
        name = name.replace("_", "-")
        cursor = org_data['repositories']['edges'][99]['cursor']
        cursors[name] = cursor
        account_type = org_list[org_list["owner"] == name.replace("_","-")].iloc[0]['account_type']
        query += gq.single_org_query(name,account_type,limit=100, completequery=False,pagination=True, cursor=cursor)
    query = "query { " + query + " }"

    #Send query, recurse
    status, result = try_gql_query(query)
    if status == "success":
        paginated_result = paginate_repos(result, org_list, depth = depth + 1)
        for org in orgs_to_paginate:
            name, org_data = org
            input_data['data'][name]['repositories']['edges'] = input_data['data'][name]['repositories']['edges'] + \
                                                                paginated_result['data'][name]['repositories']['edges']
        return input_data
    else:
        logging.warn("repo pagination failed with cursors: %s and result: %s", cursors, result)
        raise GithubPaginationError(result[0], result[1], result, query, cursors,depth)

def subdivide_queries(multi_repo_list, recs_per_call=20):
    'Subdivide queries sent to github API to avoid rate limiting'
    querylist = []
    rec_len = len(multi_repo_list)
    remainder = rec_len % recs_per_call
    if remainder == rec_len or rec_len == recs_per_call:
        num_calls = 1
    else:
        num_calls = rec_len / recs_per_call
    logging.debug("%s queries are divided into %s partitions with %s recs per call", rec_len, num_calls, recs_per_call)

    for i in range(1, num_calls + 1):
        sublist = multi_repo_list[((i - 1) * recs_per_call):(i * recs_per_call)]
        querylist.append(gq.multiple_org_query(sublist, limit = 100))
        if (i == num_calls and remainder > 0):
            sublist = multi_repo_list[(i * recs_per_call):((i * recs_per_call) + remainder)]
            querylist.append(gq.multiple_org_query(sublist, limit = 100))
    return querylist

def get_org_repos():
    'Get all github repositories under a coin github account'
    multiple_repo_list, single_repo_list = get_repo_lists(multiple_only=True), get_repo_lists(single_only=True)
    repo_data, multi_querylist, repo_count = {}, subdivide_queries(multiple_repo_list), 0

    #Get all repos from coin githubs from github api with more than 1 repo
    for query in multi_querylist:
        status, result = try_gql_query(query)
        if status == "success":
            result = paginate_repos(result, multiple_repo_list)
            repo_data.update(result['data'])
        else:
            logging.error("Bad API Query result: %s - HTTP status code: %s",
                          result, status, exc_info=True)
            raise GithubAPIBadQueryError(result, query, status)

    #Associate repos with their coin symbol
    for alias, org in repo_data.items():
        owner, multiple_repo_list["owner"] = org["login"].lower(), multiple_repo_list["owner"].str.lower()
        coin_name = multiple_repo_list[multiple_repo_list["owner"] == owner].iloc[0]["id"]
        coin_symbol = multiple_repo_list[multiple_repo_list["owner"] == owner].index[0]
        repo_data[alias]["coin_name"], repo_data[alias]["coin_symbol"] = coin_name, coin_symbol

    #Associate coins with only one repo with their coin symbols
    #Integrate them into the master list of repos
    for k, v in single_repo_list.iterrows():
        org,repo = v["owner"], v["main_repo"]
        query, alias = gq.simple_repo_query(org, repo, full_query=True)
        org_data = {org:{"coin_name": v["id"], "coin_symbol": k, "repositories": {"totalCount": 1, "edges":[]}, "login": v["owner"]}}
        status, result = try_gql_query(query)
        if status == "success":
            org_data[org]["repositories"]["edges"].append({"node":result['data'][alias]})
            repo_data.update(org_data)
        else:
            logging.error("Bad API Query result: %s - query: %s - HTTP status code: %s",
                          result, query, status, exc_info=True)
            raise GithubAPIBadQueryError(result, query, status)

    repo_count += sum(git_account['repositories']['totalCount'] for name, git_account in repo_data.iteritems())
    logging.info("%s github accounts collected with %s total repos", len(repo_data.keys()), repo_count)
    du.write_json("repo_list_" + str(datetime.now()).replace(" ", "_"), repo_data)
    return repo_data

def page_backwards(org_data, owner, repo, since, num):
    since = unicode(since.strftime("%Y-%m-%dT%H:%M:%SZ"))
    'If records exist prior to requested date cannot be accessed by a single query, page backwards using cursors'
    logging.debug("pagination beginning for github account: %s - repo: %s", owner, repo)
    alias, depth = gq.generate_alias(repo), 0
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

def collect_repo_data(since=None, existing_data=None):
    'Collect statistics on cryptocoin github accounts'
    logging.info("Beggining collection of stats on repos in Github Accounts from Github GraphQL API")
    if existing_data:
        since = existing_data["records_start_date"]
        repo_data = existing_data
        logging.info("existing data being used, starting from date %s, # orgs %s, # repos %s, orgs being updated %s",
                     since, len(repo_data.keys()), sum(len(v.keys()) for k, v in repo_data),repo_data.keys())
    else:
        repo_data = get_org_repos()
        if since == None:
            logging.error("Start date not specified in repo stat collection, error will be thrown")
            raise ValueError("Start date not specified for logging, must specify start date")
    until = du.get_time(now=True, utc_string=True)
    result_set = {}
    limit_exceeded, resetAt, unsearched_set, error_log = (False, "", {}, [])
    result_set["records_start_date"], result_set["records_end_date"], = since, until
    since, until = du.convert_time_format(since, str2dt=True), du.convert_time_format(until, str2dt=True)

    #Get data on accounts with more than one repo
    for alias, org in repo_data.iteritems():
        if alias == "records_start_date" or alias == "records_end_date": continue
        org_name = org["login"]
        org_alias = alias
        repos_to_search, repos, org_stats, org_errors = ([], org['repositories']['edges'], {org_name: {}}, {org_name: {}})
        for repo in repos:
            updated_at = parser.parse(repo['node']['updatedAt'])
            try:
                repo['node']['defaultBranchRef']['name']
            except:
                continue
            if updated_at > since:
                repos_to_search.append(repo)
        if not repos_to_search:
            continue
        if limit_exceeded and repos_to_search:
            unsearched_set[org_alias] = {"repositories":{"edges":repos_to_search}, "login":org_name}
        else:
            idx = 0
            for repo in repos_to_search:
                parent_org_name, repo_name = (repo['node']['owner']['login'], repo['node']['name'])
                default_branch = repo['node']['defaultBranchRef']['name']
                logging.debug("acquiring data for github account: %s , repo: %s , default branch is %s",
                              parent_org_name, repo_name, default_branch)
                query, repo_alias = gq.repo_query(parent_org_name, repo_name, 50, 50, 50, 50, 50, full_query=True, create_alias=True, default_branch=default_branch)

                status, result = try_gql_query(query, limitcheck=True)
                if status == "success":
                    try:
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
                        break
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

    if limit_exceeded == True:
        logging.warn("Limit exceeded, exiting api data collection, unsearched data is %s, errors are %s query was searching from %s reset at %s",
                     unsearched_set, error_log, since, resetAt)
        remaining_query_dump(since, unsearched_set)
        error_processor(collect_repo_data.__name__, error_log)
        du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
        return result_set, error_log

    try:
        num_repos = sum(len(git_account.keys()) for name, git_account in result_set.iteritems() if not isinstance(git_account, str))
        logging.info("repo activity data collection from API finished, errors are %s", error_log)
        logging.info("number of git accounts returning data is %s , number of repos is %s number of git accounts with errors is %s",
            len(result_set.keys()), num_repos, len(error_log))
    except:
        pass
    error_processor(collect_repo_data.__name__, error_log)
    du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
    return result_set, error_log, since