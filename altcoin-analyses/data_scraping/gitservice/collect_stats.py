import pandas as pd
import git_queries as gq
from dateutil import parser
from datetime import datetime
from data_scraping import datautils as du
from custom_errors.github_errors import GithubAPILimitExceededError, GithubPaginationError, GithubAPIBadQueryError, GithubAPIError
from data_writing import remaining_query_dump, error_processor
from api_communication import try_gql_query, check_limit, get_gsheet_data

###Analytics data collection methods
def get_repo_lists(single_only=False, multiple_only=False):
    'get list of coin github accounts, sort by single repo or multiple repo accounts'
    repo_list = get_gsheet_data("https://docs.google.com/spreadsheet/ccc?key=1la_UVV4c3YLnvTesLTEq-F1wxL9Rtd2sZkdyq9OBMB4&output=csv")
    repo_list = repo_list[~repo_list.duplicated("owner") & pd.notnull(repo_list["owner"])]
    if single_only == True:
        single_repo_list = repo_list[repo_list["single_repo"] == True]
        return single_repo_list

    if multiple_only == True:
        multiple_repo_list = repo_list[repo_list["single_repo"] == False]
        return multiple_repo_list
    return repo_list

def paginate_repos(input_data, org_list, error_log):
    orgs_to_paginate = [(name, data) for name, data in input_data['data'].iteritems() if len(data['repositories']['edges']) == 100]
    if not orgs_to_paginate:
        return input_data
    query = ""
    for org in orgs_to_paginate:
        name, org_data = org
        cursor = org_data['repositories']['edges'][99]['cursor']
        account_type = org_list[org_list["owner"] == name].iloc[0]['account_type']
        query += gq.single_org_query(name,account_type,limit=100, completequery=False,pagination=True, cursor=cursor)
    query = "query { " + query + " }"
    status, result = try_gql_query(query)
    if status == "success":
        paginated_result = paginate_repos(result, org_list, error_log)
        for org in orgs_to_paginate:
            name, org_data = org
            input_data['data'][name]['repositories']['edges'] = input_data['data'][name]['repositories']['edges'] + paginated_result['data'][name]['repositories']['edges']
        return input_data
    else:
        error_log.append([query, result])
        return input_data

def get_org_repos(recs_per_call = 20, get_org_stats=False):
    'Get all github repositories under a coin github account'
    repo_data = {}
    error_log = []
    querylist = []
    multiple_repo_list = get_repo_lists(multiple_only=True)
    #Check to see if the github API rate limit has been reached, if so, raise exception
    rec_len = len(multiple_repo_list)
    ###Send query to github API to return all repos under each coin's github account
    #Determine subdivision of records in query
    remainder = rec_len % recs_per_call
    if remainder == rec_len or rec_len == recs_per_call:
        num_calls = 1
    else:
        num_calls = rec_len / recs_per_call
    #Cycle through the list of repositories at the amount specified by recs_per_call
    for i in range(1, num_calls + 1):
        sublist = multiple_repo_list[((i - 1) * recs_per_call):(i * recs_per_call)]
        querylist.append(gq.multiple_org_query(sublist, limit = 100))
        if (i == num_calls and remainder > 0):
            sublist = multiple_repo_list[(i * recs_per_call):((i * recs_per_call) + remainder)]
            querylist.append(gq.multiple_org_query(sublist, limit = 100))
    for query in querylist:
        status, result = try_gql_query(query)
        if status == "success":
            result = paginate_repos(result, multiple_repo_list, error_log)
            repo_data.update(result['data'])
        else:
            error_log.append([query,result])
    if get_org_stats:
        for alias, org in repo_data.iteritems():
            print ("alias:",alias)
            owner = org["login"].lower()
            print(owner)
            multiple_repo_list["owner"] = multiple_repo_list["owner"].str.lower()
            coin_name = multiple_repo_list[multiple_repo_list["owner"] == owner].iloc[0]["id"]
            coin_symbol = multiple_repo_list[multiple_repo_list["owner"] == owner].index[0]
            repo_data[alias]["coin_name"] = coin_name
            repo_data[alias]["coin_symbol"] = coin_symbol
        single_repo_list = get_repo_lists(single_only=True)
        for k, v in single_repo_list.iterrows():
            repo_data.update({v["owner"]:{"coin_name":v["id"], "coin_symbol":k,"repositories":{"totalCount":1},"login":v["owner"]}})
    try:
        error_processor(get_org_repos.__name__, error_log)
    except:
        pass
    try:
        du.write_json("multi_repo_list_" + str(datetime.now()).replace(" ", "_"), repo_data)
    except:
        pass
    return repo_data

def page_backwards(org_data, owner, repo, since, stars=True, forks=True, commits = False, open_issues=True, closed_issues=True, open_requests=True, merged_requests=True, num=50, depth = 0):
    'If records exist prior to requested date cannot be accessed by a single query, use cursors to retrieve earlier records'
    print ("onwner %s repo %s depth %s since %s") %(owner, repo, depth, since)
    print(since)
    print(depth)
    alias = gq.generate_alias(repo)
    cursors = {}
    p_stars, p_forks, p_open_issues, p_closed_issues, p_open_requests, p_merged_requests, p_commits = (False, False, False, False, False, False, False)
    if stars and len(org_data["data"][alias]["stargazers"]["edges"]) >= num:
        first_star = org_data["data"][alias]["stargazers"]["edges"][0]["starredAt"]
        print "first star at %s" %(first_star)
        if first_star > since:
            cursors["stars"] = org_data["data"][alias]["stargazers"]["edges"][0]["cursor"]
            p_stars = True
    if forks and len(org_data["data"][alias]["forks"]["edges"]) >= num:
        first_fork = org_data["data"][alias]["forks"]["edges"][0]["node"]["createdAt"]
        if first_fork > since:
            cursors["forks"] = org_data["data"][alias]["forks"]["edges"][0]["cursor"]
            p_forks = True
    if open_issues and len(org_data["data"][alias]["openissues"]["edges"]) >= num:
        first_open_issue = org_data["data"][alias]["openissues"]["edges"][0]["node"]["createdAt"]
        if first_open_issue > since:
            cursors["open_issues"] = org_data["data"][alias]["openissues"]["edges"][0]["cursor"]
            p_open_issues = True
    if closed_issues and len(org_data["data"][alias]["closedissues"]["edges"]) >= num:
        closed_dates = []
        timelines = [edge['node']['timeline']['nodes'] for edge in org_data["data"][alias]["closedissues"]["edges"]]
        for timeline in timelines:
            closed_date = [event for event in timeline if bool(event)]
            if closed_date:
                closed_dates.append(closed_date[0]['createdAt'])
            if closed_dates and max(closed_dates) > since:
                cursors["closed_issues"] = org_data["data"][alias]["closedissues"]["edges"][0]["cursor"]
                p_closed_issues = True
    if open_requests and len(org_data["data"][alias]["openrequests"]["edges"]) >= num:
        first_open_request = org_data["data"][alias]["openrequests"]["edges"][0]["node"]["createdAt"]
        if first_open_request > since:
            cursors["open_issues"] = org_data["data"][alias]["openrequests"]["edges"][0]["cursor"]
            p_open_issues = True
    if merged_requests and len(org_data["data"][alias]["mergedrequests"]["edges"]) >= num:
        latest_merge_date = max([edge['node']['mergedAt'] for edge in org_data["data"][alias]["mergedrequests"]["edges"]])
        if latest_merge_date > since:
            cursors["merged_requests"] = org_data["data"][alias]["mergedrequests"]["edges"][0]["cursor"]
            p_merged_requests = True
    if not any([p_stars, p_forks, p_open_issues, p_closed_issues, p_open_requests, p_merged_requests, p_commits]):
        return org_data
    else:
        query = gq.pagination_query(owner, repo, cursors, p_stars, p_forks, p_commits, p_open_issues,
                     p_closed_issues, p_merged_requests, p_open_requests, num)
        truth_table = [p_stars, p_forks, p_open_issues, p_closed_issues, p_open_requests, p_merged_requests]
        print ("owner: " + owner + "repo: " + repo)
        print truth_table
        status, paged_result = try_gql_query(query, limitcheck=True)
        if status == "success":
            paged_stats = page_backwards(paged_result, owner, repo, since, p_stars, p_forks, p_commits, p_open_issues, p_closed_issues, p_open_requests, p_merged_requests, depth=depth + 1)
            attributes = ["stargazers", "forks", "openissues", "closedissues", "openrequests", "mergedrequests"]
            for i,attribute in enumerate(attributes):
                if truth_table[i] == True:
                    org_data["data"][alias][attribute]["edges"] = paged_stats["data"][alias][attribute]["edges"] + org_data["data"][alias][attribute]["edges"]
            return org_data
        else:
            print('pagination error with result:')
            print(paged_result)
            print('query was: ' + query)
            print('cursors were: %s') %(cursors)
            print('depth was: ' + str(depth))
            raise GithubPaginationError(paged_result, query, org_data, cursors, depth)

def collect_repo_data(since=du.get_time(now=False, months=1, utc_string=True, give_unicode=True)):
    'Collect statistics on cryptocoin github accounts'
    #Make sure date passed is in unicode, str, or datetime format
    if isinstance(since, str) or isinstance(since, unicode):
        since = parser.parse(since)
        print(since)
    elif isinstance(since, datetime):
        pass
    else:
        raise TypeError("date passed to collect_repo_data was " + type(since).__name__ + ", str, unicode, or datetime required")

    #Get information on github accounts, declare variables global to the function
    result_set = {}
    result_set["records_end_date"] = du.get_time(now=True, utc_string=True)
    result_set["records_start_date"] = du.dt_tostring(since)
    multi_repo_orgs = get_org_repos()
    single_repo_orgs = get_repo_lists(single_only=True)
    limit_exceeded = False
    resetAt = ""
    unsearched_set = []
    error_log = []
    ###Get data on accounts with more than one repo
    for name, org in multi_repo_orgs.iteritems():
        repos_to_search = []
        repos = org['repositories']['edges']
        org_stats = {name: {}}
        org_errors = {name: {}}
        for repo in repos:
            updated_at = parser.parse(repo['node']['updatedAt'])
            if updated_at > since:
                repos_to_search.append(repo)
        if not repos_to_search:
            continue
        if limit_exceeded and repos_to_search:
            unsearched_set.append({org:repos_to_search})
        else:
            idx = 0
            for repo in repos_to_search:
                org = repo['node']['owner']['login']
                repo = repo['node']['name']
                query, alias = gq.repo_query(org, repo, 50, 50, 50, 50, 50, full_query=True, create_alias=True)
                status, result = try_gql_query(query, limitcheck=True)
                if status == "success":
                    try:
                        result = page_backwards(result, org, repo, unicode(since.strftime("%Y-%m-%dT%H:%M:%SZ")))
                        org_stats[name].update(result['data'])
                    except GithubPaginationError as e:
                        org_errors[name].update({repo:e.data})
                    except GithubAPILimitExceededError as e:
                        remaining_repos = [repos_to_search[x] for x in range(idx, len(repos_to_search))]
                        resetAt = e.resetAt
                        unsearched_set.append({org:remaining_repos})
                        limit_exceeded = True
                        break
                elif status == "limit_exceeded":
                    remaining_repos = [repos_to_search[x] for x in range(idx, len(repos_to_search))]
                    unsearched_set.append({org: remaining_repos})
                    resetAt = result[3]
                    limit_exceeded = True
                    break
                else:
                    print "appending error log"
                    org_errors[name].update({repo:result})
                idx += 1
        if bool(org_errors[name]):
            error_log.append(org_errors)
        if bool(org_stats[name]):
            result_set.update(org_stats)
    if limit_exceeded == True:
        remaining_query_dump(single_repo_orgs, resetAt, since, unsearched_set)
        error_processor(collect_repo_data.__name__, error_log)
        du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
        msg = "Github limit exceed, statistics collection exited reset at: " + resetAt
        print msg
        return result_set, error_log
    #Get data on accounts with only
    idx=0
    for name, row in single_repo_orgs.iterrows():
        org = row['owner']
        repo = row['main_repo']
        query, alias = gq.repo_query(org, repo, 100, 100, 100, 100, 100, create_alias=True)
        print "graphql call fired"
        status, content = try_gql_query(query)
        print status
        if status == "success":
            updated_at = parser.parse(content['data'][alias]['updatedAt'])
            if updated_at > since:
                try:
                    content = page_backwards(content, org, repo, unicode(since.strftime("%Y-%m-%dT%H:%M:%SZ")))
                    print(content)
                    result_set.update({org:content['data']})
                except GithubPaginationError as e:
                    error_log.append({org:content})
                except GithubAPILimitExceededError as e:
                    remaining_query_dump(single_repo_orgs, e.resetAt, since, idx=idx, single_repos_only=True)
                    error_processor(collect_repo_data.__name__, error_log)
                    du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
                    msg = "Github limit exceed, statistics collection exited at {" + org + ":" + repo + " }, reset at: " + e.resetAt
                    print msg
                    return msg
        elif status == "limit_exceeded":
            remaining_query_dump(single_repo_orgs, content[3], since, idx=idx, single_repos_only=True)
            error_processor(collect_repo_data.__name__, error_log)
            du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
            msg = "Github limit exceed, statistics collection exited at {" + org + ":" + repo + " }, reset at: " + content[3]
            print msg
            return msg
        else:
            print "appending error log"
            error_log.append({org:{repo:content}})
        idx += 1
    try:
        print "error processor attempted"
        error_processor(collect_repo_data.__name__, error_log)
    except:
        pass
    try:
        du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
    except:
        pass
    return result_set, error_log