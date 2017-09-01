import pandas as pd
import git_queries as gq
from dateutil import parser
from datetime import datetime
from data_scraping import datautils as du
from requests import HTTPError
from custom_errors.github_errors import GithubAPILimitExceededError, GithubPaginationError, GithubAPIBadQueryError, GithubAPIError
from data_writing import remaining_query_dump, error_processor
from api_communication import try_gql_query, check_limit, get_gsheet_data

###Analytics data collection methods
def get_repo_lists(single_only=False, multiple_only=False):
    'get list of coin github accounts, sort by single repo or multiple repo accounts'
    repo_list = get_gsheet_data("https://docs.google.com/spreadsheet/ccc?key=1la_UVV4c3YLnvTesLTEq-F1wxL9Rtd2sZkdyq9OBMB4&output=csv")
    single_repo_list = repo_list[repo_list["single_repo"] == True]
    if single_only == True:
        return single_repo_list
    repo_list = repo_list[~repo_list.duplicated("owner") & pd.notnull(repo_list["owner"])]
    multiple_repo_list = repo_list[repo_list["single_repo"]==False]
    if multiple_only == True:
        return multiple_repo_list
    return multiple_repo_list, single_repo_list

def get_org_repos(recs_per_call = 20):
    'Get all github repositories under a coin github account'
    repo_data = {}
    error_log = []
    querylist = []
    multiple_repo_list = get_repo_lists(multiple_only=True)
    #Check to see if the github API rate limit has been reached, if so, raise exception
    rec_len = len(multiple_repo_list)
    cost = (float(rec_len)*102.)/100.
    try:
        status, remaining, limit_exceeded, resetAt = check_limit(cost)
        if limit_exceeded:
            raise GithubAPILimitExceededError(resetAt, remaining, cost)
    except HTTPError:
        pass
    else:
        pass
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
            repo_data.update(result['data'])
        else:
            error_log.append([query,result])
    try:
        error_processor(get_org_repos.__name__, error_log)
    except:
        pass
    try:
        du.write_json("multi_repo_list_" + str(datetime.now()).replace(" ", "_"), repo_data)
    except:
        pass
    return repo_data

def page_backwards(org_data, owner, repo, since, stars=True, forks=True, commits = False, open_issues=True, closed_issues=True, open_requests=True, merged_requests=True, num=100, depth = 0):
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

def get_org_statistics(since=du.get_time(now=False, months=1, utc_string=True, give_unicode=True)):
    'Collect statistics on cryptocoin github accounts'
    #Make sure date passed is in unicode, str, or datetime format
    if isinstance(since, str) or isinstance(since, unicode):
        since = parser.parse(since)
        print(since)
    elif isinstance(since, datetime):
        pass
    else:
        raise TypeError("date passed to get_org_statistics was " + type(since).__name__ + ", str, unicode, or datetime required")

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
                query, alias = gq.repo_query(org, repo, 100, 100, 100, 100, 100, full_query=True, create_alias=True)
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
        error_processor(get_org_statistics.__name__, error_log)
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
                    error_processor(get_org_statistics.__name__, error_log)
                    du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
                    msg = "Github limit exceed, statistics collection exited at {" + org + ":" + repo + " }, reset at: " + e.resetAt
                    print msg
                    return msg
        elif status == "limit_exceeded":
            remaining_query_dump(single_repo_orgs, content[3], since, idx=idx, single_repos_only=True)
            error_processor(get_org_statistics.__name__, error_log)
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
        error_processor(get_org_statistics.__name__ ,error_log)
    except:
        pass
    try:
        du.write_json("org_statistics_" + du.get_time(now=True, utc_string=True), result_set)
    except:
        pass
    return result_set, error_log

#Data Processing Methods
def collect_aggregate_totals(org, since, org_name):
    org_totals = {}
    org_totals["stargazers"], org_totals["forks"], org_totals["open_issues"], org_totals["closed_issues"] = 0,0,0,0
    org_totals["total_issues"], org_totals["open_pull_requests"], org_totals["merged_pull_requests"] = 0,0,0
    org_totals["since"], org_totals["name"] = since, org_name
    for name, repo in org.iteritems():
        org_totals["stargazers"] += repo['stargazers']['totalCount']
        org_totals["forks"] += repo['forks']['totalCount']
        org_totals["open_issues"] += repo['openissues']['totalCount']
        org_totals["closed_issues"] += repo['closedissues']['totalCount']
        org_totals["total_issues"] += repo["closedissues"]['totalCount'] + repo["openissues"]['totalCount']
        org_totals["open_pull_requests"] += repo['openrequests']['totalCount']
        org_totals["merged_pull_requests"] += repo['mergedrequests']['totalCount']
    result = (org_totals["name"], org_totals["since"], org_totals["stargazers"], org_totals["forks"],
              org_totals["open_issues"], org_totals["closed_issues"], org_totals["total_issues"],
              org_totals["open_pull_requests"], org_totals["merged_pull_requests"])
    return result

def stage_data(org_data):
    stars, forks, openissues, closedissues, openrequests, mergedrequests, org_statistics  = [],[],[],[],[],[],[]
    for org_name, org in org_data.iteritems():
        if org_name == "records_end_date" or org_name == "records_start_date":
            continue
        print org_name
        org_statistics.append(collect_aggregate_totals(org, org_name, org_data["records_end_date"]))
        for repo_name, repo in org.iteritems():
            print repo_name
            if repo["stargazers"]["edges"]:
                entry = [(edge["node"]["id"],repo["owner"]["login"],repo["name"], edge["node"]["login"],
                          edge["starredAt"],edge["cursor"]) for edge in repo["stargazers"]["edges"]]
                stars += entry
            if repo["forks"]["edges"]:
                entry = [(edge["node"]["id"], repo["owner"]["login"],repo["name"], edge["node"]["owner"]["login"],
                          edge["node"]["createdAt"], edge["cursor"]) for edge in repo["forks"]["edges"]]
                forks += entry
            if repo["openissues"]["edges"]:
                for edge in repo["closedissues"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                             edge["node"]["createdAt"], edge["node"]["state"], "", "", edge["cursor"])
                    openissues.append(entry)
            if repo["closedissues"]["edges"]:
                for edge in repo["closedissues"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    closedevents = [(event["createdAt"], event["actor"]["login"]) if bool(event["actor"]) else
                                    (event["createdAt"], "") for event in edge["node"]["timeline"]["nodes"] if bool(event)]
                    print closedevents
                    if closedevents:
                        closedevent = max(closedevents)
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], closedevent[1], closedevent[0], edge["cursor"])
                    else:
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], "", "", edge["cursor"])
                    closedissues.append(entry)
            if repo["openrequests"]["edges"]:
                for edge in repo["openrequests"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                              edge["node"]["createdAt"], edge["node"]["state"],"", edge["cursor"])
                    openrequests.append(entry)
            if repo["mergedrequests"]["edges"]:
                for edge in repo["mergedrequests"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                             edge["node"]["state"], edge["node"]["mergedAt"], edge["cursor"])
                    mergedrequests.append(entry)
    return stars, forks, openissues, closedissues, openrequests, mergedrequests, org_statistics