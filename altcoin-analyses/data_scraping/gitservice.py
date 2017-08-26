import json
import pandas as pd
from dateutil import parser
from datetime import datetime
from data_scraping import scrapingutils as su
from data_scraping import datautils as du
from requests import HTTPError

###Custom Errors
class GithubAPIError(Exception):
    'If a misc Github API error is reached pass error'
    def __init__(self, data, msg=None):
        if msg is None:
            # Default Message
            msg = "Github API Error!"
        super(GithubAPIError, self).__init__(msg)
        self.data = data

class GithubPaginationError(GithubAPIError):
    'If pagination of Github records backwards in time fails throw error'
    def __init__(self, result, query, org_data, cursors, depth, msg=None):
        if msg is None:
            msg = "Back pagination error at depth " + str(depth)
        data = [result, query, org_data, cursors, depth, msg]
        super(GithubAPIError, self).__init__(data, msg)
        self.query = query
        self.result = result
        self.org_data = org_data
        self.cursors = cursors
        self.depth = depth
        self.data = data

class GithubAPILimitExceededError(GithubAPIError):
    'If Github API Limit Exceed pass error'
    def __init__(self, resetAt, remaining, cost, msg=None):
        data = [resetAt, remaining, cost]
        if msg is None:
            msg = "cost of query was %s, remaining hourly rate limit is %s, limit exceeded, reset at %s" % (cost, remaining, resetAt)
        super(GithubAPILimitExceededError, self).__init__(data, msg)
        self.resetAt = resetAt
        self.remaining = remaining
        self.cost = cost

class GithubAPIBadQueryError(GithubAPIError):
    'Throw error if query returns malformed result'
    def __init__(self, result, query, msg=None):
        data = [result, query]
        if msg is None:
            msg = "Query returned bad result"
        super(GithubAPIError, self).__init__(data, msg)
        self.query = query
        self.result = result

###Environment Variables
#Github Secret Key (IMPORTANT: REMOVE SECRET BEFORE COMMITTING)
secret = "7c2aa86d1217d2d02ecde9311b87cbc479e14ae6"

###Data Preparation Methods (consider removing to datautils library)
def striplines(file):
    'strip lines'
    f = open(file, 'r')
    lines = f.readlines()
    result = ' '.join([line.strip() for line in lines])
    return result

def generate_alias(repo):
    alias = repo
    alias = alias.replace("-", "_")
    alias = alias.replace(".", "_")
    return alias

###Graphql query formation methods
def accounttype_query(owner, repo):
    'determine if single github account is owner or user type'
    owner_alias = "".join(owner.split("-"))
    repo_alias = "".join(repo.split("-"))
    alias = owner_alias + "_" + repo_alias
    query = str(alias) + ': repository(owner: "'+str(owner)+'", name: "'+str(repo)+'") { owner { __typename } } '
    return query

def accounttype_update(limit=10):
    'determine if single multiple github accounts are owner or user type'
    repo_list = su.read_csv("https://docs.google.com/spreadsheet/ccc?key=1la_UVV4c3YLnvTesLTEq-F1wxL9Rtd2sZkdyq9OBMB4&output=csv")
    query = ""
    for index, row in repo_list.iterrows():
        if row["repo_site"] == "github":
            if not pd.isnull(row["main_repo"]):
                query = query + accounttype_query(row["owner"], row["main_repo"])
    query = "query { " + query + " }"
    return query

def single_org_query(owner, account_type, limit=10, completequery=True):
    'Form graphql query for the repositories of a list of a signle organization or user (account type must be specified)'
    if completequery:
        query = 'query { organization(login: "'+owner+'") { repositories(first: '+str(limit)+') { edges { node { name updatedAt owner { login } forks{ totalCount } } } } } }'
    else:
        owner_alias = "".join(owner.split("-"))
        query = str(owner_alias) + ': '+str(account_type) + '(login: "' + owner + '") { repositories(first: ' + str(limit) + ') { edges { node { name updatedAt forks{ totalCount } owner { login }  } } } } '
    return query


def multiple_org_query(repo_list, limit=10):
    'Form graphql query for the repositories of a list of organizations or users (account type must be specified)'
    query = ""
    for index, row in repo_list.iterrows():
        query = query + single_org_query(row["owner"], row["account_type"], limit, False)
    query = "query { " + query + " }"
    return query


def repo_query(owner, repo, num_commits=100, num_stars=100, num_forks=100, num_issues=100, num_pulls=100, full_query = True, create_alias=False):
    'Form graphql query for a single repo'
    if full_query == True:
        if create_alias == True:
            alias = generate_alias(repo)
            query = 'query { '+ alias +': repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } ref(qualifiedName: "master") { target { ... on Commit { id history(first: '+str(num_commits)+') { edges { node { oid author { name date } } cursor } pageInfo { hasNextPage } } } } } stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt author { login } timeline(first: 12) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id mergedAt author { login } } cursor } } } }'
            return query, alias
        else:
            query = 'query { repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } ref(qualifiedName: "master") { target { ... on Commit { id history(first: '+str(num_commits)+') { edges { node { oid author { name date } } cursor } pageInfo { hasNextPage } } } } } stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt author { login } timeline(first: 12) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id mergedAt author { login } } cursor } } } }'
            return query
    else:
        alias = generate_alias(repo)
        query = alias + ': repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } ref(qualifiedName: "master") { target { ... on Commit { id history(first: '+str(num_commits)+') { edges { node { oid author { name date } } cursor } pageInfo { hasNextPage } } } } } stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt author { login } timeline(first: 12) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id mergedAt author { login } } cursor } } }'
        return query, alias

def pagination_query(owner, repo, cursors, stars=False, forks=False, commits=False, open_issues=False,
                     closed_issues=False, merged_requests=False, open_requests=False, num=100):
    'Form and execute query for pagination'
    alias = generate_alias(repo)
    query = 'query { ' + alias + ' : repository(owner:"' + owner + '", name:"' + repo + '") { name updatedAt owner{ login } '
    if stars:
        query += 'stargazers(last: ' + str(num) + ', before: "' + cursors[
            "stars"] + '", orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } '
    if forks:
        query += 'forks(last: ' + str(num) + ', before: "' + cursors[
            "forks"] + '" ) { totalCount edges { node { id createdAt owner { login } } cursor } } '
    if open_issues:
        query += 'openissues: issues(last: ' + str(num) + ', before: "' + cursors[
            "open_issues"] + '", states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } '
    if closed_issues:
        query += 'closedissues: issues(last: ' + str(num) + ', before: "' + cursors[
            "closed_issues"] + '", states: CLOSED) { totalCount edges { node { id createdAt author { login } timeline(first: 12) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } '
    if merged_requests:
        query += 'mergedrequests: pullRequests(last: ' + str(num) + ', before: "' + cursors[
            "merged_requests"] + '", states: MERGED) { totalCount edges { node { id mergedAt author { login } } cursor } } '
    if open_requests:
        query += 'openrequests: pullRequests(last: ' + str(num) + ', before: "' + cursors[
            "open_requests"] + '", states: OPEN) { totalCount edges { node { id createdAt author { login } } cursor } } '
    query += '}}'
    return query

###Data Fetching Utility Methods
def post_gql_query(gql, secret_):
    'Send call to Github API via HTTP, throw error if failure'
    if gql.endswith('.graphql'):
        query = striplines(gql)
    elif isinstance(gql, str) or isinstance(gql, unicode):
        query = gql
    else:
        raise TypeError("gql passed to post_gql_query function was " + type(gql).__name__ + ", string or .graphql required")
    token = "bearer " + secret_
    headers = {"Authorization": token}
    response = su.post_data('https://api.github.com/graphql', json.dumps({"query": query}), headers=headers, max_retries=10)
    if response.status_code == 200:
        try:
            result = response.json()
        except:
            raise (response, "json encoding failed")
        if 'errors' in result:
            print "error detected"
            raise GithubAPIBadQueryError(result, query)
        else:
            return response.status_code, result
    elif (400 <= response.status_code < 600) :
        raise response.raise_for_status()
    else:
        msg = "github API returned a non 200 SUCCESS code of %s" % response.status_code
        raise GithubAPIError(response, msg)

def try_gql_query(query, limitcheck=False, cost=100):
    'Attempt a graphql query, if it fails, return response with information about failure'
    if limitcheck:
        try:
            status, remaining, limit_exceeded, resetAt = check_limit(cost)
            if limit_exceeded:
                return "limit exceeded", (status, remaining, limit_exceeded, resetAt)
        except:
            pass
    try:
        status, result = post_gql_query(query, secret)
        return "success", result
    except HTTPError as e:
        response = e.response
        print HTTPError.__name__ + " with response of" + str(response.status_code) + " on query:"
        error_data = [e.message, response.status_code, response, query]
        return "error", error_data
    except GithubAPIBadQueryError as e:
        print GithubAPIBadQueryError.__name__ + " on query:"
        error_data = [e.message, e.result, e.query]
        return "error", error_data
    except GithubAPIError as e:
        response = e.data
        print GithubAPIError.__name__ + "with message of " + e.message + "and status code of " + response.status_code + " on query:"
        error_data = [e.message, response.status_code, response, query]
        return "error", error_data

    except TypeError as e:
        return "error", [e.message, query]

def check_limit(cost):
    'Check Github API limit, return information on remaining limits'
    cost = float(cost)
    limit_exceeded = False
    query = "query { rateLimit { remaining resetAt } }"
    status, response = post_gql_query(query, secret)
    if status == 200:
        result = response
        remaining = result['data']['rateLimit']['remaining']
        resetAt = result['data']['rateLimit']['resetAt']
        if remaining < cost:
            limit_exceeded = True
            return status, remaining, limit_exceeded, resetAt
        else:
            return status, remaining, limit_exceeded, resetAt
    elif (400 <= response.status_code < 600):
        raise response.raise_for_status()
    else:
        msg = "github API returned a non 200 SUCCESS code of %s" % response.status_code
        raise GithubAPIError(response, msg)

###Information-to-disk methods
def error_processor(calling_func_name, errors):
    'Write errors to disk'
    print "error processor fired by " + calling_func_name
    print "errors are: "
    print errors
    du.write_log(calling_func_name + "_errors_" + str(datetime.now()).replace(" ", "_"), errors)
    return

def remaining_query_dump(single_repos, resetAt, since, multi_repos=[], idx=0, single_repos_only=False):
    'If query exceeds limit, write remaining repos to search to disk'
    output = {}
    single_repos = single_repos.ix[idx:len(single_repos)]
    single_repos = single_repos[["owner", "main_repo"]].set_index("owner")
    single_remaining_repos = single_repos.to_dict()
    single_remaining_repos = single_remaining_repos["main_repo"]
    if single_repos_only == True:
        output["single_repo_orgs"] = single_remaining_repos
    else:
        output["single_repo_orgs"] = single_remaining_repos
        output.update["multi_repo_orgs"] = multi_repos
    now = du.get_time(now =True, utc_string=True)
    output["date"] = now
    output["resetAt"] = resetAt
    output["since"] = since
    du.write_json("remaining_repos_"+now, output)

###Analytics data collection methods
def get_repo_lists(single_only=False, multiple_only=False):
    'get list of coin github accounts, sort by single repo or multiple repo accounts'
    repo_list = su.read_csv("https://docs.google.com/spreadsheet/ccc?key=1la_UVV4c3YLnvTesLTEq-F1wxL9Rtd2sZkdyq9OBMB4&output=csv")
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
        querylist.append(multiple_org_query(sublist, limit = 100))
        if (i == num_calls and remainder > 0):
            sublist = multiple_repo_list[(i * recs_per_call):((i * recs_per_call) + remainder)]
            querylist.append(multiple_org_query(sublist, limit = 100))
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
    alias = generate_alias(repo)
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
        query = pagination_query(owner, repo, cursors, p_stars, p_forks, p_commits, p_open_issues,
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
    'Collect statistics on cryptocoin github accoubts'
    print(since)
    #Make sure date passed is in unicode, str, or datetime format
    if isinstance(since, str) or isinstance(since, unicode):
        since = parser.parse(since)
        print(since)
    elif isinstance(since, datetime):
        pass
    else:
        raise TypeError("date passed to get_org_statistics was " + type(since).__name__ + ", str, unicode, or datetime required")

    #Get information on github accounts, declare variables global to the function
    multi_repo_orgs = get_org_repos()
    single_repo_orgs = get_repo_lists(single_only=True)
    result_set = {}
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
                query, alias = repo_query(org, repo, 100, 100, 100, 100, 100, full_query=True, create_alias=True)
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
        du.write_json("org_statistics_" + str(datetime.now()).replace(" ", "_"), result_set)
        msg = "Github limit exceed, statistics collection exited reset at: " + resetAt
        print msg
        return result_set, error_log, msg
    #Get data on accounts with only
    idx=0
    for name, row in single_repo_orgs.iterrows():
        org = row['owner']
        repo = row['main_repo']
        query, alias = repo_query(org, repo, 100, 100, 100, 100, 100, create_alias=True)
        print "graphql call fired"
        status, content = try_gql_query(query)
        print status
        if status == "success":
            updated_at = parser.parse(content['data'][alias]['updatedAt'])
            if updated_at > since:
                try:
                    content = page_backwards(content, org, repo, unicode(since.strftime("%Y-%m-%dT%H:%M:%SZ")))
                    print(content)
                    result_set.update(content['data'])
                except GithubPaginationError as e:
                    error_log.append({org: {repo: content}})
                except GithubAPILimitExceededError as e:
                    remaining_query_dump(single_repo_orgs, e.resetAt, since, idx=idx, single_repos_only=True)
                    error_processor(get_org_statistics.__name__, error_log)
                    du.write_json("org_statistics_" + str(datetime.now()).replace(" ", "_"), result_set)
                    msg = "Github limit exceed, statistics collection exited at {" + org + ":" + repo + " }, reset at: " + e.resetAt
                    print msg
                    return msg
                result_set.update(content['data'])
        elif status == "limit_exceeded":
            remaining_query_dump(single_repo_orgs, content[3], since, idx=idx, single_repos_only=True)
            error_processor(get_org_statistics.__name__, error_log)
            du.write_json("org_statistics_" + str(datetime.now()).replace(" ", "_"), result_set)
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
        du.write_json("org_statistics_" + str(datetime.now()).replace(" ","_"), result_set)
    except:
        pass
    return result_set, error_log, ""

#Data Processing & Writing Methods
def repo_totals(data):
    data = {}
    try:
        data["closed_issues"] = git['data']['repository']['closedissues']['totalCount']
        data["open_issues"] = git['data']['repository']['openissues']['totalCount']
        data["total_issues"] = data["closed_issues"] + data["open_issues"]
        data["merged_pull_requests"] = git['data']['repository']['mergedrequests']['totalCount']
        data["merged_pull_requests"] = git['data']['repository']['mergedrequests']['totalCount']
        data["stars"] = git['data']['repository']['stargazers']['totalCount']
        data["forks"] = git['data']['repository']['forks']['totalCount']
    except:
        print 'github graphql response malformed'
    return data

def monthly_totals(git):
    return

def update_dev_data(newdata, olddata={}):
    data = olddata
    repo_owner = newdata['data']['repository']['owner']['login']
    repo_name = newdata['data']['repository']['name']
    coin_symbol = getsymbol(repo_owner, repo_name)
    if bool(olddata) == False:
        for edge in newdata['data']['repository']["mergedrequests"]["edges"]:
            id = edge["node"]["id"]
            createdat = edge['node']['createdAt']
            author = edge['node']['author']['login']
            data[id] = {}