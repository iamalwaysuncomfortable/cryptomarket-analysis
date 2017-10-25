from data_scraping.gitservice.db_interface import get_github_analytics_data
from data_scraping.datautils import get_time
import pandas as pd
from data_scraping.datautils import convert_time_format

import log_service.logger_factory as lf

##Setup Logger
logging = lf.get_loggly_logger(__name__)

###METHODS FOR FETCHING AND PROCESSESING DATA FROM THE DATABASE
def count_github_org_stats(data, start=None, end=None):
    #Initialize Data]
    if start:
        start = convert_time_format(start, dt2str=True)
    if end:
        end = convert_time_format(end, dt2str=True)
    stats = {}

    #Internal methods
    def get_unique_orgs(input_data, index):
        return set(e[index] for e in input_data)

    def get_org_repos(input_data, org, org_index, repo_index):
        return set(e[repo_index] for e in input_data if e[org_index] == org)

    def init_repo(org, repo):
        stats[org]["repos"][repo] = {"stars": 0, "forks": 0, "open_issues": 0, "issue_openers":0, "closed_issues": 0,
                                     "issue_closers":0,"open_pull_requests": 0,"request_openers":0,
                                     "merged_pull_requests": 0, "request_mergers":0, "commits":0, "committers":0}

    def init_org(org):
        stats[org] = {"ts_begin": start, "ts_end": end, "coin_name":"", "coin_symbol":"",
                      "stats": {"stars": 0, "forks": 0, "open_issues": 0,"issue_openers":0, "closed_issues": 0,"issue_closers":0,
                                "open_pull_requests": 0,"request_openers":0,"merged_pull_requests": 0,
                                "request_mergers":0, "commits":0, "committers":0},
                      "lifetime_stats": {"stars": 0, "forks": 0, "open_issues": 0, "closed_issues": 0, "open_pull_requests": 0,
                                        "merged_pull_requests": 0}, "repos": {}, "total_repos": 0, "active_repos": 0}
    def check_existence(org, repo=None):
        if not repo:
            try:
                stats[org]
            except KeyError:
                init_org(org)
        else:
            try:
                stats[org]["repos"][repo]
            except KeyError:
                init_repo(org, repo)

    def count_stats(stat):
        if stat == "issues":
            keys = ("open_issues", "closed_issues","issue_openers","issue_closers","OPEN","CLOSED")
        if stat == "pullrequests":
            keys = ("open_pull_requests", "merged_pull_requests", "request_openers", "request_mergers", "OPEN", "MERGED")
        if stat == "stars" or stat == "forks":
            for org in get_unique_orgs(data[stat], 1):
                check_existence(org)
                stats[org]["stats"][stat] = sum(1 for e in data[stat] if e[1] == org)
                for repo in get_org_repos(data[stat], org, 1, 2):
                    check_existence(org, repo)
                    stats[org]["repos"][repo][stat] = sum(1 for e in data[stat] if e[1] == org and e[2] == repo)
        if stat == "issues" or stat == "pullrequests":
            for org in get_unique_orgs(data[stat],1):
                check_existence(org)
                stats[org]["stats"][keys[0]] = sum(1 for e in data[stat] if e[1]==org and e[5]==keys[4])
                stats[org]["stats"][keys[1]] = sum(1 for e in data[stat] if e[1]==org and e[5]==keys[5])
                stats[org]["stats"][keys[2]] = len(set(e[3] for e in data[stat] if e[1]==org and e[5]==keys[4]))
                stats[org]["stats"][keys[3]] = len(set(e[6] for e in data[stat] if e[1]==org and e[5]==keys[5]))
                for repo in get_org_repos(data[i],org, 1,2):
                    check_existence(org, repo)
                    stats[org]["repos"][repo][keys[0]] = sum(1 for e in data[stat] if e[1] == org and e[2] == repo and e[5] == keys[4])
                    stats[org]["repos"][repo][keys[1]] = sum(1 for e in data[stat] if e[1]==org and e[2]==repo and e[5] == keys[5])
                    stats[org]["repos"][repo][keys[2]] = len(set(e[3] for e in data[stat] if e[1] == org and e[2] == repo and e[5] == keys[4]))
                    stats[org]["repos"][repo][keys[3]] = len(set(e[6] for e in data[stat] if e[1] == org and e[2] == repo and e[5] == keys[5]))
        if stat == "commits":
            for org in get_unique_orgs(data[stat], 1):
                check_existence(org)
                stats[org]["stats"][stat] = sum(1 for e in data[stat] if e[1] == org)
                stats[org]["stats"]["committers"] = len(set(e[3] for e in data[stat] if e[1] == org))
                for repo in get_org_repos(data[i], org, 1, 2):
                    stats[org][repo]["stats"][stat] = sum(1 for e in data[stat] if e[1] == org and e[2] == repo)
                    stats[org][repo]["stats"]["committers"] = len(set(e[3] for e in data[stat] if e[1] == org and e[2] == repo))
        if stat == "orgstats":
            for line in data[stat]:
                org, coin_name, coin_symbol, last_updated, num_repos = line
                check_existence(org)
                stats[org]["total_repos"] = num_repos
                stats[org]["active_repos"] = len(stats[org]["repos"])
                stats[org]["coin_name"] = coin_name
                stats[org]["coin_symbol"] = coin_symbol
        if stat == "agstats":
            for line in data[stat]:
                repo, owner, since, stars, forks, open_issues, closed_issues, total_issues, open_pull_requests, merged_pull_requests = line
                check_existence(owner)
                check_existence(owner, repo)
                stats[owner]["repos"][repo]["lifetime_stats"] = {"stars":stars, "forks":forks,
                                                               "open_issues":open_issues, "closed_issues":closed_issues,
                                                               "open_pull_requests":open_pull_requests, "merged_pull_requests":merged_pull_requests}
                for k,v in stats[owner]["lifetime_stats"].iteritems():
                    stats[owner]["lifetime_stats"][k] += stats[owner]["repos"][repo]["lifetime_stats"][k]

    #Fill in stats
    for i in ["stars", "forks","issues","pullrequests","agstats","orgstats"]:
        count_stats(i)
    logging.info("stats for %s github accounts successfully aggregated", len(stats.keys()))
    return stats

def count_time_history_stats(_input, out_format="df", aggregate_period=None):
    if not out_format in ["df", "dict", "list"]:
        raise ValueError("out_format must be specified to be either list, dict, or df (Pandas Dataframe)")
    stat_dict = {}
    idx_ref = {"stars":2, "forks":3, "issues":4, "pullrequests":5, "commits":6}
    def insert(_data, date, owner, entry, idx):
        try:
            _data[date]
        except KeyError:
            _data[date] = {owner: [date, owner, 0, 0, 0 ,0, 0]}
        try:
            _data[date][owner][idx_ref[idx]] = entry
        except KeyError:
            _data[date][owner] = [date, owner, 0, 0, 0 ,0, 0]
            _data[date][owner][idx_ref[idx]] = entry

    for key in idx_ref.keys():
        for rec in _input[key]:
            insert(stat_dict, rec[2], rec[1], rec[0], key)
    if out_format == "dict":
        return stat_dict

    stat_list = []
    for repo in stat_dict.itervalues():
        for data in repo.itervalues():
            stat_list.append(data)
    if out_format == "list":
        return stat_list

    stat_df = pd.DataFrame(stat_list, columns=["date", "owner", "stars", "forks","issues", "pullrequests"])
    #stat_df.sort(columns='date', inplace=True)
    stat_df.sort_values(by='date', axis=0, inplace=True)
    stat_df.dropna(inplace=True)
    stat_df.set_index(pd.DatetimeIndex(stat_df['date']), inplace=True)
    if aggregate_period == "Monthly":
        stat_df = stat_df.groupby(pd.TimeGrouper(freq='M'))
    elif aggregate_period == "Weekly":
        stat_df = stat_df.groupby(pd.TimeGrouper(freq='W'))
    elif aggregate_period == "Quarterly":
        stat_df = stat_df.groupby(pd.TimeGrouper(freq='Q'))
    return stat_df


###METHODS FOR PRE-PROCESSING DATA GATEHERED FROM THE API
def collect_org_aggregate_totals(org, since, org_name):
    since = convert_time_format(since, dt2str=True)
    org_totals = {}
    org_totals["stars"], org_totals["forks"], org_totals["open_issues"], org_totals["closed_issues"] = 0,0,0,0
    org_totals["total_issues"], org_totals["open_pull_requests"], org_totals["merged_pull_requests"] = 0,0,0
    org_totals["since"], org_totals["name"] = since, org_name
    for name, repo in org.iteritems():
        org_totals["stars"] += repo['stars']['totalCount']
        org_totals["forks"] += repo['forks']['totalCount']
        org_totals["open_issues"] += repo['openissues']['totalCount']
        org_totals["closed_issues"] += repo['closedissues']['totalCount']
        org_totals["total_issues"] += repo["closedissues"]['totalCount'] + repo["openissues"]['totalCount']
        org_totals["open_pull_requests"] += repo['openrequests']['totalCount']
        org_totals["merged_pull_requests"] += repo['mergedrequests']['totalCount']
    result = (org_totals["name"], org_totals["since"], org_totals["stars"], org_totals["forks"],
              org_totals["open_issues"], org_totals["closed_issues"], org_totals["total_issues"],
              org_totals["open_pull_requests"], org_totals["merged_pull_requests"])
    return result

def stage_org_totals(org_list, since):
    since = convert_time_format(since, dt2str=True)
    return [(org["login"],org["coin_name"], org["coin_symbol"],
             since, org["repositories"]["totalCount"]) for name, org in org_list.iteritems()]


def collect_repo_totals(org, since):
    since = convert_time_format(since, dt2str=True)
    repo_stats = []
    for name, repo in org.iteritems():
        repo["totalissues"] = {'totalCount': repo['openissues']['totalCount'] + repo['closedissues']['totalCount']}
        repo_stats.append((repo["name"], repo["owner"]["login"], since, repo['stars']['totalCount'],
                          repo['forks']['totalCount'], repo['openissues']['totalCount'],
                          repo['closedissues']['totalCount'], repo["totalissues"]["totalCount"],
                          repo['openrequests']['totalCount'], repo['mergedrequests']['totalCount']))
    return repo_stats

def stage_data(org_data, db_format=False):
    stars, forks, openissues, closedissues, openrequests, mergedrequests, org_statistics, commits  = [],[],[],[],[],[],[],[]

    for org_name, org in org_data.iteritems():
        if org_name == "records_end_date" or org_name == "records_start_date":
            continue
        org_statistics += collect_repo_totals(org, org_data["records_end_date"])
        for repo_name, repo in org.iteritems():
            if repo["stars"]["edges"]:
                entry = [(edge["node"]["id"],repo["owner"]["login"],repo["name"], edge["node"]["login"],
                          edge["starredAt"],edge["cursor"]) for edge in repo["stars"]["edges"]]
                stars += entry
            if repo["forks"]["edges"]:
                entry = [(edge["node"]["id"], repo["owner"]["login"],repo["name"], edge["node"]["owner"]["login"],
                          edge["node"]["createdAt"], edge["cursor"]) for edge in repo["forks"]["edges"]]
                forks += entry
            if repo["openissues"]["edges"]:
                for edge in repo["openissues"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                             edge["node"]["createdAt"], edge["node"]["state"], None, None, edge["cursor"])
                    openissues.append(entry)
            if repo["closedissues"]["edges"]:
                for edge in repo["closedissues"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    closedevents = [(event["createdAt"], event["actor"]["login"]) if bool(event["actor"]) else
                                    (event["createdAt"], "") for event in edge["node"]["timeline"]["nodes"] if bool(event)]
                    if closedevents:
                        closedevent = max(closedevents)
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], closedevent[1], closedevent[0], edge["cursor"])
                    else:
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"],None, None, edge["cursor"])
                    closedissues.append(entry)
            if repo["openrequests"]["edges"]:
                for edge in repo["openrequests"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                              edge["node"]["createdAt"], edge["node"]["state"],None, None, edge["cursor"])
                    openrequests.append(entry)
            if repo["mergedrequests"]["edges"]:
                for edge in repo["mergedrequests"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"login":""}
                    mergedevents = [(event["createdAt"], event["actor"]["login"]) if bool(event["actor"]) else
                                    (event["createdAt"], "") for event in edge["node"]["timeline"]["nodes"] if bool(event)]
                    if mergedevents:
                        mergedevent = max(mergedevents)
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], mergedevent[1], mergedevent[0], edge["cursor"])
                    else:
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], None, None, edge["cursor"])
                    mergedrequests.append(entry)
            if repo["commits"]["target"]["history"]["edges"]:
                for edge in repo["commits"]["target"]["history"]["edges"]:
                    if edge["node"]["author"] is None:
                        edge["node"]["author"] = {"name":""}
                    entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["name"],
                              edge["node"]["committedDate"])
                    commits.append(entry)

    num_orgs = len(org_data.keys()) - 2
    issues = openissues + closedissues
    requests = openrequests + mergedrequests
    logging.info("staging stats::git accounts: %s - repos: %s - stars: %s - forks: %s - pullrequests: %s - commits: %s",
                 num_orgs, len(org_statistics), len(stars), len(forks), len(issues), len(requests), len(commits))
    if db_format == False:
        return stars, forks, openissues, closedissues, openrequests, mergedrequests, org_statistics, commits
    else:
        return stars, forks, issues, requests, org_statistics, commits