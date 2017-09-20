from db_services.db import get_github_analytics_data
###METHODS FOR FETCHING AND PROCESSESING DATA FROM THE DATABASE
def all_org_stats(start=None, end=None):
    #Internal methods
    def get_unique_orgs(input_data, index):
        return set(e[index] for e in input_data)

    def get_org_repos(input_data, org, org_index, repo_index):
        return set(e[repo_index] for e in input_data if e[org_index] == org)

    def init_repo(org, repo):
        stats[org]["repos"][repo] = {"stars": 0, "forks": 0, "openissues": 0, "issueopeners":0, "closedissues": 0, "issueclosers":0,
                                     "openrequests": 0,"requestopeners":0, "mergedrequests": 0, "requestmergers":0}

    def init_org(org):
        stats[org] = {"ts_begin": start, "ts_end": end,
                      "stats": {"stars": 0, "forks": 0, "openissues": 0,"issueopeners":0, "closedissues": 0,"issueclosers":0,
                                "openrequests": 0,"requestopeners":0,"mergedrequests": 0, "requestmergers":0},
                      "lifetimestats": {"stars": 0, "forks": 0, "openissues": 0, "closedissues": 0, "openrequests": 0,
                                        "mergedrequests": 0}, "repos": {}, "total_repos": 0, "active_repos": 0}

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
            keys = ("openissues", "closedissues","issueopeners","issueclosers","OPEN","CLOSED")
        if stat == "pullrequests":
            keys = ("openrequests", "mergedrequests", "requestopeners", "requestmergers", "OPEN", "MERGED")
        if stat == "stars" or stat == "forks":
            for org in get_unique_orgs(data[stat], 1):
                check_existence(org)
                stats[org]["stats"][stat] = len([e[3] for e in data[stat] if e[1] == org])
                for repo in get_org_repos(data[stat], org, 1, 2):
                    check_existence(org, repo)
                    stats[org]["repos"][repo][stat] = len([e[3] for e in data[stat] if e[1] == org and e[2] == repo])
        if stat == "issues" or stat == "pullrequests":
            for org in get_unique_orgs(data[stat],1):
                check_existence(org)
                stats[org]["stats"][keys[0]] = len([e[3] for e in data[stat] if e[1]==org and e[5]==keys[4]])
                stats[org]["stats"][keys[1]] = len([e[3] for e in data[stat] if e[1]==org and e[5]==keys[5]])
                stats[org]["stats"][keys[2]] = len(set([e[3] for e in data[stat] if e[1]==org and e[5]==keys[4]]))
                stats[org]["stats"][keys[3]] = len(set([e[6] for e in data[stat] if e[1]==org and e[5]==keys[5]]))
                for repo in get_org_repos(data[i],org, 1,2):
                    check_existence(org, repo)
                    stats[org]["repos"][repo][keys[0]] = len([e[3] for e in data[stat] if e[1] == org and e[2] == repo and e[5] == keys[4]])
                    stats[org]["repos"][repo][keys[1]] = len([e[3] for e in data[stat] if e[1]==org and e[2]==repo and e[5] == keys[5]])
                    stats[org]["repos"][repo][keys[2]] = len(set([e[3] for e in data[stat] if e[1] == org and e[2] == repo and e[5] == keys[4]]))
                    stats[org]["repos"][repo][keys[3]] = len(set([e[6] for e in data[stat] if e[1] == org and e[2] == repo and e[5] == keys[5]]))
        if stat == "orgstats":
            for line in data[stat]:
                org, last_updated, num_repos = line
                check_existence(org)
                stats[org]["total_repos"] = num_repos
                stats[org]["active_repos"] = len(stats[org]["repos"])
        if stat == "agstats":
            for line in data[stat]:
                owner, repo, since, stars, forks, open_issues, closed_issues, total_issues, open_pull_requests, merged_pull_requests = line


    #Initialize Data
    data = get_github_analytics_data(True, True, True, True, True, True, start=start, end=end)
    stats = {}

    #Fill in stats
    for i in ["stars", "forks","issues","pullrequests", "orgstats"]:
        count_stats(i)
    return stats

def org_stats(org, repo=None, start=None, end=None):
    data = get_github_analytics_data(True, True, True, True, True, start=start, end=end, org=org, repo=repo)

###METHODS FOR PRE-PROCESSING DATA GATEHERED FROM THE API
def collect_org_aggregate_totals(org, since, org_name, repo_count_only=True):
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

def stage_org_totals(org_list, since):
    return [(org["name"],since, org["totalCount"]) for name, org in org_list["data"].iteritems()]


def collect_repo_totals(org, since):
    repo_stats = []
    for name, repo in org.iteritems():
        repo["totalissues"] = {'totalCount': repo['openissues']['totalCount'] + repo['closedissues']['totalCount']}
        repo_stats.append((repo["name"], repo["owner"]["login"], since, repo['stargazers']['totalCount'],
                          repo['forks']['totalCount'], repo['openissues']['totalCount'],
                          repo['closedissues']['totalCount'], repo["totalissues"]["totalCount"],
                          repo['openrequests']['totalCount'], repo['mergedrequests']['totalCount']))
    return repo_stats

def stage_data(org_data, db_format=False):
    stars, forks, openissues, closedissues, openrequests, mergedrequests, org_statistics  = [],[],[],[],[],[],[]
    for org_name, org in org_data.iteritems():
        if org_name == "records_end_date" or org_name == "records_start_date":
            continue
        print org_name
        org_statistics += collect_repo_totals(org, org_data["records_end_date"])
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
                    print closedevents
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
                    print mergedevents
                    if mergedevents:
                        mergedevent = max(mergedevents)
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], mergedevent[1], mergedevent[0], edge["cursor"])
                    else:
                        entry = (edge["node"]["id"], repo["owner"]["login"], repo["name"], edge["node"]["author"]["login"],
                                 edge["node"]["createdAt"], edge["node"]["state"], None, None, edge["cursor"])
                    mergedrequests.append(entry)

    if db_format == False:
        return stars, forks, openissues, closedissues, openrequests, mergedrequests, org_statistics
    else:
        issues = openissues + closedissues
        requests = openrequests + mergedrequests
        return stars, forks, issues, requests, org_statistics