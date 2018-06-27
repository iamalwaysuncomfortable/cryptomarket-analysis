import data_scraping.scrapingutils as su
import pandas as pd

##Graphql Prep Methods
def generate_alias(name):
    alias = name
    alias = alias.replace("-", "_")
    alias = alias.replace(".", "_")
    if alias[0].isdigit():
        alias = "r" + alias
    return alias

###Graphql query formation methods
def accounttype_query(owner, repo):
    'determine if single github account is owner or user type'
    owner_alias = generate_alias(owner)
    repo_alias = generate_alias(repo)
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

def single_org_query(owner, account_type, limit=10, completequery=True, pagination=False, cursor=None, single_repo_org=False):
    'Form graphql query for the repositories of a list of a signle organization or user (account type must be specified)'
    owner_alias = generate_alias(owner)
    if single_repo_org:
        query = 'query { ' + str(owner_alias) + ': '+str(account_type) + '(login: "' + owner + '") { login } defaultBranchRef { name } }'
        return query
    if pagination and cursor and completequery:
        query = 'query { ' + str(owner_alias) + ': '+str(account_type) + '(login: "' + owner + '") { login repositories(first: ' + str(limit) + ' after: "' + cursor + '") { totalCount edges { node { name updatedAt owner { login } defaultBranchRef { name } } cursor } } } }'
        return query
    if pagination and cursor and not completequery:
        query = str(owner_alias) + ': '+str(account_type) + '(login: "' + owner + '") { login repositories(first: ' + str(limit) + ' after: "' + cursor + '") { totalCount edges { node { name updatedAt owner { login } defaultBranchRef { name } } cursor } } } '
        return query
    if completequery:
        query = 'query { organization(login: "'+owner+'") { login repositories(first: '+str(limit)+') { totalCount edges { node { name updatedAt owner { login } defaultBranchRef { name } } cursor } } } }'
    else:
        query = str(owner_alias) + ': '+str(account_type) + '(login: "' + owner + '") { login repositories(first: ' + str(limit) + ') { totalCount edges { node { name updatedAt owner { login } defaultBranchRef { name } } cursor } } } '
    return query

def multiple_org_query(repo_list, limit=10):
    'Form graphql query for the repositories of a list of organizations or users (account type must be specified)'
    query = ""
    for index, row in repo_list.iterrows():
        query = query + single_org_query(row["owner"], row["account_type"], limit, False)
    query = "query { " + query + " }"
    return query


def repo_query(owner, repo, num_commits=50, num_stars=50, num_forks=50, num_issues=50, num_pulls=50, full_query = True, create_alias=True, default_branch = "master"):
    'Form graphql query for a single repo'
    #if full_query == True:
    if create_alias == True:
        alias = generate_alias(repo)
        query = alias + ': repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } defaultBranchRef { name } commits: ref(qualifiedName: "' + default_branch + '") { target { ... on Commit { history(first: '+str(num_commits)+') { edges { node { id committedDate author { name } } cursor } pageInfo { hasNextPage } } } } } stars: stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 100) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } timeline(first: 60) { nodes { ... on MergedEvent { createdAt actor { login } } } } } cursor } } }'
        if full_query == True: query = 'query { ' + query + ' }'
        return query, alias
    else:
        query = 'query { repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } defaultBranchRef { name } commits: ref(qualifiedName: "' + default_branch + '") { target { ... on Commit { history(first: '+str(num_commits)+') { edges { node { id committedDate author { name } } cursor } pageInfo { hasNextPage } } } } } stars: stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 100) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } timeline(first: 60) { nodes { ... on MergedEvent { createdAt actor { login } } } } } cursor } } } }'
        return query

def simple_repo_query(owner, repo, full_query = True, create_alias=True):
    if create_alias == True:
        alias = generate_alias(repo)
        query = alias + ': repository(owner:"' + owner + '", name:"' + repo + '") { name updatedAt owner { login } defaultBranchRef { name } }'
        if full_query == True: query = 'query { ' + query + ' }'
        return query, alias
    else:
        query = 'query { repository(owner:"' + owner + '", name:"' + repo + '") { name updatedAt owner { login } defaultBranchRef { name } } }'
    return query

def pagination_query(owner, repo, cursors, num=50, stars=False, forks=False, commits=False, open_issues=False,
                     closed_issues=False, open_requests=False, merged_requests=False):
    'Form and execute query for pagination'
    alias = generate_alias(repo)
    query = 'query { ' + alias + ' : repository(owner:"' + owner + '", name:"' + repo + '") { name updatedAt owner{ login } defaultBranchRef { name } '
    if stars:
        query += 'stars: stargazers(last: ' + str(num) + ', before: "' + cursors[
            "stars"] + '", orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } '
    if forks:
        query += 'forks(last: ' + str(num) + ', before: "' + cursors[
            "forks"] + '" ) { totalCount edges { node { id createdAt owner { login } } cursor } } '
    if open_issues:
        query += 'openissues: issues(last: ' + str(num) + ', before: "' + cursors[
            "openissues"] + '", states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } '
    if closed_issues:
        query += 'closedissues: issues(last: ' + str(num) + ', before: "' + cursors[
            "closedissues"] + '", states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 75) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } '
    if merged_requests:
        query += 'mergedrequests: pullRequests(last: ' + str(num) + ', before: "' + cursors[
            "mergedrequests"] + '", states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } timeline(first: 60) { nodes { ... on MergedEvent { createdAt actor { login } } } } } cursor } } '
    if open_requests:
        query += 'openrequests: pullRequests(last: ' + str(num) + ', before: "' + cursors[
            "openrequests"] + '", states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } '
    if commits:
        query += 'commits: ref(qualifiedName: "' + cursors["default_branch"
        ] + '") { target { ... on Commit { history(first: '+str(num)+' after: "'+cursors["commits"
        ]+'") { edges { node { id committedDate author { name } } cursor } pageInfo { hasNextPage } } } } } '

    query += '}}'
    return query
