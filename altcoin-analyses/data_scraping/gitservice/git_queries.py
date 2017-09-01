import data_scraping.scrapingutils as su
import pandas as pd
##Graphql Prep Methods
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
            query = 'query { '+ alias +': repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } ref(qualifiedName: "master") { target { ... on Commit { id history(first: '+str(num_commits)+') { edges { node { oid author { name date } } cursor } pageInfo { hasNextPage } } } } } stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 40) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } } cursor } } } }'
            return query, alias
        else:
            query = 'query { repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } ref(qualifiedName: "master") { target { ... on Commit { id history(first: '+str(num_commits)+') { edges { node { oid author { name date } } cursor } pageInfo { hasNextPage } } } } } stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 40) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } } cursor } } } }'
            return query
    else:
        alias = generate_alias(repo)
        query = alias + ': repository(owner:"'+owner+'", name:"'+repo+'") { name updatedAt owner{ login } ref(qualifiedName: "master") { target { ... on Commit { id history(first: '+str(num_commits)+') { edges { node { oid author { name date } } cursor } pageInfo { hasNextPage } } } } } stargazers(last: '+str(num_stars)+', orderBy: {field: STARRED_AT, direction: ASC}) { totalCount edges { starredAt node { id login } cursor } } forks(last: '+str(num_forks)+') { totalCount edges { node { id createdAt owner { login } } cursor } } openissues: issues(last: '+str(num_issues)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } closedissues: issues(last: '+str(num_issues)+', states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 40) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } openrequests: pullRequests(last: '+str(num_pulls)+', states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } mergedrequests: pullRequests(last: '+str(num_pulls)+', states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } } cursor } } }'
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
            "open_issues"] + '", states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } '
    if closed_issues:
        query += 'closedissues: issues(last: ' + str(num) + ', before: "' + cursors[
            "closed_issues"] + '", states: CLOSED) { totalCount edges { node { id createdAt state author { login } timeline(first: 12) { nodes { ... on ClosedEvent { createdAt actor { login } } } } } cursor } } '
    if merged_requests:
        query += 'mergedrequests: pullRequests(last: ' + str(num) + ', before: "' + cursors[
            "merged_requests"] + '", states: MERGED) { totalCount edges { node { id createdAt mergedAt state author { login } } cursor } } '
    if open_requests:
        query += 'openrequests: pullRequests(last: ' + str(num) + ', before: "' + cursors[
            "open_requests"] + '", states: OPEN) { totalCount edges { node { id createdAt state author { login } } cursor } } '
    query += '}}'
    return query
