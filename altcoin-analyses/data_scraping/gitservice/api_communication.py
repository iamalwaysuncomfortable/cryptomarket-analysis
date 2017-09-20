from custom_errors.github_errors import GithubAPIBadQueryError, GithubAPIError
import data_scraping.datautils as du
from data_scraping import scrapingutils as su
from requests import HTTPError
import json

###API Secret
secret = ""

###Data Fetching Utility Methods
def post_gql_query(gql, secret_):
    'Send call to Github API via HTTP, throw error if failure'
    if gql.endswith('.graphql'):
        query = du.striplines(gql)
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
        print(error_data)
        return "error", error_data
    except GithubAPIBadQueryError as e:
        print GithubAPIBadQueryError.__name__ + " on query:"
        error_data = [e.message, e.result, e.query]
        print(error_data)
        return "error", error_data
    except GithubAPIError as e:
        response = e.data
        print GithubAPIError.__name__ + "with message of " + e.message + "and status code of " + response.status_code + " on query:"
        error_data = [e.message, response.status_code, response, query]
        print(error_data)
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

#Google sheets communication (delete later in favor of POSTGRES solution)
def get_gsheet_data(url):
    data = su.read_csv(url)
    return data
