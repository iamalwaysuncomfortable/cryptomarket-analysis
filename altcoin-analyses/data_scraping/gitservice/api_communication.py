import json
import time

from requests import HTTPError

import custom_utils.datautils as du
import log_service.logger_factory as lf
from custom_utils.errors.github_errors import GithubAPIBadQueryError, GithubAPIError
from data_scraping import scrapingutils as su
from res.env_config import get_envar

##Setup Logger
logging = lf.get_loggly_logger(__name__)

###API Secret
secret = get_envar("GITHUB_API_SECRET")

###Data Fetching Utility Methods
def post_gql_query(gql, secret_):
    'Send call to Github API via HTTP, throw error if failure'
    if gql.endswith('.graphql'):
        query = du.striplines(gql)
    elif isinstance(gql, str) or isinstance(gql, unicode):
        query = gql
    else:
        logging.warn("gql passed to post_gql_query function was " + type(gql).__name__ + ", string or .graphql required")
        raise TypeError("gql passed to post_gql_query function was " + type(gql).__name__ + ", string or .graphql required")
    token = "bearer " + secret_
    headers = {"Authorization": token}
    response = su.post_http('https://api.github.com/graphql', json.dumps({"query": query}), headers=headers, max_retries=10)
    if response.status_code == 200:
        try:
            result = response.json()
        except:
            raise (response, "json encoding failed")
        if 'errors' in result:
            logging.warn("error detected in gql results")
            logging.warn("error query: %s error data is: %s", query, result["errors"])
            raise GithubAPIBadQueryError(result, query, response.status_code)
        else:
            return response.status_code, result
    elif (400 <= response.status_code < 600):
        logging.warn("bad response code of %s found on query %s response was %s", response.status_code, query, response)
        raise response.raise_for_status()
    else:
        msg = "github API returned a non 200 SUCCESS code of %s" % response.status_code
        logging.warn("OK response code of %s found on query %s response was %s", response.status_code, query, response)
        raise GithubAPIError(response, response.status_code, msg)

def try_gql_query(query, limitcheck=True, cost=100, wait=True):
    'Attempt a graphql query, if it fails, return response with information about failure'
    if limitcheck == True:
        try:
            status, remaining, limit_exceeded, resetAt = check_limit(cost)
            if limit_exceeded:
                logging.warn("limit exceeded, reset at %s", resetAt)
                if wait==True:
                    now, reset = du.get_time(now=True), du.convert_time_format(resetAt, str2dt=True)
                    if (reset > now):
                        interval = (reset - now).seconds
                        logging.warn("wait option True, queries to github graphql api will resume in %s seconds",
                                     interval + 90)
                        time.sleep(interval + 90)
                        pass
                    else:
                        time.sleep(30)
                        pass
                else:
                    logging.warn("Wait option False, queries to github API will terminate, remaining queries stored for later use")
                    return "limit exceeded", (status, remaining, limit_exceeded, resetAt)
        except:
            logging.warn("Limit check failed", exc_info=True)
            pass
    try:
        status, result = post_gql_query(query, secret)
        return "success", result
    except HTTPError as e:
        response = e.response
        error_data = ["HTTP Error", e.message, response.status_code, query, response]
        logging.exception("HTTP Error Raised")
        return "error", error_data
    except GithubAPIBadQueryError as e:
        error_data = ["Github API Bad Query Error", e.message, e.status, e.query, e.result]
        logging.exception("GithubAPIBadQuery Error Raised")
        return "error", error_data
    except GithubAPIError as e:
        response = e.data
        logging.exception("GithubAPI Error Raised")
        error_data = ["Github API Error", e.message, e.status, query, response]
        return "error", error_data
    except TypeError as e:
        logging.exception("Type Error Raised")
        return "error", ["Type Error", e.message, query]
    except Exception as e:
        logging.exception("Misc Error Raised")
        return "error", ["Other Exception", e.message, query]
    except:
        logging.exception("Unknown Error Raised")
        return "error", ["Unknown Error", "",query]

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
