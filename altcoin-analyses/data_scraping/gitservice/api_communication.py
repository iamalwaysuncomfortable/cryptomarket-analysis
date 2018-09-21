import json
import time
import custom_utils.HTTP_helpers as HTTPh
from requests import HTTPError
import re
import custom_utils.datautils as du
import log_service.logger_factory as lf
from custom_utils.errors.github_errors import GithubAPIBadQueryError, GithubAPIError
from data_scraping import scrapingutils as su
from res.env_config import get_envar

##Setup Logger
logging = lf.get_loggly_logger(__name__)

###API Secret
secret = get_envar("GITHUB_API_SECRET")

def get_all_repos_in_account_http(account_type, account_name, retries=0):
    repo_list = []
    page = 1
    while True:
        result, status = repo_query_http(account_type, account_name, page, retries)
        if status != 200:
            raise HTTPError(status, "Non 200 status encountered, data was %s", result)
        repo_list += result
        if len(result) < 30:
            break
        page += 1
    return repo_list

def  repo_query_http(account_type, account_name, page, retries=0):
    token = "bearer " + secret
    headers = {"Authorization": token}
    if account_type in ("user", "u", "users"):
        account_type = "users"
    elif account_type in ("org", "o", "organization", "orgs"):
        account_type = "orgs"
    else:
        raise ValueError("Account type must be users or orgs")
    call = "https://api.github.com/" + account_type + "/" + account_name + "/repos?page=" + str(page)
    result, status = HTTPh.general_api_call(call, return_json=True, return_status=True, headers=headers)
    if status != 200 and isinstance(retries, (int, float)) and retries > 0:
        while retries > 0 and status != 200:
            logging.warn("Status was %s retrying, %s retries left", status, retries)
            logging.warn("Message was %s", result)
            time.sleep(3)
            result, status = HTTPh.general_api_call(call, return_json=True, return_status=True)
            retries -= 1
            if status == 200 or retries <= 0:
                logging.warn("Number of retries allotted exceeded, returning error status & message")
                break
    logging.info("Result OK, returning")
    return result, status



###Data Fetching Utility Methods
def post_gql_query(gql, secret_, retry_bad_status_codes=True, retries=5):
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
            logging.error("error in gql results query: %s error data is: %s",
                          query, result["errors"], extra={"status":response.status_code})
            raise GithubAPIBadQueryError(result, query, response.status_code)
        else:
            return response.status_code, result
    elif (400 <= response.status_code < 600):
        if retry_bad_status_codes == True and retries > 0:
            for retry in range(1, retries+1):
                logging.warn("Status code of %s given, retrying. %s retries left"
                             , response.status_code, retries - retry
                             , extra={"status":response.status_code, "query": re.sub('"', '', query)})
                response = su.post_http('https://api.github.com/graphql', json.dumps({"query": query}), headers=headers,
                                        max_retries=10)
                if response.status_code == 200:
                    try:
                        result = response.json()
                    except:
                        raise (response, "json encoding failed")
                    if 'errors' in result:
                        logging.error("error in gql results query: %s error data is: %s",
                                      query, result["errors"], extra={"status": response.status_code})
                        raise GithubAPIBadQueryError(result, query, response.status_code)
                    else:
                        return response.status_code, result
        if retry_bad_status_codes == True:
            logging.error("Query failure after %s retries ", retries
                          , extra={"status":response.status_code, "query":query,
                                   "query_failure":True})
        else:
            logging.error("Query failure"
                          , extra={"status": response.status_code, "query": query,
                                   "query_failure": True})
        raise response.raise_for_status()
    else:
        msg = "github API returned a non 200 SUCCESS code of %s" % response.status_code
        logging.warn("OK response code of %s found on query %s response was %s",
                     response.status_code, query, response, extra={"status":response.status_code})
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