import data_scraping.scrapingutils as su
import requests
import log_service.logger_factory as lf
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.util.retry import Retry
logging = lf.get_loggly_logger(__name__)

def general_api_call(call, return_json=False, return_status=False,
                     method="get", data="", headers=None, auth=None):
    if method == "get":
        r = get_http(call, headers=headers)
    elif method == "post":
        r = post_http(call,data,headers,auth=auth)
    else:
        raise ValueError("HTTP method specified must be get or post")
    status_code = r.status_code
    if status_code == 200:
        logging.info("Response from %s OK", call)
    else:
        logging.warn("Response from %s failed with status code: %s", call, status_code)
    if return_json==True and return_status==False:
        return try_json_conversion(r)
    elif return_status == True and return_json == True:
        return try_json_conversion(r), status_code
    elif return_json == False and return_status == True:
        return r, status_code
    else:
        return r

def try_json_conversion(response):
    try:
        logging.info("Returning response in JSON format")
        json = response.json()
        return json
    except:
        logging.warn("No Json Encoding detected in response, returning original response format")
        return response

def throw_HTTP_error(data, message):
    data.raise_for_status(message)

def post_http(uri, data="", headers = None, max_retries=5, auth=None):
    retries = Retry(total=max_retries, backoff_factor=1)
    with requests.Session() as s:
        a = HTTPAdapter(max_retries=retries)
        s.mount('https://', a)
        response = s.post(uri, data, headers=headers, auth=auth)
    return response

def get_oauth_token(url, app_id, app_secret, post_data, header = None):
    logging.debug("get_oauth_token launched")
    client_auth = HTTPBasicAuth(app_id, app_secret)
    response = post_http(url, auth=client_auth, data=post_data, headers=header)
    logging.debug("client_auth is %s, response is %s", client_auth, response)
    return response.json()

def get_http(uri, parameters = None, max_retries=5, headers = None):
    """ Get data from API or download HTML, try each URI 5 times """
    with requests.Session() as s:
        a = requests.adapters.HTTPAdapter(max_retries)
        s.mount('https://', a)
        response = s.get(uri, params = parameters, headers = headers)
    return response