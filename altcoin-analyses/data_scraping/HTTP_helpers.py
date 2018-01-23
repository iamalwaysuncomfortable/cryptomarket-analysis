import data_scraping.scrapingutils as su
import requests
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)

def general_api_call(call, return_json=False, return_status=False):
    r = su.get_http(call)
    status_code = r.status_code
    if r.status_code == 200:
        logging.info("Response from %s OK", call)
        if return_json==True and return_status==False:
            return try_json_conversion(r)
        elif return_status == True and return_json == True:
            return try_json_conversion(r), status_code
        elif return_json == False and return_status == True:
            return r, status_code
        else:
            return r
    else:
        logging.warn("Response from %s BAD with response code of %s", call, r.status_code)
        logging.warn("Response content was %s", r)
        if return_json==True:
            return try_json_conversion(r)
        return r

def try_json_conversion(response):
    try:
        logging.info("Returning response in JSON format")
        json = response.json()
        return json
    except:
        logging.warn("No Json Encoding detected in response, returning original response format")
        return response

def throw_HTTP_error(data):
    requests.HTTPError(data)