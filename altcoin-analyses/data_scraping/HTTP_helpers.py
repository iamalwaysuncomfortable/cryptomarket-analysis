import data_scraping.scrapingutils as su
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)

def general_api_call(call):
    r = su.get_http(call)
    if r.status_code == 200:
        logging.info("Response from %s OK", call)
        return r
    else:
        logging.warn("Response from %s BAD with response code of %s", call, r.status_code)
        logging.warn("Response content was %s", r)
        return r

def try_json_conversion(response):
    try:
        logging.info("Returning response in JSON format")
        json = response.json()
        return json
    except:
        logging.warn("No Json Encoding detected in response, returning original response format")
        return response