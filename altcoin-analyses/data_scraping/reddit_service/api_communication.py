import log_service.logger_factory as lf
import res.env_config as env
import data_scraping.scrapingutils as su
logging = lf.launch_logging_service(__name__)

try:
    app_id = env.get_envar("REDDIT_APP_ID")
    app_secret = env.get_envar("REDDIT_APP_SECRET")
    username = env.get_envar("REDDIT_USER")
    password = env.get_envar("REDDIT_USER_SECRET")
except:
    err_msg = "Required environment vars %s, %s, %s, %s not found, ensure environment vars are correctly configured"
    logging.error(err_msg, "REDDIT_APP_ID", "REDDIT_APP_SECRET", "REDDIT_USER", "REDDIT_USER_SECRET", exc_info=True)
    raise EnvironmentError(err_msg % ("REDDIT_APP_ID", "REDDIT_APP_SECRET", "REDDIT_USER", "REDDIT_USER_SECRET"))

api_url = "https://www.reddit.com/api/v1/access_token"
post_data = {"grant_type": permissions_requested, "username": username, "password": password}

token = su.get_oauth_token(api_url, app_id=app_id, app_secret=app_secret, post_data=post_data)


permissions_requested = ""