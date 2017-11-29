import log_service.logger_factory as lf
import res.env_config as env
import data_scraping.scrapingutils as su
import data_scraping.datautils as du
import environment_config as ec
import types
import os
from prawtools_fork.stats import SubredditStats
logging = lf.get_loggly_logger(__name__)


TOP_VALUES = {'all', 'day', 'month', 'week', 'year'}

class SubredditStatCollector(object):
    def __init__(self, period, subbreddit_list=None, client_id=None, client_secret=None, user=None,
                 pw=None, use_default_envars=True):
        if period in TOP_VALUES:
            self.period = period
        else:
            error_msg = "period specified was %d value must be 'day', 'week', 'month', 'year', or 'all'" % (period)
            logging.error(error_msg, exc_info=True)
            raise ValueError(error_msg)
        self.sr_analyzer = None
        self.subreddit_list = self.__validate_subbredit_list__(subbreddit_list)
        if isinstance(self.subreddit_list, list):
            subreddit = subbreddit_list[0]
            self.sr_analyzer = self.__initialize_reddit_tools_object__(client_id, client_secret,
                                                                              user,pw, subreddit, use_default_envars)

    def get_stats_from_next_subreddit(self):
        if self.subreddit_list == None:
            raise ValueError("Subreddit List not initalized, initialize subreddit list before continuing")
        elif not isinstance(self.subreddit_list, list):
            raise ValueError("Some sneaky pete set the subreddit list object to something else, bad dog")
        if isinstance(self.subreddit_list, list):
            while bool(self.subreddit_list):
                subreddit = self.subreddit_list.pop(0)
                self.sr_analyzer.__reset__(subreddit)
                results = self.sr_analyzer.run(self.period, 10, 10)
                yield results

    def initialize_new_subreddit_list(self, period, subreddit_list):
        self.subreddit_list = self.__validate_subbredit_list__(subreddit_list)
        if period in TOP_VALUES:
            self.period = period

    ##Internal Utility Methods
    def __validate_subbredit_list__(self, subreddit_list):
        if subreddit_list == None:
            logging.info("subreddit list not initialized")
            return None
        if isinstance(subreddit_list, (list, tuple, set)) and len(subreddit_list) > 0:
            logging.info("subreddit list initialized with %s elements", len(subreddit_list))
            return list(subreddit_list)
        elif not isinstance(subreddit_list, (list, tuple, set) and len(subreddit_list) < 1):
            error_msg = "Empty set, list, or tuple passed. List of subreddits cannot be empty"
            logging.error(error_msg, exc_info=True)
            raise ValueError(error_msg)
        else:
            error_msg = "Subreddit list must be set, list, tuple, or None type, passed type was %s"\
                        % (type(subreddit_list))
            logging.error(error_msg, exc_info=True)
            raise ValueError(error_msg)

    @staticmethod
    def __validate_credentials__(client_id, client_secret, user, pw, use_default_envars):
        inputs = (client_id, client_secret, user, pw)
        if not any(inputs) and use_default_envars == True:
            ec.set_default_reddit_envars()
            _inputs = ec.get_reddit_envars()
            return _inputs[0], _inputs[1], _inputs[2], _inputs[3]
        elif all(inputs) and use_default_envars == False:
            return inputs
        else:
            logging.error("If not using default config, all inputs must be given")
            raise ValueError("If not using default config, all inputs must be specified")

    @staticmethod
    def __initialize_reddit_tools_object__(client_id, client_secret, user, pw, subbredit, use_default_envars):
        if os.path.isfile("praw.ini"):
            os.remove("praw.ini")
        _client_id, _client_secret, _user, _pw = SubredditStatCollector.__validate_credentials__(
            client_id, client_secret, user, pw, use_default_envars)
        praw_temp_file = open("praw.ini", "w")
        praw_temp_file.writelines(["[the_site]" + "\n", "client_id=" + _client_id + "\n",
                                   "client_secret=" + _client_secret + "\n", "password=" + _pw + "\n",
                                   "username=" + _user])
        praw_temp_file.close()
        srs = SubredditStats(subbredit, "the_site", False)
        os.remove("praw.ini")
        return srs

    @staticmethod
    def collect_stats_on_single_subreddit(subreddit, period, client_id=None, client_secret=None,
                                          user=None, pw=None, use_default_envars=True):
        _client_id, _client_secret, _user, _pw = SubredditStatCollector.__validate_credentials__(
            client_id, client_secret, user, pw, use_default_envars)
        sr_analyzer = SubredditStatCollector.__initialize_reddit_tools_object__(_client_id, _client_secret,
                                                                              _user,_pw, subreddit)

        if period not in TOP_VALUES:
            error_msg = "period specified was %s value must be 'day', 'week', 'month', 'year', or 'all'" %(period)
            logging.error(error_msg)
            raise ValueError(error_msg)

        results = sr_analyzer.run(period, 10, 10)
        sr_analyzer = None
        return results

class CustomRedditManager(object):
    def __init__(self, initialize_token = True, app_id=None, app_secret=None, user=None, pw=None,
              grant_type=None, header_type=None, header_value=None):
        self.__token__, self.token_expiry, self.custom_credentials = None, None, False
        if initialize_token == True:
            _inputs = (app_id, app_secret, user, pw, grant_type, header_type, header_value)
            self.__validate_oauth_inputs__(_inputs, initialize=True)
            self.__token__ = self.get_oauth_token()

    def __validate_oauth_inputs__(self, inputs, initialize=False):
        if any(inputs) and not all(inputs):
            logging.error("If not using default config, all inputs must be given")
            raise ValueError("If not using default config, all inputs must be specified")
        elif any(inputs) == False:
            logging.info("Using inputs in envrionment config for Reddit Oauth")
            if initialize == True:
                ec.set_default_reddit_envars()
                return
            _app_id, _app_secret, _user, _pw, _grant_type, _header = ec.get_reddit_envars(self.custom_credentials)
            _inputs = _app_id, _app_secret, _user, _pw, _grant_type, _header, False
        else:
            logging.info("Using custom inputs for Reddit Oauth")
            if initialize == True:
                _app_id, _app_secret, _user, _pw, _grant_type, _header_type, _header_value = inputs
                ec.set_custom_reddit_envars(app_id=_app_id, app_secret=_app_secret, user=_user, pw=_pw,
                                  grant_type=_grant_type, header_type=_header_type, header_value=_header_value)
                return
            _app_id, _app_secret, _user, _pw, _grant_type, _header = inputs
            _inputs = _app_id, _app_secret, _user, _pw, _grant_type, _header, True
        return _inputs

    def __is_token_expired__(self):
        if self.token_expiry == None:
            return True
        return not du.time_is_after(self.token_expiry)

    def __check_token__(self):
        if (self.__token__ == None or self.__is_token_expired__()):
            logging.debug("Token expired, getting new token")
            _token = self.get_oauth_token()
        else:
            logging.debug("Current Token is Valid")
            _token = self.__token__
        return _token

    def get_oauth_token(self, app_id=None, app_secret=None, user=None, pw=None, grant_type=None, header=None):
        _inputs = (app_id, app_secret, user, pw, grant_type, header)
        _app_id, _app_secret, _user, _pw, _grant_type, _header, ext_inputs = self.__validate_oauth_inputs__(_inputs)
        _oauth_url = "https://www.reddit.com/api/v1/access_token"
        _post_data = {"grant_type": _grant_type, "username": _user, "password": _pw}
        _token = su.get_oauth_token(_oauth_url, app_id=_app_id, app_secret=_app_secret,
                                  post_data=_post_data, header=_header)
        if ext_inputs == True:
            logging.info("External inputs set, internal class tokens will not be set")
            return _token
        if ext_inputs == False:
            self.__token__, self.token_expiry = _token, du.get_time(seconds=_token["expires_in"] - 10, add=True)
            return _token

    def custom_api_call(self, api_call, token=None, user_agent=None):
        if not isinstance(api_call, str):
            raise ValueError("Api call input must be of type string, data passed was %s", api_call)
        if bool(token) and bool(user_agent):
            logging.info("Custom token and user-agent %s being used", user_agent)
            _token = token
            _user_agent = user_agent
        elif not bool(token) and not bool(user_agent):
            logging.info("Default token with credentials specified within environment being used")
            _token = self.__check_token__()
            _user_agent = env.get_envar("REDDIT_APP_HEADER_AGENT_DESCRIPTION")
        else:
            msg = "If using custom token, specify token and user-agent, if using default envars, specify neither"
            logging.warn(msg)
            raise ValueError(msg)
        auth = "bearer " + str(_token['access_token'])
        headers = {"Authorization": auth,
                          "User-Agent":_user_agent}
        response = su.get_http(api_call, headers=headers)
        return response.json()