import log_service.logger_factory as lf
import res.env_config as env
logging = lf.get_loggly_logger(__name__)

def set_default_reddit_envars():
    try:
        #Check if envars already exist
        _app_id = env.get_envar("REDDIT_APP_ID")
        _app_secret = env.get_envar("REDDIT_APP_SECRET")
        _user = env.get_envar("REDDIT_USER")
        _pw = env.get_envar("REDDIT_USER_SECRET")
        _grant_type = env.get_envar("REDDIT_GRANT_TYPE")
        _header_type = env.get_envar("REDDIT_APP_HEADER_AGENT_TYPE")
        _header_value = env.get_envar("REDDIT_APP_HEADER_AGENT_DESCRIPTION")
    except:
        #If not try to set them
        try:
            env.set_master_config()
        except:
            raise EnvironmentError("Default Environment Configuration Failed")


def set_custom_reddit_envars(app_id=None, app_secret=None, user=None, pw=None,
              grant_type=None, header_type=None, header_value=None, enforce_all=True):
    custom_params = (app_id, app_secret, user, pw, grant_type, header_type, header_value)
    pairs = (("ALT_REDDIT_APP_ID", app_id), ("ALT_REDDIT_APP_SECRET", app_secret), ("ALT_REDDIT_USER", user),
             ("ALT_REDDIT_USER_SECRET", pw), ("ALT_REDDIT_GRANT_TYPE", grant_type),
             ("ALT_REDDIT_HEADER_AGENT_TYPE", header_type), ("ALT_REDDIT_HEADER_AGENT_DESCRIPTION"),
             header_value)
    if enforce_all==True:
        if not all(custom_params):
            error_msg = "If setting custom environment variables, set default_envars parameter to false and specify"\
                        "all required parameters. If using default environment variables set default_envars to True" \
                             "and leave all custom reddit config options blank."
            logging.error(error_msg)
            raise ValueError(error_msg)
        else:
            env.set_multiple_envars(pairs)
    else:
        for pair in pairs:
            if pair[1] == None:
                pass
            else:
                env.set_envar(pair[0], pair[1])

def get_reddit_envars(custom_envars = False):
    varnames = ["REDDIT_APP_ID","REDDIT_APP_SECRET","REDDIT_USER","REDDIT_USER_SECRET", "REDDIT_GRANT_TYPE",
                   "REDDIT_APP_HEADER_AGENT_TYPE","REDDIT_APP_HEADER_AGENT_DESCRIPTION"]
    if custom_envars == True:
        varnames = ["ALT_" + name for name in varnames]

    try:
        _app_id = env.get_envar(varnames[0])
        _app_secret = env.get_envar(varnames[1])
        _user = env.get_envar(varnames[2])
        _pw = env.get_envar(varnames[3])
        _grant_type = env.get_envar(varnames[4])
        _header_type = env.get_envar(varnames[5])
        _header_value = env.get_envar(varnames[6])
        _header = {_header_type: _header_value}
        return _app_id, _app_secret, _user, _pw, _grant_type, _header
    except:
        msg = "One or more of the required environment variables was not found, " \
              "make sure environment variables are configured"
        logging.error(msg, exc_info=True)
        raise EnvironmentError(msg)