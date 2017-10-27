import os
import sys
import platform
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)

def config_environment():
    _platform = platform.system()
    if _platform == "linux" or _platform == "linux2":
        print "It's linux"
    elif _platform == "Darwin":
        #OSX
        _path = os.environ["HOME"] + "/bb-env-config"
        sys.path.append(_path)
        import bb_envar_config
        bb_envar_config.set_env_var()

def get_envar(key):
    if not (isinstance(key, str) or isinstance(key, unicode)):
        logging.warn("input passed must be string or unicode, type passed was %", type(key))
        raise TypeError("input passed must be string or unicode")
    if key in os.environ.keys():
        return os.environ[key]
    else:
        logging.warn("requested environment variable %s does not exist", key)
        raise ValueError("requested environment variable does not exist")