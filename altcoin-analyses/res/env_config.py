import os
import sys
import platform
import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)

def set_master_config():
    _platform = platform.system()
    _platform = _platform.lower()
    if _platform == "linux" or _platform == "linux2" or _platform == "darwin":
        _path = os.environ["HOME"] + "/bb-env-config"
        sys.path.append(_path)
        import bb_envar_config
        bb_envar_config.set_env_var()

def set_specific_config(config_name):
    _platform = platform.system()
    if _platform == "linux" or _platform == "linux2":
        print "It's linux"
    elif _platform == "Darwin":
        #OSX
        _path = os.environ["HOME"] + "/bb-env-config"
        sys.path.append(_path)
        import bb_envar_config
        if config_name == "cryptocurrencychart":
            bb_envar_config.set_cryptocurrencychart_envars()

def append_sys_path(path, relative_from_home_dir=True):
    _platform = platform.system()
    if _platform == "linux" or _platform == "linux2":
        print "It's linux"
    elif _platform == "Darwin":
        #OSX
        if relative_from_home_dir == True:
            _path = os.environ["HOME"] + path
        else:
            _path = path
        sys.path.append(_path)

def get_envar(key):
    if not isinstance(key, (str, unicode)):
        logging.warn("input passed must be string or unicode, type passed was %", type(key))
        raise TypeError("input passed must be string or unicode")
    if key in os.environ.keys():
        return os.environ[key]
    else:
        logging.warn("requested environment variable %s does not exist", key)
        raise ValueError("requested environment variable does not exist")

def set_envar(key, value):
    if not isinstance(key, (str, unicode)):
        logging.warn("input passed must be string or unicode, type passed was %", type(key))
        raise TypeError("input passed must be string or unicode")
    os.environ[key] = value

def set_multiple_envars(pairs):
    for pair in pairs:
        if  len(pair) != 2 or not hasattr(pair, '__iter__'):
            logging.info("All pairs in input must be iterable and have exactly 2 elements of type str or unicode")
            raise ValueError("All pairs in input must be iterable and have exactly 2 elements of type str or unicode")
        set_envar(pair[0], pair[1])