import logging
import logging.config
import loggly.handlers
import res.resource_helpers as res

def log_exceptions_from_entry_function(logger):
    def decorator(a_function):
        def wrapper(*args):
            try:
                print "try function"
                a_function()
            except Exception as e:
                print e
                logger.exception("Entry Level Function Exception Raised")
        return wrapper
    return decorator

def generate_logger(level="DEBUG", format="default", stream_handler=True, file_handler=False, file_name="default", loggly=False):
    if not isinstance(level, str):
        raise ValueError("Specifications must be in str format")
    if loggly == True:
        logging.config.fileConfig('python.conf')

    logger = logging.getLogger(__name__)
    states = {"DEBUG":logging.DEBUG, "INFO":logging.INFO, "WARNING":logging.WARNING, "ERROR":logging.ERROR,
              "CRITICAL":logging.CRITICAL, "NOTSET":logging.NOTSET}
    if level in states.keys(): logger.setLevel(states[str.upper(level)])
    else: raise ValueError("logging level invalid, must specify INFO, WARNING, ERROR, DEBUG, or NOTSET")
    if format == "default":
        formatter = logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(module)s:%(filename)s:%(funcName)s:%(message)s')
    else:
        formatter = logging.Formatter(format)
    if stream_handler == True:
        s_handler = logging.StreamHandler()
        s_handler.setFormatter(formatter)
        logger.addHandler(s_handler)
    if file_handler == True:
        if file_name == "default":
            f_handler = logging.FileHandler('logfile.log')
            f_handler.setFormatter(formatter)
            logger.addHandler(f_handler)
        elif file_name != "default" and isinstance(file_name, str):
            f_handler = logging.FileHandler(file_name)
            f_handler.setFormatter(formatter)
            logger.addHandler(f_handler)
    return logger

def get_loggly_logger(name):
    logger_name = "logservice." + name
    logger = logging.getLogger(logger_name)
    return logger

def launch_logging_service(console=True, loggly=False, file=False, filename="program_stream.log"):
    config_file_path = res.get_log_data_dir() + "/python.conf"
    logging.config.fileConfig(config_file_path)