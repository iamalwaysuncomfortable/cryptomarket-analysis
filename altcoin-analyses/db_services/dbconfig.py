from configparser import ConfigParser
import res.resource_helpers as res
import log_service.logger_factory as lg
logging = lg.get_loggly_logger(__name__)
FILENAME = res.get_db_data_dir() + "/config.ini"

def config(filename=FILENAME, section='postgresql'):
    logging.info("Config Filename is %s", FILENAME)
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        logging.warn('Section %s not found in the %s file', section, filename)
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db