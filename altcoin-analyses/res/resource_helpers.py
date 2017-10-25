import os.path

def get_res_dir():
    return os.path.dirname(os.path.realpath(__file__))

def get_log_data_dir():
    return os.path.dirname(os.path.realpath(__file__)) + "/logging_data"

def get_db_data_dir():
    return os.path.dirname(os.path.realpath(__file__)) + "/db_data"
