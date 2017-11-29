import os.path
import sys

def get_current_dir(input_dir):
    return os.path.dirname(os.path.realpath(input_dir))

def get_praw_fork_directory():
    return os.getcwd() + "/CryptoGit/prawtools_fork"

def add_dir_to_path(dir):
    sys.path.append(dir)

def get_res_dir():
    return os.path.dirname(os.path.realpath(__file__))

def get_log_data_dir():
    return os.path.dirname(os.path.realpath(__file__)) + "/logging_data"

def get_db_data_dir():
    return os.path.dirname(os.path.realpath(__file__)) + "/db_data"
