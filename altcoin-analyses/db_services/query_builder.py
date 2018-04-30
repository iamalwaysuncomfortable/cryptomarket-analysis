import custom_utils.datautils as du
import log_service.logger_factory as lf

logging = lf.get_loggly_logger(__name__)


def add_between_clauses(pairs, pair_type, default_min, default_max, clauses):
    if pair_type == "num":
        validator = du.validate_number
    elif pair_type == "time":
        validator = du.validate_epoch
    else:
        error_msg=("Invalid Pair type specified 'num' or 'time' must be specified, pair_type "
                   "specified was %s") % (str(pair_type))
        logging.error(error_msg)
        raise ValueError(error_msg)

    for pair in pairs:
        if du.any_num(pair[1], pair[2]):
            clauses.append(numeric_between_clause(pair[0], validator(pair[1], default_min),
                                                     validator(pair[2], default_max)))

def add_IN_clauses(columns, clauses):
    for column in columns:
        if column[1]: clauses.append(make_IN_clause(column[0], column[1]))

def query_builder(null_condition, clauses, query_columns="*", order_columns=None, order_direction=None, table=None):
    if not isinstance(table, (str, unicode)):
        error_msg = "Table variable must be str or unicode, table value specified was %s" %(str(table))
        logging.error(error_msg, exc_info=True)
        raise ValueError(error_msg)
    _columns = column_selection_validator(query_columns)
    statement = "SELECT " + _columns + " FROM " + table
    first_clause = False
    for clause in clauses:
        if clause == null_condition:
            continue
        elif first_clause == False:
            statement = statement + " WHERE " + clause
            first_clause = True
        elif first_clause == True:
            statement = statement + " AND " + clause
    if order_columns:
        _order_columns = " ORDER BY " + column_selection_validator(order_columns)
        if order_direction in ("ASC", "DESC", "asc", "desc"):
            _order_columns += " " + str(order_direction)
        statement += _order_columns
    return statement




def column_selection_validator(columns):
    if columns == "*":
        return columns
    elif isinstance(columns, (str, unicode)):
        return str(columns)
    elif isinstance(columns, (tuple, list, set)):
        if not all(isinstance(x, (str, unicode)) for x in columns):
            raise ValueError("All values in subreddit input must be in str or unicode format")
        else:
            _columns = ""
            for column in columns:
                if _columns == "":
                    _columns = _columns + str(column)
                else:
                    _columns = _columns + ", " + str(column)
            return _columns
    else:
        raise ValueError("input must be string, unicode or tuple/set/list with all elements of type str or unicode")

def numeric_between_clause(column_name, start, end):
    print(start, end)
    if isinstance(start, (int, float)) and isinstance(end, (int, float)):
        if not (start < end):
            error_msg = "End value must be greater than start value. " \
                        "Data was column: %s start: %s end: %s" % (column_name, start, end)
            logging.error(error_msg, exc_info=True)
            raise ValueError(error_msg)
    else:
        error_msg = "start and end values must be int or float"
        print((start, end))
        logging.error(error_msg, exc_info=True)
        raise ValueError(error_msg)
    return column_name + " BETWEEN " + str(start) + " and " + str(end)


def make_IN_clause(column_name, subreddits):
    if isinstance(subreddits, (tuple, list, set)):
        if not all(isinstance(x, (str, unicode)) for x in subreddits):
            raise ValueError("All values in subreddit input must be in str or unicode format")
        elif len(subreddits) > 0:
            if len(subreddits) == 1:
                subreddit_clause = column_name + " in " + "('" + subreddits[0] + "')"
            else:
                subreddit_clause = column_name + " in " + str(tuple(str(x) for x in subreddits))
            return subreddit_clause

    elif isinstance(subreddits, (str, unicode)):
        if subreddits[0] == '(' and subreddits[-1] == ')':
            _subreddits = subreddits
        else:
            _subreddits = "(" + subreddits + ')'
        subreddit_clause = column_name + " in " + _subreddits
        return subreddit_clause
    else:
        error_msg = "subreddits specified must be a single string or list/tuple/set of subreddits, subreddit " \
                    "specified was %s" %(str(subreddits))
        logging.error(error_msg, exc_info=True)
        raise ValueError(error_msg)