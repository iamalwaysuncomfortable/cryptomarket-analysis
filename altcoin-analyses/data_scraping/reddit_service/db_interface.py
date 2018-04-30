import time

import custom_utils.datautils as du
import db_services.db as db
import db_services.query_builder as qb
import log_service.logger_factory as lf

logging = lf.get_loggly_logger(__name__)

sql_write_statements = {"subreddit_stats": "INSERT INTO redditdata.subreddit_stats (subreddit, collection_interval, "
                                     "collection_start_time, first_submission, last_submission, "
                                     "submission_interval, comment_score, comment_rate, submission_score, "
                                     "submission_rate, num_comments, num_commenters, num_submissions, num_submitters,"
                                           "subscribers, active_users)"
                                     " VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON "
                                     "CONFLICT DO NOTHING",
                        "submission_data": "INSERT INTO redditdata.submission_data (s_id, collection_start_time, "
                                           "title, subreddit, submission_score, num_comments, created_at, author) "
                                           "VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"}

def push_record_with_existing_conn(conn, subreddit_stats = None, submission_data = None):
    table_names = ("subreddit_stats", "submission_data")
    input_data = (subreddit_stats, submission_data)
    i = 0
    for data in input_data:
        if data:
            sql = sql_write_statements[table_names[i]]
            with conn.cursor() as cur:
                if isinstance(data, (tuple, list)):
                    if table_names[i] == "submission_data":
                        for submission in submission_data:
                            logging.info("sql statement %s being executed with data: %s", sql, submission)
                            cur.execute(sql, submission)
                    else:
                        logging.info("sql statement %s being executed with data: %s", sql, data)
                        cur.execute(sql, data)
                else:
                    logging.info("sql statement %s being executed", sql)
                    cur.execute(sql)
                # commit the changes to the database
                logging.info('Changes being committed')
                conn.commit()
        i += 1


def push_batch_reddit_data(subreddit_stats = None, submission_data = None):
    data_to_write = []
    table_names = ("subreddit_stats", "submission_data")
    table_subset = []
    input_data = (subreddit_stats, submission_data)
    i = 0
    for values in input_data:
        if values:
            sql = sql_write_statements[table_names[i]]
            table_subset.append(table_names[i])
            data_to_write.append((sql, input_data[i]))
        i += 1
    logging.info("github data: %s being written to db", table_subset)
    db.write_data(data_to_write)

def submission_query(subreddits=None, start=None, end=None, min_comments=None, max_comments=None, min_score=None,
                          max_score=None, query_columns="*", order_columns=None, order_direction=None, s_ids=None,
                          titles=None, query_only=False, table="submission_data"):
    """ query submissions """
    date_clause, subreddit_clause, comment_clause, score_clause, sid_clause, title_clause = "", "", "", "", "", ""

    if du.any_num(start, end): date_clause = qb.numeric_between_clause("created_at", du.validate_epoch(start, default_date=0),
                                      du.validate_epoch(end, default_date=time.time()))

    if subreddits: subreddit_clause = qb.make_IN_clause("subreddit", subreddits)

    if s_ids: sid_clause = qb.make_IN_clause("s_id", s_ids)

    if titles: title_clause = qb.make_IN_clause("title", titles)

    if du.any_num(min_comments, max_comments):
        comment_clause = qb.numeric_between_clause("num_comments", du.validate_number(min_comments, 0),
                                             du.validate_number(max_comments, 1000000))

    if du.any_num(min_score, max_score):
        score_clause = qb.numeric_between_clause("submission_score", du.validate_number(min_score, 0),
                                       du.validate_number(max_score, 1000000))

    query = qb.query_builder("",(date_clause, subreddit_clause, comment_clause, score_clause, title_clause, sid_clause),
                          query_columns, order_columns, order_direction, table="redditdata." + table)
    if query_only == True:
        return query
    else:
        result = db.query_database([(table, query)], 0, True)
        return result

def stats_query(query_columns="*", subreddits=None, start=None,end=None, min_submissions=None, max_submissions=None,
                max_submission_score=None,min_submission_score=None,min_submission_rate=None, max_submission_rate=None,
                min_submitters=None,max_submitters=None, min_comments = None, max_comments = None,
                min_comment_score=None,max_comment_score=None, min_commenters=None, max_commenters=None,
                min_comment_rate = None, max_comment_rate=None, order_columns=None, order_direction=None,
                query_only=False, table="subreddit_stats"):

    clauses = []
    time_pairs = (("collection_start_time", start, end),)
    num_pairs = (("num_submissions", min_submissions, max_submissions),("submission_score", min_submission_score,
                  max_submission_score), ("num_submitters", min_submitters, max_submitters),("submission_rate",
                  min_submission_rate, max_submission_rate), ("num_comments",min_comments,max_comments),
                 ("comment_score", min_comment_score, max_comment_score),("num_commenters",min_commenters,
                  max_commenters),("comment_rate",min_comment_rate, max_comment_rate))
    text = (("subreddit", subreddits),)

    qb.add_between_clauses(time_pairs, "time", 0, time.time(), clauses)
    qb.add_between_clauses(num_pairs, "num", 0, 1000000, clauses)
    qb.add_IN_clauses(text, clauses)

    query = qb.query_builder("",clauses,query_columns, order_columns, order_direction, table="redditdata." + table)
    if query_only == True:
        return query
    else:
        result = db.query_database([(table, query)], 0, True)
        return result