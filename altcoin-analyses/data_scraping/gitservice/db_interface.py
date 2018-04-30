import log_service.logger_factory as lf
import res.resource_helpers as res
from custom_utils.datautils import get_time, convert_time_format, import_json_file
from db_services.db import query_database, write_single_record, make_single_query, write_data

##Setup Logger
logging = lf.get_loggly_logger(__name__)

def get_github_analytics_data(stars=False, forks=False, pullrequests=False, issues=False, stats=False, orgtotals = False, commits = False, all_records=True, num_records=0, start=None, end=None, org = None, repo = None):
    """ query parts from the parts table """
    query_list = []
    start_date = "2007-10-01T00:00:00Z"
    end_date = get_time(now=True, utc_string=True)
    org_clause = ""
    org_clause_stats = ""

    if org:
        if repo:
            org_clause = " AND (owner, repo) = ('"+org+"', '"+repo+"')"
            org_clause_stats = " WHERE (owner, repo) = ('"+org+"', '"+repo+"')"
        else:
            org_clause = " AND owner = '"+org+"'"
            org_clause_stats = " WHERE owner = '"+org+"'"
    if start:
        start_date = convert_time_format(start, dt2str=True)
    if end:
        end_date = convert_time_format(end, dt2str=True)

    if stars:
        query_list.append(("stars",
            "SELECT * FROM devdata.stars WHERE starred_at BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'"+ org_clause +" ORDER BY owner, repo, starred_at DESC"))
    if forks:
        query_list.append(
            ("forks","SELECT * FROM devdata.forks WHERE created_at BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'"+ org_clause +" ORDER BY owner, repo, created_at DESC"))
    if issues:
        query_list.append(
            ("issues", "SELECT * FROM devdata.issues WHERE COALESCE(closed_at, created_at) BETWEEN timestamp '"+ start_date +"' and timestamp '"+ end_date +"'"+ org_clause +" ORDER BY owner, repo, COALESCE(closed_at, created_at) DESC"))
    if pullrequests:
        query_list.append(
            ("pullrequests", "SELECT * FROM devdata.pullrequests WHERE COALESCE(merged_at, created_at) BETWEEN timestamp '"+ start_date +"' and timestamp '"+ end_date +"' ORDER BY COALESCE(merged_at, created_at) DESC"))
    if commits:
        query_list.append(
            ("commits",
             "SELECT * FROM devdata.commits WHERE committed_at BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "' ORDER BY owner, repo, committed_at DESC"))
    if stats:
        query_list.append(
            ("agstats", "SELECT * FROM devdata.agstats"+ org_clause_stats +" ORDER BY _at DESC"))
    if orgtotals:
        query_list.append(
            ("orgstats", "SELECT * FROM devdata.orgstats"))

    logging.info("getting github data from db, start date: %s - end date: %s - data requested: %s %s",
                  start_date, end_date, [q[0] for q in query_list], org_clause_stats)
    result = query_database(query_list, num_records, all_records)
    return result


def get_remaining_queries(since=None):
    if since == None:
        query = "SELECT * FROM devdata.remainingqueries"
    else:
        convert_time_format(since, dt2str=True)
        query = "SELECT * FROM devdata.remainingqueries WHERE (_at) = ('"+since+"')"
    data = query_database([("remainingqueries", query)], num_records=0, all=True)
    return data


def write_remaining_orgs(since, records):
    convert_time_format(since, dt2str=True)
    sql = "INSERT INTO devdata.remainingqueries (_at, doc) VALUES(%s, %s) ON CONFLICT (_at) DO UPDATE SET (doc) = (EXCLUDED.doc)"
    write_single_record(sql, (since,records))


def edit_last_updated_timestamp(since):
    since_notz = since.replace(tzinfo=None)
    ts_data = get_last_updated_timestamp()
    if ts_data:
        timestamp = ts_data[0][0]
        if since_notz > timestamp:
            UPDATE_sql = "INSERT INTO devdata.lastupdated (_id, _at) VALUES(%s, %s) ON CONFLICT (_id) DO UPDATE SET (_at) = (EXCLUDED._at)"
            write_single_record(UPDATE_sql, (True, since_notz))
    else:
        UPDATE_sql = "INSERT INTO devdata.lastupdated (_id, _at) VALUES(%s, %s) ON CONFLICT (_id) DO UPDATE SET (_at) = (EXCLUDED._at)"
        write_single_record(UPDATE_sql, (True, since_notz))


def get_last_updated_timestamp():
    SELECT_sql = "SELECT _at FROM devdata.lastupdated"
    return make_single_query(SELECT_sql)


def clean_temp_data(since):
    convert_time_format(since, dt2str=True)
    sql = "DELETE FROM devdata.remainingqueries WHERE (_at) = ('"+since+"')"
    write_single_record(sql, since)

    #query_database(query_list, num_records, all):
def push_github_analytics_data(stars = None, forks = None, issues = None, pullrequests = None, stats = None, orgstats=None, commits=None):
    data_to_write = []
    table_names = ("stars", "forks", "issues", "pullrequests","agstats", "orgstats","commits")
    table_subset = []
    input_data = (stars, forks, issues, pullrequests, stats, orgstats, commits)
    statements = import_json_file(res.get_db_data_dir() + "/gitgql_sql_push_statements.json")
    i = 0
    for values in input_data:
        if values:
            sql = statements[table_names[i]]
            table_subset.append(table_names[i])
            data_to_write.append((sql, input_data[i]))
        i += 1
    logging.info("github data: %s being written to db", table_subset)
    write_data(data_to_write)

def get_time_stats(stars = True, forks = True, issues = True, pullrequests = True, commits = True, all_records=True, num_records = 0, start = None, end = None, org=None, repo=None, utf_format=True):
    org_clause = ""
    start_date = "2007-10-01T00:00:00Z"
    end_date = get_time(now=True, utc_string=True)
    if start:
        start_date = convert_time_format(start, dt2str=True)
    if end:
        end_date = convert_time_format(end, dt2str=True)

    if org:
        if repo:
            org_clause = " AND (owner, repo) = ('"+org+"', '"+repo+"')"
        else:
            org_clause = " AND owner = '"+org+"'"

    query_list = []
    select_statements = {"stars": "select count(id) as star_count, owner, starred_at::date as _date FROM devdata.stars WHERE starred_at::date BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'"+ org_clause +" GROUP BY owner, starred_at::date ORDER BY lower(owner) ASC",
    "forks": "select count(id) as fork_count, owner, created_at::date as _date FROM devdata.forks WHERE created_at::date BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'"+ org_clause +" GROUP BY owner, created_at::date ORDER BY lower(owner) ASC",
    "issues": "select count(id) as issue_close_count, owner, closed_at::date as _date FROM devdata.issues WHERE closed_at::date BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'"+ org_clause +" GROUP BY owner, closed_at::date ORDER BY lower(owner) ASC",
    "pullrequests": "select count(id) request_merge_count, owner, merged_at::date as _date FROM devdata.pullrequests WHERE merged_at BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'"+ org_clause +" GROUP BY owner, merged_at::date ORDER BY lower(owner) ASC",
    "commits": "select count(id) commit_count, owner, committed_at::date as _date FROM devdata.commits WHERE committed_at BETWEEN timestamp '" + start_date + "' and timestamp '" + end_date + "'" + org_clause + " GROUP BY owner, committed_at::date ORDER BY lower(owner) ASC"}
    for k, v in {"stars":stars, "forks":forks, "issues":issues, "pullrequests":pullrequests, "commits":commits}.iteritems():
        if v == True:
            query_list.append((k, select_statements[k]))
    if utf_format == True:
        result = query_database(query_list, num_records, all_records, "DateTime->String", "cur")
    else:
        result = query_database(query_list, num_records, all_records)
    logging.info("getting time stat data from db, start date: %s - end date: %s - data requested: %s %s",
                  start_date, end_date, [q[0] for q in query_list], org_clause)
    return result