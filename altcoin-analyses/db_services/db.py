import psycopg2
from dbconfig import config
from data_scraping.datautils import import_json_file
from data_scraping.datautils import get_time

def query_database(query_list, num_records, all):
    conn = None
    results = {}
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        for query in query_list:
            cur.execute(query[1])
            if all == True:
                results[query[0]] = cur.fetchall()
            elif num_records == 1 and all == False:
                results[query[0]] = [cur.fetchone()]
            elif num_records > 1 and all == False:
                results[query[0]] = cur.fetchmany(num_records)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return results

def get_github_analytics_data(stars=False, forks=False, pullrequests=False, issues=False, stats=False, orgtotals = False, all_records=True, num_records=0, start=None, end=None, org = None, repo = None):
    """ query parts from the parts table """
    query_list = []
    start_date = "2007-10-01 00:00:00"
    end_date = get_time(now=True, utc_string=True, custom_format="%Y-%m-%d %H:%M:%S")
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
        start_date = start
    if end:
        end_date = end

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
    if stats:
        query_list.append(
            ("stats", "SELECT * FROM devdata.agstats"+ org_clause_stats +" ORDER BY _at DESC"))
    if orgtotals:
        query_list.append(
            ("orgtotals", "SELECT * FROM devdata.orgstats"))

    result = query_database(query_list, num_records, all_records)
    return result

def write_data(inputs):
    conn = None
    results = {}
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        for input in inputs:
            sql, data = input
            for item in data:
                print(sql)
                print(item)
                cur.execute(sql, item)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return

def push_github_analytics_data(stars = None, forks = None, issues = None, pullrequests = None, stats = None, orgstats=None):
    data_to_write = []
    table_names = ("stars", "forks", "issues", "pullrequests", "agstats", "orgstats")
    input_data = (stars, forks, issues, pullrequests, stats, orgstats)
    requests = import_json_file("db_services/git_sql_pushstatements.json")
    i = 0
    for values in input_data:
        if values:
            sql = requests[table_names[i]]
            data_to_write.append((sql, input_data[i]))
        i += 1
    write_data(data_to_write)
    return