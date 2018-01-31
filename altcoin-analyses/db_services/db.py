import psycopg2
from dbconfig import config
import log_service.logger_factory as lg
logging = lg.get_loggly_logger(__name__)

def change_encoding(_types, scope=None):
    def set_type(_type):
        if _type == "DateTime->String":
            NewDate = psycopg2.extensions.new_type((1082,), 'DATE', psycopg2.STRING)
            logging.debug("Type is %s, scope is %s", _type, scope)
            psycopg2.extensions.register_type(NewDate, scope)
        if _type == "BigInt->Float":
            logging.debug("Type is %s", _type)
            SmallInt = psycopg2.extensions.new_type((20,), 'INT8', psycopg2.NUMBER)
            logging.debug("Type is %s, scope is %s", _type, scope)
            psycopg2.extensions.register_type(SmallInt, scope)
    if isinstance(_types, str):
        set_type(_types)
    if hasattr(_types, "__iter__"):
        for _type in _types:
            set_type(_type)

def check_existence():
    conn = None
    exists = False
    params = config()
    try:
        conn = psycopg2.connect(**params)
        logging.info("DB exists and is accessible with parameters passed: %s", params)
        exists = True
    except Exception as e:
        logging.exception("Passed configuration parameters %s fail for reason: %", params, e.message)
    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')
    return exists

def query_database(query_list, num_records, all, typecasts=None, scope=None):
    conn = None
    results = {}
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        #Set any custom return types desired
        if typecasts:
            logging.info('Typecasting was enacted with scope %s and type %s', scope, typecasts)
            if scope == "cur":
                change_encoding(typecasts, cur)
            if scope == "conn":
                change_encoding(typecasts, conn)

        # execute a statement
        for query in query_list:
            cur.execute(query[1])
            if all == True:
                results[query[0]] = cur.fetchall()
                logging.info("query %s being executed, all records requested", query)
            elif num_records == 1 and all == False:
                logging.info("query %s being executed, %s record requested", query, num_records)
                results[query[0]] = [cur.fetchone()]
            elif num_records > 1 and all == False:
                logging.info("query %s being executed, %s records requested", query, num_records)
                results[query[0]] = cur.fetchmany(num_records)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("query failed with stack trace:")
    finally:
        if conn is not None:
            conn.close()
            logging.info("database connection closed")
    return results

def make_single_query(query, num_records=0, all=True):
    conn = None
    results = {}
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        cur.execute(query)
        if all == True:
            logging.info("query %s being executed, all records requested", query)
            results = cur.fetchall()
        elif num_records == 1 and all == False:
            logging.info("query %s being executed, %s record requested", query, num_records)
            results = cur.fetchone()
        elif num_records > 1 and all == False:
            logging.info("query %s being executed, %s records requested", query, num_records)
            results = cur.fetchmany(num_records)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("query failed with stack trace:")
    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')
    return results

def write_data(inputs):
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        for input in inputs:

            sql, data = input
            logging.info("%s items with sql write statement %s being executed", len(data), sql)
            i = 0
            for item in data:
                if i < 2:
                    logging.debug("sql is %s", sql)
                    logging.debug("data is %s", item)
                i += 1
                cur.execute(sql, item)
        logging.info('Changes being committed')
        conn.commit()
        # close communication with the database
        cur.close()
        status = "success"
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Error in db write, stack trace is:")
        status = "fail"
    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')
    return status

def return_connection():
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Error in db write, stack trace is: %s", error)
        if conn is not None:
            conn.close()

def write_single_record(sql, data=None):
    conn = None
    results = {}
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        if data:
            logging.info("sql statement %s being executed with data: %s", sql, data)
            cur.execute(sql, data)
        else:
            logging.info("sql statement %s being executed", sql)
            cur.execute(sql)
        # commit the changes to the database
        logging.info('Changes being committed')
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Error in db write, stack trace is:")
    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')
    return