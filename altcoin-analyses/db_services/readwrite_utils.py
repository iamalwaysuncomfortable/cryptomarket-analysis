import log_service.logger_factory as lf
import db
logging = lf.get_loggly_logger(__name__)

def get_data_from_one_table(query):
    return db.make_single_query(query)

def get_data_from_multiple_tables(table_names, table_queries):
    if len(set([len(table_names),len(table_queries)])) != 1:
        raise ValueError("error: inequally sized inputs data and sql statements must match 1 to 1")
    return db.query_database([list(zip(table_names, table_queries))], num_records=0, all=True)

def push_batch_data(data_tables, sql_write_statements):
    if len(set([len(data_tables),len(sql_write_statements)])) != 1:
        raise ValueError("error: inequally sized inputs data and sql statements must match 1 to 1")
    data_to_write = []
    input_data = data_tables
    i = 0
    for values in input_data:
        if values:
            sql = sql_write_statements[i]
            data_to_write.append((sql, input_data[i]))
        i += 1
    logging.info("github data: %s being written to db", sql_write_statements)
    status = db.write_data(data_to_write)
    return status

def push_record_with_existing_conn(conn, data_tables, sql_write_statements):
    if len(set([len(data_tables),len(sql_write_statements)])) != 1:
        raise ValueError("error: inequally sized inputs data and sql statements must match 1 to 1")
    input_data = data_tables
    i = 0
    for data in input_data:
        if data:
            sql = sql_write_statements[i]
            with conn.cursor() as cur:
                if isinstance(data, (tuple, list)):
                    logging.info("sql statement %s being executed with data: %s", sql, data)
                    cur.execute(sql, data)
                else:
                    logging.info("sql statement %s being executed", sql)
                    cur.execute(sql)
                # commit the changes to the database
                logging.info("github data: %s being written to db", sql_write_statements)
                logging.info('Changes being committed')
                conn.commit()
        i += 1