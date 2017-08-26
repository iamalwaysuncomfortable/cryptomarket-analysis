import psycopg2
from sqlalchemy import create_engine


def returndb(user="CommanderTurner", password="", host="localhost", database="statsdata"):
    try:
        # read the connection parameters
        params = {"host": host, "database":database, "user":user, "password": password}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        return conn, cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def returnsqlengine(user="CommanderTurner", password="", host="localhost", database="statsdata", port='5432'):
    engine = create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+database+'')
    return engine
