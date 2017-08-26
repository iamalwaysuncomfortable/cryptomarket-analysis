import psycopg2

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE devdata.forks (
            id VARCHAR(255) PRIMARY KEY,
            org VARCHAR(255) NOT NULL,
            repo VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_by VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE devdata.stars (
            id VARCHAR(255) PRIMARY KEY,
            org VARCHAR(255) NOT NULL,
            repo VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
        """,
        """
        CREATE TABLE devdata.issues (
            id VARCHAR(255) PRIMARY KEY,
            org VARCHAR(255) NOT NULL,
            repo VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_by VARCHAR(255) NOT NULL,
            closed BOOLEAN NOT NULL,
            cloased_at TIMESTAMP WITH TIME ZONE NOT NULL,
            closed_by VARCHAR(255) DEFAULT ''
        )
        """,
        """
        CREATE TABLE devdata.pullrequests (
            id VARCHAR(255) PRIMARY KEY,
            org VARCHAR(255) NOT NULL,
            repo VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_by VARCHAR(255) NOT NULL,
            merged BOOLEAN NOT NULL,
            merged_at TIMESTAMP,
            merged_by VARCHAR(255) DEFAULT ''
        )
        """,
        """
        CREATE TABLE devdata.commits (
            id VARCHAR(255) PRIMARY KEY,
            org VARCHAR(255) NOT NULL,
            repo VARCHAR(255) NOT NULL,
            date TIMESTAMP WITH TIME ZONE NOT NULL,
            created_by VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE devdata.repos (
            repo VARCHAR(255) PRIMARY KEY,
            org VARCHAR(255) NOT NULL,
            orgtype ORG_TYPE NOT NULL,
            last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
            forks INTEGER NOT NULL default 0,
            open_requests INTEGER NOT NULL default 0,
            merged_pull_requests INTEGER NOT NULL default 0,
            open_issues INTEGER NOT NULL default 0,
            closed_issues INTEGER NOT NULL default 0,
            stars INTEGER NOT NULL default 0,
            commits INTEGER NOT NULL default 0
        )
        """
        )
    conn = None
    try:
        # read the connection parameters
        params = {"host":"localhost", "database":"statsdata", "user":"CommanderTurner", "password":""}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()