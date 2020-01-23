# Prerequisite script for creating the MusicStream database and its tables
# Run before other scripts

import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    '''
    Creates the MusicStream database
    '''
    # connect to database studentdb and get cursor
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    
    # create the MusicStream database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS musicstreamdb")
    cur.execute("CREATE DATABASE musicstreamdb WITH encoding 'UTF8' TEMPLATE template0")
    # template0 used to get a clean db creation
    
    # close default db connection
    conn.close()
    
    # connect to musicstreamdb
    conn = psycopg2.connect("host=127.0.0.1 dbname=musicstreamdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def create_tables(cur, conn):
    '''
    Create tables defined in sql_queries
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    

def drop_tables(cur, conn):
    '''
    Drop tables from the database
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    

def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()
    

if __name__ == "__main__":
    main()