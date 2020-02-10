import psycopg2
from sql_queries import drop_queries, create_queries
from helper import config_parser, connect_redshift

def drop_tables():
    '''
    Drop existing tables
    '''
    print('Dropping tables')
    for query in drop_queries:
        cur.execute(query)
        conn.commit()
    
def create_tables():
    '''
    Create tables
    '''
    print('Creating tables')
    for query in create_queries:
        cur.execute(query)
        conn.commit()

def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    config_parser()
    conn, cur = connect_redshift()
    
    drop_tables(conn, cur)
    create_tables(conn, cur)
    
    conn.close()
    
if __name__ == "__main__":
    main()
    