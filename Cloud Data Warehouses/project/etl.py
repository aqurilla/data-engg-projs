import psycopg2
from sql_queries import copy_queries, insert_queries
from helper import config_parser, connect_redshift

def stage_data():
    '''
    Loads data into staging tables
    '''
    print('Staging data')
    for query in copy_queries:
        cur.execute(query)
        conn.commit()
        
def insert_data():
    '''
    Inserts data into fact and dim tables
    '''
    print('Inserting data into fact and dim tables')
    for query in insert_queries:
        cur.execute(query)
        conn.commit()

def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    config_parser()
    conn, cur = connect_redshift()
    
    stage_data(conn, cur)
    insert_data(conn, cur)
    
    conn.close()
    
if __name__ == "__main__":
    main()