import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' 
    TODO: Insert data from S3 into staging tables
    Lopping through copy_table_queries array that include raw queries that copy data from json log file then load into
    staging tables
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' 
    TODO: Insert data from staging tables into fact and dim tables
    Looping through insert_table_queries array that include raw queries that insert into each dimension and fact tables 
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Connection to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()