import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads JSON data from S3 into staging tables 
    using queries in copy_table_queries variable from sql_queries.py
    Args:
        cur: psycopg2 cursor object 
        conn: psycopg2 database connection object 
    """
    print ('\nLoading staging tables...\n')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into dimensional tables 
    to optimize analytics queries
    Args:
        cur: psycopg2 cursor object 
        conn: psycopg2 database connection object 
    """
    print ('Inserting data...\n')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Runs all functions in the etl.py file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()