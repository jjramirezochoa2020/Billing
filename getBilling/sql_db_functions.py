import os
import mysql.connector
import pandas as pd

#MySQL query constants
db_qa_conf = {'user':'root','password':'maria_db_q&p','host':'10.30.7.101','database':'collector_qa'}

#MySQL query constants

LOAD_HEADER = 'LOAD DATA LOCAL INFILE '
LOAD_FOOTER = """ FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"""
TRUN_HEADER = 'TRUNCATE TABLE '
TRUN_FOOTER = ';'
CONN_ERROR  = ' -- Connection error: please check network connection, VPN should be turned on'
EXEC_MESSG  = ' -- Rows affected by statement of {}: {}'

def db_connect(conn_cfg):
    """
    This function performs the connection to the DB using mysql.connector library

    Parameters
    ----------
    conn_cfg: dict
        DB connection parameters

    Returns
    -------
    conn_status: bool
        True if connection was successfull, False in other case
    db_conn: mysql.connector conn object
        A mysql.connector connection object
    """
    
    conn_status = False
    db_conn = None

    try:
        db_conn = mysql.connector.connect(user=conn_cfg['user'], password=conn_cfg['password'], host=conn_cfg['host'], database=conn_cfg['database'], allow_local_infile=True)
        conn_status = True
    except:
        print(CONN_ERROR)

    return conn_status, db_conn

def execute_query(conn_cfg, query):
    """
    This function executes a query
    
    Parameters
    ----------
    conn_cfg: dict
        DB connection parameters
    query: str
        SQL query to be executed

    Returns
    -------
    result: dataframe
        A table with the query results
    """
    
    result = None
    conn_status, db_conn = db_connect(conn_cfg)

    if conn_status:
        result = pd.read_sql(query, con=db_conn)
        db_conn.close()

    return result

        
def append_csv_data(conn_cfg, file_name, table_name):
    """
    This function performs append operation

    Parameters
    ----------
    conn_cfg: dict
        DB connection parameters
    file_name: str
        CSV file to be loaded
    table_name: str
        Destination table name

    Returns
    -------
    oper_status: bool
        True if operation was successfull, False in other case
    """

    oper_status = False
    conn_status, db_conn = db_connect(conn_cfg)

    if conn_status:
        cursor = db_conn.cursor()
        cursor.execute(LOAD_HEADER + '"{}" INTO TABLE '.format(file_name) + table_name + LOAD_FOOTER)
        db_conn.commit()
        db_conn.close()
        oper_status = True
        
    return oper_status
    
def cursor_execute(conn_cfg, query):
    """
    This function performs a cursor.execute operation
    
    Parameters
    ----------
    conn_cfg: dict
        DB connection parameters
    query: str
        SQL query to be executed

    Returns
    -------
    oper_status: bool
        True if operation was successfull, False in other case
    """

    oper_status = False
    conn_status, db_conn = db_connect(conn_cfg)

    if conn_status:
        cursor = db_conn.cursor()
        cursor.execute(query)
        db_conn.commit()
        db_conn.close()
        oper_status = True
        
    return oper_status

def replace_csv_data(conn_cfg, file_name, table_name):
    """
    This function performs a truncate operation followed by a load operation

    Parameters
    ----------
    conn_cfg: dict
        DB connection parameters
    file_name: str
        CSV file to be loaded
    table_name: str
        Destination table name

    Returns
    -------
    oper_status: bool
        True if operation was successfull, False in other case
    """
    
    oper_status = False
    conn_status, db_conn = db_connect(conn_cfg)

    if conn_status:
        cursor = db_conn.cursor()
        # Truncate statement
        cursor.execute(TRUN_HEADER + table_name + TRUN_FOOTER)
        # Load statement
        cursor.execute(LOAD_HEADER + '"{}" INTO TABLE '.format(file_name) + table_name + LOAD_FOOTER)
        print(EXEC_MESSG.format(table_name, cursor.rowcount))
        db_conn.commit()
        db_conn.close()
        oper_status = True
        
    return oper_status