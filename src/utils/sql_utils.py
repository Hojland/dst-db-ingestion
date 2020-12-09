import sqlalchemy
import pandas as pd
import os
from tqdm import tqdm
import sys
from sqlalchemy.types import String, Integer, Numeric
from typing import List, Dict
from os.path import exists, join, abspath
import logging
import re
import aiomysql
import asyncio

from utils import utils
import settings


def create_engine(db_config: dict, db_name: str=None, db_type: str='postgres', **kwargs):
    """Creates a sqlalchemy engine, with specified connection information

    Arguments
    ---------
    db_config: a dictionary with configurations for a resource
    db_name: Overwrite the database from the config
    db_type: a string with the database type for prepending the connection string
    kwargs: parameters to pass onto create engine

    Returns
    -------
    engine: sqlalchemy.Engine
    """
    if db_type == 'postgres':
       prepend = 'postgresql+psycopg2'
    elif db_type == 'mssql':
       prepend = 'mssql+pyodbc'
    elif db_type == 'mysql' or db_type == 'mariadb':
       prepend = 'mysql+mysqldb'

    uid, psw, host, port, db = db_config.values()
    if db_name:
       db = db_name
    conn_string = f"{prepend}://{uid}:{psw}@{host}:{port}/{db}"

    if db_type == 'mssql':
        driverfile = '/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so'
        conn_string = conn_string + f"?DRIVER={driverfile};TDS_VERSION=7.2"
    elif db_type == 'mysql':
        conn_string = conn_string + '?charset=utf8'

    engine = sqlalchemy.create_engine(conn_string, **kwargs)
    return engine

async def async_mysql_create_engine(loop, db_config: dict, db_name: str=None):
    uid, psw, host, port, db = db_config.values()
    if db_name:
       db = db_name
    pool = await aiomysql.create_pool(user=uid,
                                      db=db,
                                      host=host,
                                      port=port,
                                      password=psw,
                                      loop=loop,
                                      autocommit=True)
    return pool


async def create_table(mysql_engine_pool: aiomysql.Pool, table_name: str, col_datatype_dct: dict, primary_key: str=None, index_lst: list=None, foreignkey_ref_dct: dict=None):
    #primary_key = "id INT AUTO_INCREMENT PRIMARY KEY"
    def_strings = []
    col_definition_str =  ', '.join([f"{k} {v}" for k, v in col_datatype_dct.items()])
    if primary_key:
        col_definition_str = primary_key + ', ' + col_definition_str
    def_strings.append(col_definition_str)
    if foreignkey_ref_dct:
        foreign_key_strs = [f'FOREIGN KEY ({k}) REFERENCES {v}' for k,v in foreignkey_ref_dct.items()]
        foreign_str = ", ".join(foreign_key_strs)
        def_strings.append(foreign_str)
    if index_lst:
        index_str = ", ".join([f'INDEX ({index})' for index in index_lst])
        def_strings.append(index_str)

    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({','.join(def_strings)});"""

    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(create_table_query)    
    await cur.close()
    await mysql_engine_pool.release(conn)

async def df_to_sql_split(mysql_engine_pool: aiomysql.Pool, df: pd.DataFrame, table_name: str, chunksize: int=50):
    for i in range(0, len(df), chunksize):
        await df_to_sql(mysql_engine_pool, df.iloc[i:i+chunksize],table_name)

async def df_to_sql(mysql_engine_pool: aiomysql.Pool, df: pd.DataFrame, table_name: str):
    def delete_quotation(str: str):
        return str.replace("'","").replace('"','')

    df = df.astype(str)
    df_values = [[delete_quotation(value) for value in values] for values in df.values]
    sql_query_start = f'INSERT INTO {table_name}'
    column_str = ','.join(list(df))
    values_str = ','.join([f'''('{"','".join(values)}')''' for values in df_values])
    values_str = utils.multiple_replace({"'nan'": 'NULL', "'<NA>'": 'NULL'}, values_str)

    sql_query = f"{sql_query_start} ({column_str}) VALUES {values_str}"
    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(sql_query)
    await cur.close()
    await mysql_engine_pool.release(conn)

async def get_latest_date_in_table(mysql_engine_pool: aiomysql.Pool, table_name: str, date_col: str='date'):
    sql_query = f'SELECT MAX({date_col}) FROM {table_name}'
    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(sql_query)
    (latest_date, ) = await cur.fetchone()

    await cur.close()
    await mysql_engine_pool.release(conn)

    if not latest_date:
        raise IndexError(f"No data in variable '{date_col}' in table")
    return latest_date

def delete_index_from_table(db_engine: sqlalchemy.engine, index_dct: dict, table_name: str):
    index_string = ' AND '.join(f"{key}='{value}'" for key, value in index_dct.items())
    sql_query = f'DELETE FROM output.{table_name} WHERE {index_string}'
    db_engine.execute(sql_query)

def delete_date_entries_in_table(db_engine: sqlalchemy.engine, min_date: str, table_name: str):
    db_engine.execute(f'DELETE FROM output.{table_name} WHERE date>="{min_date}";')

def delete_table(db_engine: sqlalchemy.engine, table: str):
    db_engine.execute(f'DROP TABLE {table}')

def truncate_table(db_engine: sqlalchemy.engine, table: str):
    db_engine.execute(f'TRUNCATE TABLE {table}')

async def table_exists(mysql_engine_pool: aiomysql.pool, schema_name: str, table_name: str):
    sql_query = f'''
    SELECT EXISTS (SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND  TABLE_NAME = '{table_name}')
    '''

    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(sql_query)
    (exists_num, ) = await cur.fetchone()

    await cur.close()
    await mysql_engine_pool.release(conn)
    if exists_num == 0:
        exists = False
    elif exists_num == 1:
        exists = True
    return exists

async def several_updates_table(mysql_engine_pool: aiomysql.Pool, table_name: str, update_df: pd.DataFrame, index_df: pd.DataFrame):
    assert len(index_df) == len(update_df), 'index_df and update_df is not the same length'
    for i in range(len(index_df)):
        try: 
            update_dct = update_df.iloc[i].to_dict()
        except AttributeError: 
            update_dct = {update_df.name: update_df.iloc[i]}

        try: 
            index_dct = index_df.iloc[i].to_dict()
        except AttributeError: 
            index_dct = {index_df.name: index_df.iloc[i]}

        await update_table(mysql_engine_pool, table_name, update_dct, index_dct)

async def update_table(mysql_engine_pool: aiomysql.Pool, table_name: str, update_dct: dict, index_dct: dict):
    update_string = ', '.join(f"{key}='{value}'" for key, value in update_dct.items())
    replace_dct = {
        "'nat'": 'NULL',
        "'nan'": 'NULL',
    }
    update_string = utils.multiple_replace(replace_dct, update_string, flags=re.IGNORECASE)
    index_string = ' AND '.join(f"{key}='{value}'" for key, value in index_dct.items())
    sql_query = f'UPDATE {table_name} SET {update_string} WHERE {index_string}'
    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(sql_query)
    await cur.close()
    await mysql_engine_pool.release(conn)

def view_exists(db_engine: sqlalchemy.engine, schema: str, view: str, sql_lang: str='mysql'):
    base_sql_query = f'''EXISTS (SELECT * 
                         FROM INFORMATION_SCHEMA.VIEWS
                         WHERE TABLE_SCHEMA = '{schema}' 
                         AND  TABLE_NAME = '{view}')'''

    if sql_lang == 'mysql':
        exists_num = db_engine.execute(f'''
        SELECT {base_sql_query}
        ''').scalar()
    elif sql_lang == 'mssql':
        exists_num = db_engine.execute(f'''
        SELECT
            CASE
                WHEN
                    {base_sql_query}
                    THEN 1 
                ELSE 0 
            END
        ''').scalar()
    if exists_num == 0:
        exists = False
    elif exists_num == 1:
        exists = True
    return exists

async def table_empty(mysql_engine_pool: aiomysql.pool, table_name: str):
    sql_query = f'SELECT EXISTS(SELECT 1 FROM {table_name})'
    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(sql_query)
    (empty_num, ) = await cur.fetchone()

    await cur.close()
    await mysql_engine_pool.release(conn)
    if empty_num == 0:
        empty = False
    elif empty_num == 1:
        empty = True
    return empty

async def table_exists_notempty(mysql_engine_pool: str, schema_name: str, table_name: str):
    exists = await table_exists(mysql_engine_pool, schema_name, table_name)
    if exists:
        empty = await table_empty(mysql_engine_pool, f"{schema_name}.{table_name}")
        if empty:
            both = True
        else:
            both = False
    else:
        both = False
    return both

def table_index_exists(db_engine: sqlalchemy.engine, schema: str, table: str, index_name: str=None):
    sql_query = f'''
    SELECT COUNT(1) as IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS
    WHERE table_schema='{schema}' AND table_name='{table}'
    '''
    if index_name:
        sql_query = sql_query + f" AND index_name='{index_name}'"

    index_exists_num = db_engine.execute(sql_query).scalar()
    if index_exists_num == 0:
        index_exists = False
    elif index_exists_num > 0:
        index_exists = True
    else:
        index_exists = False
    return index_exists

async def col_dtypes(mysql_engine_pool: aiomysql.pool, schema_name: str, table_name: str):
    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(f"SELECT column_name, data_type FROM information_schema.columns where table_schema = '{schema_name}' and table_name='{table_name}'")
    res = await cur.fetchall()
    await cur.close()
    await mysql_engine_pool.release(conn)
    col_dtypes = {column_name: data_type for column_name, data_type in res}
    return col_dtypes

def load_data(engine: sqlalchemy.engine, sql_query: str):
    df_load = pd.read_sql(sql_query, engine, chunksize=20000)
    try:
        df = pd.concat([chunk for chunk in tqdm(df_load, desc='Loading data', file=sys.stdout)], ignore_index=True)
    except ValueError:
        logging.error('No data in sql query table')
        df = pd.DataFrame()
    return df


def get_dtype_trans(df: pd.DataFrame, str_len: int=150):
    obj_vars = [colname for colname in list(df) if df[colname].dtype == 'object']
    int_vars = [colname for colname in list(df) if df[colname].dtype == 'int64']
    float_vars = [colname for colname in list(df) if df[colname].dtype == 'float64']
    date_vars = [colname for colname in list(df) if df[colname].dtype == 'datetime64[ns]']

    dtype_trans = {
        obj_var: f"VARCHAR({str_len})" for obj_var in obj_vars
    }
    dtype_trans.update({
        int_var: "INT" for int_var in int_vars
    })
    dtype_trans.update({
        float_var: "FLOAT(14, 5)" for float_var in float_vars
    })
    dtype_trans.update({
        date_var: "DATE" for date_var in date_vars
    })
    return dtype_trans