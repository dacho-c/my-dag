import time
import pandas as pd
import configparser
from sqlalchemy import create_engine, delete
import datetime
from airflow.models import Variable

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from sql import sql_detail_select, sql_detail_delete

def get_last_ym():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    return lastMonth.strftime("%Y%m")

def get_this_ym():
    today = datetime.date.today()
    return today.strftime("%Y%m")

def get_today():
    today = datetime.date.today()
    return today.strftime("%Y-%m-%d")

def get_yesterday():
    today = datetime.date.today()
    yday = today - datetime.timedelta(days=1)
    return yday.strftime("%Y-%m-%d")

def get_firstdate_this_m():
    today = datetime.date.today()
    first = today.replace(day=1)
    return first.strftime("%Y-%m-%d")

def get_lastdate_this_m():
    today = datetime.date.today()
    p = pd.Period(today.strftime("%Y-%m-%d"))
    lday = p.days_in_month
    last = today.replace(day=lday)
    return last.strftime("%Y-%m-%d")

def get_firstdate_last_m():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    first = lastMonth.replace(day=1)
    return first.strftime("%Y-%m-%d")

def get_lastdate_last_m():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    return lastMonth.strftime("%Y-%m-%d")

# get fisical year

def get_pg_connection():
    config = configparser.ConfigParser()
    config.read(Variable.get('db2pg_config'))
    pgdatabase = config['PG']['database']
    pghost = config['PG']['host']
    pgport = config['PG']['port']
    pguid = config['PG']['uid']
    pgpwd = config['PG']['pwd']
    # Connection String to Postgres DATA Lake
    pgstrcon = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
    return pgstrcon

def get_db2_connection():
    config = configparser.ConfigParser()
    config.read(Variable.get('db2pg_config'))
    db2database = config['DB2']['database']
    db2host = config['DB2']['host']
    db2port = config['DB2']['port']
    db2protocal = config['DB2']['protocal']
    db2uid = config['DB2']['uid']
    db2pwd = config['DB2']['pwd']
    # Connection String to EGKopen db2
    db2strcon = "db2://%s:%s@%s:%s/%s" % (db2uid, db2pwd, db2host, db2port, db2database)
    return db2strcon

def read_load_save_data(**kwargs): 

    db2strcon = get_db2_connection

    pgstrcon = get_pg_connection

    # Create SQLAlchemy engine
    engine_pg = create_engine(pgstrcon,client_encoding="utf8")
    engine_db2 = create_engine(db2strcon)
    conn_db2 = engine_db2.connect().execution_options(stream_results=True)

    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']

    start_time = time.time()
    n = 0
    rows = 0
    sqlstr = "SELECT * FROM db2admin." + tb_from

    for chunk_df in pd.read_sql(sqlstr, conn_db2, chunksize=c_size):
        rows += len(chunk_df)
        print(f"Got dataframe {rows}/All rows")
        # Load to DB-LAKE not transfrom
        if n == 0:
            chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='replace')
            n = n + 1
        else:
            chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
            print(f"Already Save to data lake {rows} rows")
    print("EL Process finished")
    end_time = time.time()
    print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
    return True

def delete_before_append(**kwargs):

    pgstrcon = get_pg_connection

    # Create SQLAlchemy engine
    engine_pg = create_engine(pgstrcon,client_encoding="utf8")

    pgtb = kwargs['To_Table']
    pgcondition = kwargs['Condition']

    sqlstr = "DELETE FROM %s WHERE %s" % (pgtb, pgcondition)
    engine_pg.execute(sqlstr)
    return True

def delete_before_append_detail(**kwargs):

    pgstrcon = get_pg_connection

    # Create SQLAlchemy engine
    engine_pg = create_engine(pgstrcon,client_encoding="utf8")

    pgtb = kwargs['To_Table']

    sqlstr = sql_detail_delete(pgtb)
    engine_pg.execute(sqlstr)
    return True

def read_load_update_data(**kwargs): 

    db2strcon = get_db2_connection

    pgstrcon = get_pg_connection

    # Create SQLAlchemy engine
    engine_pg = create_engine(pgstrcon,client_encoding="utf8")
    engine_db2 = create_engine(db2strcon)
    conn_db2 = engine_db2.connect().execution_options(stream_results=True)

    tb_from = kwargs['From_Table']
    condition = kwargs['Condition']
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']

    start_time = time.time()
    n = 0
    rows = 0
    sqlstr = "SELECT * FROM db2admin.%s WHERE %s" % (tb_from, condition)

    for chunk_df in pd.read_sql(sqlstr, conn_db2, chunksize=c_size):
        rows += len(chunk_df)
        print(f"Got dataframe {rows}/All rows")
        # Load to DB-LAKE not transfrom
        if n == 0:
            delete_before_append(**kwargs)
            chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
            print(f"Already Update to data lake {rows} rows")
            n = n + 1
        else:
            chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
            print(f"Already Update to data lake {rows} rows")
    print("EL Process finished")
    end_time = time.time()
    print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
    return True

def read_load_update_detail_data(**kwargs): 

    db2strcon = get_db2_connection

    pgstrcon = get_pg_connection

    # Create SQLAlchemy engine
    engine_pg = create_engine(pgstrcon,client_encoding="utf8")
    engine_db2 = create_engine(db2strcon)
    conn_db2 = engine_db2.connect().execution_options(stream_results=True)

    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']

    start_time = time.time()
    n = 0
    rows = 0
    sqlstr = sql_detail_select(tb_from)

    for chunk_df in pd.read_sql(sqlstr, conn_db2, chunksize=c_size):
        rows += len(chunk_df)
        print(f"Got dataframe {rows}/All rows")
        # Load to DB-LAKE not transfrom
        if n == 0:
            delete_before_append_detail(**kwargs)
            chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
            print(f"Already Update to data lake {rows} rows")
            n = n + 1
        else:
            chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
            print(f"Already Update to data lake {rows} rows")
    print("EL Process finished")
    end_time = time.time()
    print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
    return True