from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

import pandas as pd
import sqlalchemy
from sqlalchemy import Column, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect
import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from sql import sql_stock
from function import get_firstdate_this_m

def ETL_process(**kwargs):

    db2strcon = common.get_db2_connection('')
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine_dl = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    engine_db2 = sqlalchemy.create_engine(db2strcon)

    n = 0
    rows = 0
    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']
    C_condition = kwargs['Condition']

    ETL_Status = False
    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine_dl)
    if result.loc[0,'exists']:
        ETL_Status = True
    
    sqlstr_main = sql_stock() + C_condition

    for df_main in pd.read_sql_query(sql=sqlalchemy.text(sqlstr_main), con=engine_db2, chunksize=c_size):
        rows += len(df_main)
        print(f"Got dataframe w/{rows} rows")
        # Load & transfrom
        ##################
        if n == 0:
            df_main.to_sql(tb_to + "_tmp", engine_dl, index=False, if_exists='replace')
            n = n + 1
        else:
            df_main.to_sql(tb_to + "_tmp", engine_dl, index=False, if_exists='append')
            n = n + 1
        print(f"Save to data W/H {rows} rows")
    print("ETL Process finished")

    return ETL_Status

def upsert(session, table, update_cols, rows):

    stmt = insert(table).values(rows)

    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=table.primary_key.columns,
        set_={k: getattr(stmt.excluded, k) for k in update_cols}
        #,
        #index_where= ( as_of_date_col < getattr(stmt.excluded, as_of_date_col))
        )
    session.execute(on_conflict_stmt)

def UPSERT_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']
    schema = 'public'
    no_update_cols = []

    print('Initial state:\n')
    for df_main in pd.read_sql_query(sql=sqlalchemy.text("select * from %s_tmp" % (tb_to)), con=engine, chunksize=c_size):
        rows = len(df_main)
        if (rows > 0):
            # Start Session ########################
            Base = declarative_base()
            session = sessionmaker(bind=engine)
            session = session()
            Base.metadata.create_all(engine)
            ########################################
            metadata = MetaData(schema=schema)
            metadata.bind = engine
            table = Table(tb_to, metadata, schema=schema, autoload=True)
            update_cols = [c.name for c in table.c
                if c not in list(table.primary_key.columns)
                and c.name not in no_update_cols]

            for index, row in df_main.iterrows():
                print(f"Upsert progress {index + 1}/{rows}")
                upsert(session,table,update_cols,row)
    
            print(f"Upsert Completed {rows} records.\n")
            session.commit()
            session.close()
    print('Upsert session commit')

def INSERT_bluk(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    strexec = ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
    strexec += ("ALTER TABLE %s ADD PRIMARY KEY (item_id);" % (tb_to))
    # execute
    with engine.connect() as conn:
        conn.execute(strexec)
        print("ETL WH Process finished")
        conn.close()

def Cleansing_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    C_condition = kwargs['Condition']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        strexec = ("DELETE FROM %s WHERE NOT (EXISTS (SELECT %s FROM %s WHERE %s.%s = %s.%s)) %s;") % (tb_to, primary_key, tb_to + '_tmp',tb_to,primary_key,tb_to + '_tmp',primary_key,C_condition)
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            print("Drop Temp Table.")
            conn.execute("DROP TABLE IF EXISTS %s;" % (tb_to + '_tmp'))
            conn.close()
        
def branch_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="etl_kopen_part_stock_data", key='return_value'))
    if xcom_value:
        return "upsert_part_stock_on_data_warehouse"
    else:
        return "create_new_part_stock_table"

with DAG(
    dag_id='Kopen_Stock_db2postgres_dag',
    schedule_interval='30 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Part Stock from a table in Kopen DB2
    task_ETL_Kopen_part_stock_data = PythonOperator(
        task_id='etl_kopen_part_stock_data',
        provide_context=True,
        python_callable=ETL_process,
        op_kwargs={'From_Table': "stock", 'To_Table': "kp_stock", 'Chunk_Size': 50000, 'Key': 'item_id', 'Condition': " where db2admin.stock.ST_LASTTIME >= '%s 00:00:00'" % (get_firstdate_this_m())}
    )

    # 2. Upsert Part Stock To DATA Warehouse
    task_L_WH_Service_Job = PythonOperator(
        task_id='upsert_part_stock_on_data_warehouse',
        provide_context=True,
        python_callable= UPSERT_process,
        op_kwargs={'From_Table': "stock", 'To_Table': "kp_stock", 'Chunk_Size': 20000, 'Key': 'item_id'}
    )

    # 3. Replace Part Stock Temp Table
    task_RP_WH_Service_Job = PythonOperator(
        task_id='create_new_part_stock_table',
        provide_context=True,
        python_callable= INSERT_bluk,
        op_kwargs={'From_Table': "stock", 'To_Table': "kp_stock", 'Chunk_Size': 50000, 'Key': 'item_id'}
    )

    # 4. Cleansing Part Stock Table
    task_CL_WH_Service_Job = PythonOperator(
        task_id='cleansing_part_stock_data',
        provide_context=True,
        python_callable= Cleansing_process,
        op_kwargs={'From_Table': "stock", 'To_Table': "kp_stock", 'Chunk_Size': 50000, 'Key': 'item_id', 'Condition': " and st_lasttime >= '%s 00:00:00'" % (get_firstdate_this_m())}
    )

    branch_op = BranchPythonOperator(
        task_id="check_existing_part_stock_on_data_warehouse",
        python_callable=branch_func,
    )

    branch_join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    task_ETL_Kopen_part_stock_data >> branch_op >> [task_L_WH_Service_Job,task_RP_WH_Service_Job] >> branch_join >> task_CL_WH_Service_Job