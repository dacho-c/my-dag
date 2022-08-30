from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
from pgsql_machinedelivery import sql_ET_machine_cust, sql_ET_machine, sql_ET_invoice

def ETL_process(**kwargs):

    whstrcon = common.get_wh_connection('')
    dlstrcon = common.get_dl_connection('')
    # Create SQLAlchemy engine
    engine_dl = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    engine_wh = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")
    #conn_dl = engine_dl.connect().execution_options(stream_results=True)
    ETL_Status = False
    cstr = ""
    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine_wh)
    if result.loc[0,'exists']:
        ETL_Status = True
        cstr = " and DATE(UR_LASTTIME) >= (current_date - 31)"
    
    sqlstr_main = sql_ET_machine_cust(cstr)
    sqlstr_mc = sql_ET_machine()
    sqlstr_inv = sql_ET_invoice()

    df_mc = pd.read_sql_query(sqlstr_mc, con=engine_dl)
    df_inv = pd.read_sql_query(sqlstr_inv, con=engine_dl)

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']

    for df_main in pd.read_sql_query(sql=sqlalchemy.text(sqlstr_main), con=engine_dl, chunksize=c_size):
        rows += len(df_main)
        print(f"Got dataframe w/{rows} rows")
        # Load & transfrom
        df_miam = df_main.reset_index()
        for index, row in df_main.iterrows():
            rslt_df = df_mc.loc[(df_mc['ur_cus'] == row['cus_id']) & (df_mc['mcfy'] < row['mc_fy'])]
            df_main.loc[index,'mc_bfp'] = len(rslt_df)
            if len(rslt_df) == 0:
                rslt_df1 = df_inv.loc[(df_inv['pih_cus_id'] == row['cus_id']) & (df_inv['invfy'] < row['mc_fy'])]
                df_main.loc[index,'inv_bfp'] = len(rslt_df1)
                if len(rslt_df1) > 0:
                    df_main.loc[index,'cus_group'] = 'Old'
                else:
                    df_main.loc[index,'cus_group'] = 'New'
            else:
                df_main.loc[index,'cus_group'] = 'Old'
        ##################
        if n == 0:
            df_main.to_sql(tb_to + "_tmp", engine_wh, index=False, if_exists='replace')
            n = n + 1
        else:
            df_main.to_sql(tb_to + "_tmp", engine_wh, index=False, if_exists='append')
            n = n + 1
        print(f"Save to data W/H {rows} rows")
    print("ETL Process finished")

    return ETL_Status

def upsert(session, engine, schema, table_name, rows, no_update_cols=[]):
    
    metadata = MetaData(schema=schema)
    metadata.bind = engine

    table = Table(table_name, metadata, schema=schema, autoload=True)

    stmt = insert(table).values(rows)

    update_cols = [c.name for c in table.c
        if c not in list(table.primary_key.columns)
        and c.name not in no_update_cols]

    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=table.primary_key.columns,
        set_={k: getattr(stmt.excluded, k) for k in update_cols}
        #,
        #index_where= ( as_of_date_col < getattr(stmt.excluded, as_of_date_col))
        )
    session.execute(on_conflict_stmt)

def UPSERT_process(**kwargs):
    
    whstrcon = common.get_wh_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")
    # Start Session
    Base = declarative_base()
    session = sessionmaker(bind=engine)
    session = session()
    Base.metadata.create_all(engine)

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']

    print('Initial state:\n')
    df_main = pd.read_sql_query(sql=sqlalchemy.text("select * from machine_delivery_tmp"), con=engine)
    rows = len(df_main)
    for index, row in df_main.iterrows():
        print(f"Upsert progress {index + 1}/{rows}")
        upsert(session,engine,'public','machine_delivery',row,[])
    
    print(f"Upsert Completed {rows} records.\n")
    session.commit()
    print('Upsert session commit.\n')
    strexec = ("DROP TABLE IF EXISTS %s;" % (tb_to + '_tmp'))
    # execute
    with engine.connect() as conn:
        conn.execute(strexec)
        print("Drop Temp Table.")

def INSERT_bluk(**kwargs):
    
    whstrcon = common.get_wh_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    strexec = ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
    strexec += ("ALTER TABLE %s ADD PRIMARY KEY (item_id);" % (tb_to))
    strexec += ("DROP TABLE IF EXISTS %s;" % (tb_to + '_tmp'))
    # execute
    with engine.connect() as conn:
        conn.execute(strexec)
    print("ETL WH Process finished")

def branch_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="et_machinedelivery_data", key='return_value'))
    if xcom_value:
        return "l_machinedelivery_data"
    else:
        return "rp_machinedelivery_data"

with DAG(
    dag_id='DWH_ETL_Machine_Delivery_dag',
    schedule_interval='30 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Machine Delivery from a DATA LAKE To DATA Warehouse
    task_ET_WH_MachineDelivery = PythonOperator(
        task_id='et_machinedelivery_data',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "machine_delivery", 'Chunk_Size': 2000}
    )

    # 2. Upsert Machine Delivery To DATA Warehouse
    task_L_WH_MachineDelivery = PythonOperator(
        task_id='l_machinedelivery_data',
        provide_context=True,
        python_callable= UPSERT_process,
        op_kwargs={'To_Table': "machine_delivery"}
    )

    # 3. Replace Machine Delivery Temp Table
    task_RP_WH_MachineDelivery = PythonOperator(
        task_id='rp_machinedelivery_data',
        provide_context=True,
        python_callable= INSERT_bluk,
        op_kwargs={'To_Table': "machine_delivery"}
    )

    branch_op = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_func,
    )


    task_ET_WH_MachineDelivery >> branch_op >> [task_L_WH_MachineDelivery, task_RP_WH_MachineDelivery]