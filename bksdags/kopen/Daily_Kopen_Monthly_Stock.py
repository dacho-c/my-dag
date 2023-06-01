from asyncio import Task
from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator, BranchPythonOperator
#from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

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
import gc
sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from Class import common
from function import get_last_ym

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
    
    sqlstr_main = "SELECT * FROM db2admin." + tb_from + C_condition

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
        del df_main
        gc.collect()
    print("ETL Process finished")
    return ETL_Status

def INSERT_bluk(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    strexec = ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
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

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        # execute
        with engine.connect() as conn:
            print("Drop Temp Table.")
            conn.execute("DROP TABLE IF EXISTS %s;" % (tb_to + '_tmp'))
            conn.close()

def Append_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']
    C_condition = kwargs['Condition']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        strexec = ("DELETE FROM %s %s;") % (tb_to, C_condition)
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            conn.close()
        for df_main in pd.read_sql_query(sql=sqlalchemy.text("select * from %s_tmp" % (tb_to)), con=engine, chunksize=c_size):
            rows = len(df_main)
            if (rows > 0):
                print(f"Got dataframe w/{rows} rows")
                # Load & transfrom
                ##################
                df_main.to_sql(tb_to, engine, index=False, if_exists='append')
                print(f"Save to data W/H {rows} rows")
            del df_main
            gc.collect()
        print("Append Process finished")

def branch_monthly_stock_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="etl_kopen_monthly_stock_data", key='return_value'))
    if xcom_value:
        return "append_monthly_stock_on_data_warehouse"
    else:
        return "create_new_monthly_stock_table"

args = {
        'owner': 'airflow',    
        #'start_date': airflow.utils.dates.days_ago(2),
        # 'end_date': datetime(),
        'depends_on_past': False,
        #'email': ['airflow@example.com'],
        #'email_on_failure': False,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    'Kopen_Monthly_Stock_Daily_db2postgres_dag',
    default_args=args,
    schedule_interval=None,
    #start_date=datetime(year=2022, month=6, day=1),
    #dagrun_timeout=timedelta(minutes=120),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    
    ################### Monthly ##########################################################################################################################
    # 1. Get the Monthly Stock Data from a table in Kopen DB2
    task_ETL_Kopen_Monthly_Stock_data = PythonOperator(
        task_id='etl_kopen_monthly_stock_data',
        provide_context=True,
        python_callable=ETL_process,
        op_kwargs={'From_Table': "MONTHLY_STOCK_ANALYSIS", 'To_Table': "kp_monthly_stock_analysis", 'Chunk_Size': 20000, 'Condition': " where mta_month >= '%s'" % (get_last_ym())}
    )

    # 3. Replace Monthly Stock Data Temp Table
    task_RP_WH_Monthly_Stock = PythonOperator(
        task_id='create_new_monthly_stock_table',
        provide_context=True,
        python_callable= INSERT_bluk,
        op_kwargs={'From_Table': "MONTHLY_STOCK_ANALYSIS", 'To_Table': "kp_monthly_stock_analysis", 'Chunk_Size': 20000}
    )

    # 4. Cleansing Monthly Stock Data Table
    task_CL_WH_Monthly_Stock = PythonOperator(
        task_id='cleansing_monthly_stock_data',
        provide_context=True,
        python_callable= Cleansing_process,
        op_kwargs={'From_Table': "MONTHLY_STOCK_ANALYSIS", 'To_Table': "kp_monthly_stock_analysis", 'Chunk_Size': 20000}
    )

    # 5. Branch Monthly Stock Data Table
    task_Monthly_Stock_Branch_op = BranchPythonOperator(
        task_id="check_existing_monthly_stock_on_data_warehouse",
        python_callable=branch_monthly_stock_func,
    )

    branch_join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # 7. Append Monthly Stock & Append Data Table
    task_AP_WH_Monthly_Stock = PythonOperator(
        task_id='append_monthly_stock_on_data_warehouse',
        provide_context=True,
        python_callable= Append_process,
        op_kwargs={'From_Table': "MONTHLY_STOCK_ANALYSIS", 'To_Table': "kp_monthly_stock_analysis", 'Chunk_Size': 20000, 'Condition': " where mta_month >= '%s'" % (get_last_ym())}
    )

    task_ETL_Kopen_Monthly_Stock_data >> task_Monthly_Stock_Branch_op >> [task_AP_WH_Monthly_Stock,task_RP_WH_Monthly_Stock] >> branch_join >> task_CL_WH_Monthly_Stock
