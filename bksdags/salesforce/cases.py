from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator, BranchPythonOperator
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
#from function import get_yesterday
from Class import common
from function import get_firstdate_this_m, get_today

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
    # Start Session
    Base = declarative_base()
    session = sessionmaker(bind=engine)
    session = session()
    Base.metadata.create_all(engine)

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    schema = 'public'
    no_update_cols = []

    print('Initial state:\n')
    df_main = pd.read_sql_query(sql=sqlalchemy.text("select * from %s_tmp" % (tb_to)), con=engine)
    rows = len(df_main)
    if (rows > 0):
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
    primary_key = kwargs['Key']
    strexec = ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
    strexec += ("ALTER TABLE %s ADD PRIMARY KEY (%s);" % (tb_to, primary_key))
    # execute
    with engine.connect() as conn:
        conn.execute(strexec)
        print("ETL DL Process finished")
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

def Check_exiting(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        return True
    else:
        return False   

def branch_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="check_exit_sf_cases_data", key='return_value'))
    if xcom_value:
        return "upsert_sf_cases_on_data_lake"
    else:
        return "create_new_sf_cases_table"

with DAG(
    'Salesforce_Cases_ETL_dag',
    tags=['Salesforce'],
    schedule_interval=None,
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='bks_api',
        endpoint='genreport/',
        execution_timeout=timedelta(seconds=120),
        timeout=3600,
        retries=3,
        mode="reschedule",
    )

    # 2. Call API Salesforce Load to Datalaken Temp Table
    task_Get_Salesforce_Data_Save_To_Datalake = SimpleHttpOperator(
        task_id='get_salesforce_cases_object',
        http_conn_id='bks_api',
        method='GET',
        endpoint='etl/sf/sfcases',
        data={"stdate": get_firstdate_this_m(), "edate": get_today()},
        log_response=True
    )

    # 3. Upsert Salesforce To DATA Lake
    task_L_DL_SF_cases = PythonOperator(
        task_id='upsert_sf_cases_on_data_lake',
        provide_context=True,
        python_callable= UPSERT_process,
        op_kwargs={'To_Table': "sf_cases", 'Chunk_Size': 5000, 'Key': 'id', 'Condition': ""}
    )

    # 4. Replace Salesforce Temp Table
    task_RP_DL_SF_cases = PythonOperator(
        task_id='create_new_sf_cases_table',
        provide_context=True,
        python_callable= INSERT_bluk,
        op_kwargs={'To_Table': "sf_cases", 'Chunk_Size': 5000, 'Key': 'id', 'Condition': ""}
    )

    # 5. Cleansing Salesforce Table
    task_CL_DL_SF_cases = PythonOperator(
        task_id='cleansing_sf_cases_data',
        provide_context=True,
        python_callable= Cleansing_process,
        op_kwargs={'To_Table': "sf_cases", 'Chunk_Size': 5000, 'Key': 'id', 'Condition': ""}
    )

    # 6. Check Exiting Salesforce Table
    task_CK_DL_SF_cases = PythonOperator(
        task_id='check_exit_sf_cases_data',
        provide_context=True,
        python_callable= Check_exiting,
        op_kwargs={'To_Table': "sf_cases", 'Chunk_Size': 5000, 'Key': 'id', 'Condition': ""}
    )

    branch_op = BranchPythonOperator(
        task_id="check_existing_sf_cases_on_data_lake",
        python_callable=branch_func,
    )

    branch_join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    task_is_api_active >> task_Get_Salesforce_Data_Save_To_Datalake >> task_CK_DL_SF_cases >> branch_op >> [task_L_DL_SF_cases, task_RP_DL_SF_cases] >> branch_join >> task_CL_DL_SF_cases