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
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_last_m_datetime

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
    print(f"The {kwargs['task_type']} task has completed.")

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
    ########################################################################
    strexec_tmp = ("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS pro_rank_utime, DROP COLUMN IF EXISTS pro_first_purdate;" % (tb_to + '_tmp'))
    strexec = ("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS pro_rank_utime, DROP COLUMN IF EXISTS pro_first_purdate;" % (tb_to))
    # execute
    with engine_dl.connect() as conn:
        conn.execute(strexec_tmp)
        conn.execute(strexec)
        conn.close()
    ########################################################################
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
                #print(f"Upsert progress {index + 1}/{rows}")
                upsert(session,table,update_cols,row)
    
            print(f"Upsert Completed {rows} records.\n")
            session.commit()
            session.close()
            del df_main
            gc.collect()
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

def Append_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    c_size = kwargs['Chunk_Size']
    C_condition = kwargs['Condition']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        strexec = ("DELETE FROM %s WHERE (EXISTS (SELECT %s FROM %s WHERE %s.%s = %s.%s));") % (tb_to, primary_key, tb_to + '_tmp',tb_to,primary_key,tb_to + '_tmp',primary_key)
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

def branch_part_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="etl_kopen_part_data", key='return_value'))
    if xcom_value:
        return "check_row_part_on_data_warehouse"
    else:
        return "create_new_part_table"

def branch_select_func(**kwargs):
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    # check exiting table
    ctable = "SELECT COUNT(*) as c FROM %s;" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    print(result)
    row = result.loc[0,'c']
    if row < 300000:
        return "upsert_part_on_data_warehouse"
    else:
        return "append_part_on_data_warehouse"

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
    'Kopen_Part_Daily_db2postgres_dag',
    default_args=args,
    schedule_interval=None,
    #start_date=datetime(year=2022, month=6, day=1),
    #dagrun_timeout=timedelta(minutes=120),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    
    ################### PART ##########################################################################################################################
    # 1. Get the Part Data from a table in Kopen DB2
    task_ETL_Kopen_Part_data = PythonOperator(
        task_id='etl_kopen_part_data',
        provide_context=True,
        python_callable=ETL_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': " where pro_lasttime >= '%s'" % (get_last_m_datetime())}
    )

    # 2. Upsert Part Data To DATA Warehouse
    task_L_WH_Part = PythonOperator(
        task_id='upsert_part_on_data_warehouse',
        provide_context=True,
        python_callable= UPSERT_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode'}
    )

    # 3. Replace Part Data Temp Table
    task_RP_WH_Part = PythonOperator(
        task_id='create_new_part_table',
        provide_context=True,
        python_callable= INSERT_bluk,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode'}
    )

    # 4. Cleansing Part Data Table
    task_CL_WH_Part = PythonOperator(
        task_id='cleansing_part_data',
        provide_context=True,
        python_callable= Cleansing_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': " and pro_lasttime >= '%s'" % (get_last_m_datetime())}
    )
    # 5. Branch Part Data Table
    task_Part_Branch_op = BranchPythonOperator(
        task_id="check_existing_part_on_data_warehouse",
        python_callable=branch_part_func,
    )

    # 6. Branch Select Way
    task_Part_Branch_op_select = BranchPythonOperator(
        task_id="check_row_part_on_data_warehouse",
        python_callable=branch_select_func,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': " and pro_lasttime >= '%s'" % (get_last_m_datetime())}
    )

    branch_join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    branch_join1 = DummyOperator(
        task_id='join1',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # 7. Cleansing Part & Append Data Table
    task_AP_WH_Part = PythonOperator(
        task_id='append_part_on_data_warehouse',
        provide_context=True,
        python_callable= Append_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': " and pro_lasttime >= '%s'" % (get_last_m_datetime())}
    )

    start_task1 = PythonOperator(
        task_id='starting_task1',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    start_task2 = PythonOperator(
        task_id='starting_task2',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    way1 = branch_join << [task_L_WH_Part,task_AP_WH_Part] << task_Part_Branch_op_select
    way2 = start_task2 << start_task1 << task_RP_WH_Part

    task_ETL_Kopen_Part_data >> task_Part_Branch_op >> [way1,way2] >> branch_join1 >> task_CL_WH_Part
