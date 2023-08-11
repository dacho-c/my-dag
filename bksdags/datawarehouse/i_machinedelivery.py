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
from pgsql_machinedelivery import sql_ET_machine_cust, sql_ET_machine, sql_ET_invoice

def ETL_process(**kwargs):

    whstrcon = common.get_wh_connection('')
    dlstrcon = common.get_dl_connection('')
    # Create SQLAlchemy engine
    engine_dl = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    engine_wh = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']
    condition = kwargs['Condition']

    ETL_Status = False
    cstr = ""
    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine_wh)
    if result.loc[0,'exists']:
        ETL_Status = True
        cstr = condition
    
    sqlstr_main = sql_ET_machine_cust(cstr)
    sqlstr_mc = sql_ET_machine()
    sqlstr_inv = sql_ET_invoice()

    df_mc = pd.read_sql_query(sqlstr_mc, con=engine_dl)
    df_inv = pd.read_sql_query(sqlstr_inv, con=engine_dl)

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
            df_main.to_sql(tb_to, engine_wh, index=False, if_exists='replace')
            n = n + 1
        else:
            df_main.to_sql(tb_to, engine_wh, index=False, if_exists='append')
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
    
    whstrcon = common.get_wh_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    strexec = ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
    strexec += ("ALTER TABLE %s ADD PRIMARY KEY (item_id);" % (tb_to))
    # execute
    with engine.connect() as conn:
        conn.execute(strexec)
        print("ETL WH Process finished")
        conn.close()

def Cleansing_process(**kwargs):
    
    whstrcon = common.get_wh_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")

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
    xcom_value = bool(ti.xcom_pull(task_ids="extract_transform_machine_delivery_from_data_lake", key='return_value'))
    if xcom_value:
        return "upsert_machine_delivery_on_data_warehouse"
    else:
        return "create_new_machine_delivery_table"

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
    print(f"The {kwargs['task_type']} task has completed.")

with DAG(
    'DWH_ETL_Machine_Delivery_dag',
    schedule_interval=None,
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    # 1. Generate Machine Delivery from a DATA LAKE To DATA Warehouse
    task_ET_WH_MachineDelivery = PythonOperator(
        task_id='extract_transform_machine_delivery_from_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "machine_delivery", 'Chunk_Size': 20000, 'Condition': ""} #" where DATE(UR_LASTTIME) >= (current_date - 31) or DATE(CUS_LASTTIME) >= (current_date - 31)"
    )

    # 2. Upsert Machine Delivery To DATA Warehouse
    task_L_WH_MachineDelivery = PythonOperator(
        task_id='upsert_machine_delivery_on_data_warehouse',
        provide_context=True,
        python_callable= UPSERT_process,
        op_kwargs={'To_Table': "machine_delivery"}
    )

    # 3. Replace Machine Delivery Temp Table
    task_RP_WH_MachineDelivery = PythonOperator(
        task_id='create_new_machine_delivery_table',
        provide_context=True,
        python_callable= INSERT_bluk,
        op_kwargs={'To_Table': "machine_delivery"}
    )

    # 4. Cleansing Machine Delivery Table
    task_CL_WH_MachineDelivery = PythonOperator(
        task_id='cleansing_machine_delivery_data',
        provide_context=True,
        python_callable= Cleansing_process,
        op_kwargs={'To_Table': "machine_delivery", 'Key': "item_id", 'Condition': ""} #" and (DATE(UR_LASTTIME) >= (current_date - 31) or DATE(CUS_LASTTIME) >= (current_date - 31))"
    )

    branch_op = BranchPythonOperator(
        task_id="check_existing_machine_delivery_on_data_warehouse",
        python_callable=branch_func,
    )

    branch_join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_task >> task_ET_WH_MachineDelivery >> end_task