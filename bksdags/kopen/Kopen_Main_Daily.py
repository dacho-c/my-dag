from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator, BranchPythonOperator
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
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_last_m_datetime

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
        
def branch_part_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="etl_kopen_part_data", key='return_value'))
    if xcom_value:
        return "upsert_part_on_data_warehouse"
    else:
        return "create_new_part_table"

args = {
        'owner': 'airflow',    
        #'start_date': airflow.utils.dates.days_ago(2),
        # 'end_date': datetime(),
        # 'depends_on_past': False,
        #'email': ['airflow@example.com'],
        #'email_on_failure': False,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id='0505_Kopen_Main_Daily_db2postgres_dag',
    default_args=args,
    schedule_interval='5 5 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    ################### DUMMY TABLE ##########################################################################################################################
    # 1. Get the revenue type data from a table in Kopen DB2
    task_EL_Kopen_Revenue_Type_data = PythonOperator(
        task_id='el_kopen_revenue_type_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "BKS_REVENUE_TYPE", 'To_Table': "kp_revenue_type", 'Chunk_Size': 50000}
    )

    # 2. Get the part_monitor data from a table in Kopen DB2
    task_EL_Kopen_Part_Monitor_data = PythonOperator(
        task_id='el_kopen_part_monitor_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "BKS_PART_MONITOR", 'To_Table': "kp_bks_part_monitor", 'Chunk_Size': 50000}
    )

    # 3. Get the sok part data from a table in Kopen DB2
    task_EL_Kopen_SOK_Part_data = PythonOperator(
        task_id='el_kopen_sok_part_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "BKS_SOK_PART", 'To_Table': "kp_bks_sok_part", 'Chunk_Size': 50000}
    )

    # 4. Get the all main filter data from a table in Kopen DB2
    task_EL_Kopen_All_Main_Filter_data = PythonOperator(
        task_id='el_kopen_all_main_filter_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "bks_all_main_filter", 'To_Table': "kp_bks_all_main_filter", 'Chunk_Size': 50000}
    )

    # 4. Get the part demand data from a table in Kopen DB2
    task_EL_Kopen_Part_Demand_data = PythonOperator(
        task_id='el_kopen_part_demand_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "bks_part_demand", 'To_Table': "kp_bks_part_demand", 'Chunk_Size': 50000}
    )
    ################### SMALL MASTER ##########################################################################################################################
    # 1. Get the Machine Model data from a table in Kopen DB2
    task_EL_Kopen_Machine_Model_data = PythonOperator(
        task_id='el_kopen_machine_model_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "unit_basic", 'To_Table': "kp_machine_model", 'Chunk_Size': 50000}
    )
    
    # 2. Get the Customer data from a table in Kopen DB2
    task_EL_Kopen_Branch_data = PythonOperator(
        task_id='el_kopen_branch_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "branch", 'To_Table': "kp_branch", 'Chunk_Size': 50000}
    )

    # 3. Get the Customer Address data from a table in Kopen DB2
    task_EL_Kopen_CustAddress_data = PythonOperator(
        task_id='el_kopen_cust_address_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "customer_address", 'To_Table': "kp_customer_address", 'Chunk_Size': 50000}
    )

    # 4. Get the Part Class data from a table in Kopen DB2
    task_EL_Kopen_Part_Class_data = PythonOperator(
        task_id='el_kopen_part_class_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "product_class", 'To_Table': "kp_part_class", 'Chunk_Size': 50000}
    )

    # 5. Get the Department data from a table in Kopen DB2
    task_EL_Kopen_Department_data = PythonOperator(
        task_id='el_kopen_department_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "department", 'To_Table': "kp_department", 'Chunk_Size': 50000}
    )

    # 6. Get the Employee data from a table in Kopen DB2
    task_EL_Kopen_Employee_data = PythonOperator(
        task_id='el_kopen_employee_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "employee", 'To_Table': "kp_employee", 'Chunk_Size': 50000}
    )

    # 7. Get the ID data from a table in Kopen DB2
    task_EL_Kopen_IDbooks_data = PythonOperator(
        task_id='el_kopen_idbooks_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "idbooks", 'To_Table': "kp_idbooks", 'Chunk_Size': 50000}
    )

    # 8. Get the service code data from a table in Kopen DB2
    task_EL_Kopen_Service_code_data = PythonOperator(
        task_id='el_kopen_service_code_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "SERV_BUSINESS_CODE", 'To_Table': "kp_service_code", 'Chunk_Size': 50000}
    )
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

    branch_join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    task_EL_Kopen_Revenue_Type_data >> task_EL_Kopen_Part_Monitor_data >> task_EL_Kopen_SOK_Part_data \
    >> task_EL_Kopen_All_Main_Filter_data >> task_EL_Kopen_Part_Demand_data \
    \
    >> task_EL_Kopen_Machine_Model_data >> task_EL_Kopen_Branch_data >> task_EL_Kopen_CustAddress_data >> task_EL_Kopen_Part_Class_data \
    >> task_EL_Kopen_Department_data >> task_EL_Kopen_Employee_data >> task_EL_Kopen_IDbooks_data >> task_EL_Kopen_Service_code_data \
    \
    >> task_ETL_Kopen_Part_data >> task_Part_Branch_op >> [task_L_WH_Part,task_RP_WH_Part] >> branch_join >> task_CL_WH_Part
