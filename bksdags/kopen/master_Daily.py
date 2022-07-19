import time
import ibm_db as db
import pandas as pd
import ibm_db_dbi as dbi
import configparser
import pendulum
from sqlalchemy import create_engine
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
"""
def getandload_data(**kwargs): 
    config = configparser.ConfigParser()
    config.read(Variable.get('db2pg_config'))
    db2database = config['DB2']['database']
    db2host = config['DB2']['host']
    db2port = config['DB2']['port']
    db2protocal = config['DB2']['protocal']
    db2uid = config['DB2']['uid']
    db2pwd = config['DB2']['pwd']
    # Connection String to EGKopen db2
    con = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=%s;UID=%s;PWD=%s;" % (db2database,db2host,db2port,db2protocal,db2uid,db2pwd)
    start_time = time.time()
    # Extract & Load MACHINE
    sql = kwargs['sql_str']
    conn = db.connect(con,'','')
    pd_conn = dbi.Connection(conn)
    # Extract to Data Frame
    df = pd.read_sql(sql, pd_conn)
    # Close Connection
    db.close(conn)
    
    if not df.empty:
        #df = df.drop('UR_ENAME', axis=1)

        pgdatabase = config['PG']['database']
        pghost = config['PG']['host']
        pgport = config['PG']['port']
        pguid = config['PG']['uid']
        pgpwd = config['PG']['pwd']
        # Create SQLAlchemy engine
        conn_str = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
        engine = create_engine(conn_str,client_encoding="utf8")
        # Load to DB-LAKE not transfrom
        df.to_sql(kwargs['tb_name'], engine, index=False, if_exists='replace')
        return True
    else: 
        return False
"""

with DAG(
    dag_id='Kopen_Master_Daily_db2postgres_dag',
    schedule_interval='5 6 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

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

    task_EL_Kopen_Machine_Model_data >> task_EL_Kopen_Branch_data >> task_EL_Kopen_CustAddress_data >> task_EL_Kopen_Part_Class_data \
    >> task_EL_Kopen_Department_data >> task_EL_Kopen_Employee_data >> task_EL_Kopen_IDbooks_data >> task_EL_Kopen_Service_code_data 
