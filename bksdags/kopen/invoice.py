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
        engine = create_engine(conn_str,client_encoding="utf8", max_overflow=-1)
        # Load to DB-LAKE not transfrom
        df.to_sql(kwargs['tb_name'], engine, index=False, if_exists='replace')
        return True
    else: 
        return False


with DAG(
    dag_id='Kopen_Invoice_db2postgres_dag',
    schedule_interval='5 9-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Invoice Head from a table in Kopen DB2
    task_ETL_Kopen_Inv_Head_data = PythonOperator(
        task_id='etl_kopen_invoice_head_data',
        provide_context=True,
        python_callable=getandload_data,
        op_kwargs={'sql_str': "select * from PART_INV_HEAD", 'tb_name': "kp_invoice_head"}
    )

    # 2. Get the Invoice Detail data from a table in Kopen DB2
    task_ETL_Kopen_Inv_Detail_data = PythonOperator(
        task_id='etl_kopen_invoice_detail_data',
        provide_context=True,
        python_callable=getandload_data,
        op_kwargs={'sql_str': "select * from PART_INV_DETAIL", 'tb_name': "kp_invoice_detail"}
    )

    task_ETL_Kopen_Inv_Head_data >> task_ETL_Kopen_Inv_Detail_data
