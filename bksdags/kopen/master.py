import time
import ibm_db as db
import pandas as pd
import ibm_db_dbi as dbi
import configparser
from sqlalchemy import create_engine
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

def getandload_data(sql_str,tb_name): 
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
    sql = sql_str
    conn = db.connect(con,'','')
    pd_conn = dbi.Connection(conn)
    # Extract to Data Frame
    df = pd.read_sql(sql, pd_conn)
    # Close Connection
    db.close(conn)
    
    df = df.drop('UR_ENAME', axis=1)

    pgdatabase = config['PG']['database']
    pghost = config['PG']['host']
    pgport = config['PG']['port']
    pguid = config['PG']['uid']
    pgpwd = config['PG']['pwd']
    # Create SQLAlchemy engine
    conn_str = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
    engine = create_engine(conn_str,client_encoding="utf8")
    # Load to DB-LAKE not transfrom
    df.to_sql(tb_name, engine, index=False, if_exists='replace')
    return True


with DAG(
    dag_id='Machine_db2postgres_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=6, day=1),
    catchup=False
) as dag:

    # 1. Get the Machine data from a table in Kopen DB2
    task_ETL_Kopen_Machine_data = PythonOperator(
        task_id='etl_kopen_machine_data',
        provide_context=True,
        python_callable=getandload_data("select * from unit_retain","kp_machine")
    )
    
    task_ETL_Kopen_Machine_data
