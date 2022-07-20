from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common

with DAG(
    dag_id='Kopen_Stock_db2postgres_dag',
    schedule_interval='30 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Part Stock from a table in Kopen DB2
    task_EL_Kopen_Part_Stock_data = PythonOperator(
        task_id='el_kopen_part_stock_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "stock", 'To_Table': "kp_stock", 'Chunk_Size': 100000}
    )

    task_EL_Kopen_Part_Stock_data