from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_last_ym

def ETL_process(**kwargs):
    return True
with DAG(
    dag_id='DWH_ETL_Machine_db2postgres_dag',
    schedule_interval='25 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Sales By Item from a DATA LAKE To DATA Warehouse
    task_ETL_WH_Machine = PythonOperator(
        task_id='etl_machine_data',
        provide_context=True,
        python_callable=ETL_process,
        op_kwargs={'From_Table': "machine", 'To_Table': "machine", 'Chunk_Size': 50000, 'Condition': "smm_account_month >= '%s'" % (get_last_ym())}
    )

    task_ETL_WH_Machine