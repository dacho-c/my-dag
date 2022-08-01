from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
#from function import get_last_ym

with DAG(
    dag_id='DWH_ETL_Machine_Delivery_dag',
    schedule_interval='30 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Machine Delivery from a DATA LAKE To DATA Warehouse
    task_ETL_WH_MachineDelivery = PythonOperator(
        task_id='etl_machinedelivery_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "machine_delivery", 'To_Table': "machine_delivery", 'Chunk_Size': 10000, 'Condition': ""}
    )

    task_ETL_WH_MachineDelivery