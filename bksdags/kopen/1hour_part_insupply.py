from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from Class import common
from function import get_last_ym

with DAG(
    'Kopen_Part_Internal_Supply_db2postgres_dag',
    schedule_interval=None,
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the part Supply Head from a table in Kopen DB2
    task_ETL_Kopen_Part_Internal_Supply_Head_data = PythonOperator(
        task_id='etl_kopen_part_supply_head_data',
        provide_context=True,
        python_callable=common.read_load_update_data,
        op_kwargs={'From_Table': "SERV_PART_OUTSTOCK", 'To_Table': "kp_service_part_supply_head", 'Chunk_Size': 50000, 'Condition': "spo_account_month >= '%s'" % (get_last_ym())}
    )

    # 2. Get the part supply Detail data from a table in Kopen DB2
    task_ETL_Kopen_Part_Internal_Supply_Detail_data = PythonOperator(
        task_id='etl_kopen_part_supply_detail_data',
        provide_context=True,
        python_callable=common.read_load_update_detail_data,
        op_kwargs={'From_Table': "SERV_PART_OUT_DTL", 'To_Table': "kp_service_part_supply_detail", 'Chunk_Size': 50000}
    )

    task_ETL_Kopen_Part_Internal_Supply_Head_data >> task_ETL_Kopen_Part_Internal_Supply_Detail_data
