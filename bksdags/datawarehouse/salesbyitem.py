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

with DAG(
    dag_id='DWH_ETL_SalesByItem_db2postgres_dag',
    schedule_interval='25 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Sales By Item from a DATA LAKE To DATA Warehouse
    task_ETL_WH_SalesByItem = PythonOperator(
        task_id='etl_salesbyitem_data',
        provide_context=True,
        python_callable=common.read_load_update_data,
        op_kwargs={'From_Table': "salesbyitem", 'To_Table': "sales_by_item", 'Chunk_Size': 50000, 'Condition': "smm_account_month >= '%s'" % (get_last_ym())}
    )

    task_ETL_WH_SalesByItem