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

self = []
with DAG(
    dag_id='Kopen_Invoice_db2postgres_dag',
    schedule_interval='10 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Invoice Head from a table in Kopen DB2
    task_ETL_Kopen_Inv_Head_data = PythonOperator(
        task_id='etl_kopen_invoice_head_data',
        provide_context=True,
        python_callable=common.read_load_update_data(self,"part_inv_head", "kp_invoice_head", 50000, "pih_account_month >= '%s'" % (get_last_ym)),
        #op_kwargs={'From_Table': "part_inv_head", 'To_Table': "kp_invoice_head", 'Chunk_Size': 50000, 'Condition': "pih_account_month >= '%s'" % (get_last_ym)}
    )

    # 2. Get the Invoice Detail data from a table in Kopen DB2
    task_ETL_Kopen_Inv_Detail_data = PythonOperator(
        task_id='etl_kopen_invoice_detail_data',
        provide_context=True,
        python_callable=common.read_load_update_detail_data(self,"part_inv_detail", "kp_invoice_detail", 50000),
        #op_kwargs={'From_Table': "part_inv_detail", 'To_Table': "kp_invoice_detail", 'Chunk_Size': 50000}
    )

    task_ETL_Kopen_Inv_Head_data >> task_ETL_Kopen_Inv_Detail_data
