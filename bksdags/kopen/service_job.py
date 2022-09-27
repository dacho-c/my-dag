from datetime import datetime
from bksdags.kopen.function import get_Last_fisical_year
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_first_ym_fisical_year

with DAG(
    dag_id='Kopen_Service_job_db2postgres_dag',
    schedule_interval='20 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Service Job from a table in Kopen DB2
    task_ETL_Kopen_Service_Job_data = PythonOperator(
        task_id='etl_kopen_service_job_data',
        provide_context=True,
        python_callable=common.read_load_update_data,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Condition': "smm_account_month >= '%s'" % ( get_first_ym_fisical_year() )}
    )

    task_ETL_Kopen_Service_Job_data