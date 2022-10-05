from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
#from function import get_yesterday
with DAG(
    dag_id='Salesforce_Account_ETL_dag',
    tags=['Salesforce'],
    schedule_interval='4 6-18/4 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='bks_api',
        endpoint='genreport/',
        execution_timeout=timedelta(seconds=120),
        timeout=3600,
        retries=3,
        mode="reschedule",
    )

    # 2. Call API Salesforce Load to Datalaken Temp Table
    task_Get_Salesforce_Data_Save_To_Datalake = SimpleHttpOperator(
        task_id='get_salesforce_account_object',
        http_conn_id='bks_api',
        method='GET',
        endpoint='etl/sf/sfaccount',
        timeout=1200,
    )

    task_is_api_active >> task_Get_Salesforce_Data_Save_To_Datalake