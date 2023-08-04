from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sensors.time_delta import TimeDeltaSensor

import sys, os
sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from function import get_lastday

default_args = {'start_date': pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_Create_Monthly_Salesbyitem_dag',
    tags=['Auto_Report'],
    schedule_interval='19 6 1 * *',
    #start_date=datetime(year=2022, month=6, day=1),
    default_args=default_args,
    catchup=False
) as dag:

    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='data_api',
        endpoint='genreport/',
        execution_timeout=timedelta(seconds=60),
        timeout=200,
        retries=3,
        mode="reschedule",
    )

    # 2. Auto create report and upload to sharepoint
    task_api_auto_create_report = SimpleHttpOperator(
        task_id='auto_create_monthly_report_to_sharepoint',
        http_conn_id='data_api',
        method='GET',
        endpoint='genreport/salebyitem',
        data={"lastdate": get_lastday(1), "case": 1},
        headers={"accept": "application/json"},
    )

    # 2.1 Wait_file_export
    #twait = TimeDeltaSensor(task_id="wait_file_export_arealdy", delta=timedelta(seconds=3000))


    task_is_api_active >> task_api_auto_create_report