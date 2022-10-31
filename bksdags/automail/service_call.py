from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from function import get_yesterday

default_args = {'start_date': pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_Mail_Daily_Service_call_dag',
    tags=['Auto_Send_Mail'],
    schedule_interval='1 7 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    default_args=default_args,
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

    # 2. Auto send mail
    task_Auto_Mail_To_Call_Center = SimpleHttpOperator(
        task_id='auto_mail_service_call',
        http_conn_id='bks_api',
        method='GET',
        endpoint='genreport/sendmail_servicecall',
        data={"lastdate": get_yesterday()},
        headers={"accept": "application/json"},
    )

    task_is_api_active >> task_Auto_Mail_To_Call_Center