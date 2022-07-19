from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from function import get_yesterday
with DAG(
    dag_id='Komtrax_machine_dag',
    tags=['Komtrax'],
    schedule_interval='0 7 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='bks_api',
        endpoint='genreport/'
    )

    # 2. Auto send mail
    task_Auto_Mail_To_Call_Center = SimpleHttpOperator(
        task_id='auto_mail_service_call',
        http_conn_id='bks_api',
        method='GET',
        endpoint='genreport/sendmail_servicecall/',
        data={"lastdate": get_yesterday()},
        headers={"accept": "application/json"},
    )

    task_is_api_active >> task_Auto_Mail_To_Call_Center