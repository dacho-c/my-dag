from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import sys, os
sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from function import get_today

default_args = {'start_date': pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_Mail_Weekly_PriceLists_dag',
    tags=['Auto_Send_Mail'],
    schedule_interval='6 7 * * 1',
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

    # 2. Auto create report and upload to sharepoint
    task_api_auto_create_report = SimpleHttpOperator(
        task_id='auto_create_pricelist',
        http_conn_id='bks_api',
        method='GET',
        endpoint='genreport/pricelists',
        data={"lastdate": get_today()},
        headers={"accept": "application/json"},
    )

    # 3. Auto send mail
    task_Auto_Mail_To_Part = SimpleHttpOperator(
        task_id='auto_mail_pricelist',
        http_conn_id='bks_api',
        method='GET',
        endpoint='genreport/sendmail_pricelists',
        data={"lastdate": get_today(),"mailto":"dacho-c@bangkokkomatsusales.com","mailcc":"","mailbcc":""},
        headers={"accept": "application/json"},
    )

    task_is_api_active >> task_api_auto_create_report >> task_Auto_Mail_To_Part