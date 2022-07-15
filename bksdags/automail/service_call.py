from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

from bksdags.kopen.Class import get_yesterday
with DAG(
    dag_id='Auto_Mail_Daily_Service_call_dag',
    tags=['Auto_Send_Mail'],
    schedule_interval='0 7 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Auto send mail
    task_Auto_Mail_To_Call_Center = SimpleHttpOperator(
        task_id='auto_mail_service_call',
        method='GET',
        endpoint='https://api.komatsuthailand.com/genreport/sendmail_servicecall',
        data={"mailto": "dacho-c@bangkokkomatsusales.com", "mailcc": "", "lastdate": get_yesterday},
        headers={"accept": "application/json"},
    )

    task_Auto_Mail_To_Call_Center