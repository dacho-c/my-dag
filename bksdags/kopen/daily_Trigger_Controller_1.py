import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timezone, timedelta
import pendulum
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common

def send_mail():
    FROM = "sysinfo@bangkokkomatsusales.com" 
    TO = ["dacho-c@bangkokkomatsusales.com"] # must be a list

    SUBJECT = "Hello!"
    TEXT = "This is a test of emailing through smtp of example.com."

    # Prepare actual message
    message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\

        %s
        """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

    # Send the mail
    server = smtplib.SMTP(host='smtp.office365.com', port=587)
    server.starttls()
    server.login("sysinfo@bangkokkomatsusales.com", "crm@12345")
    server.sendmail(FROM, TO, message)
    server.quit()
    return True

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
    print(f"The {kwargs['task_type']} task has completed.")

now = datetime.now()
now_fmt = now.strftime('%Y-%m-%d_%H:%M:%S%z')
# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'email': ['dacho-c@bangkokkomatsusales.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    '0530-daily-trigger-dag',
    #start_date= airflow.utils.dates.days_ago(0),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    schedule_interval='30 5 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    t_start = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    t1 = TriggerDagRunOperator(
        task_id="trigger_part_s3_dag",
        trigger_dag_id="Kopen_Part_Daily_db2S3minio_dag",
        wait_for_completion=True
    )
    t1.set_upstream(t_start)

    t2 = TriggerDagRunOperator(
        task_id="trigger_master_dag",
        trigger_dag_id="Kopen_Main_Daily_db2postgres_dag",
        wait_for_completion=True
    )
    t2.set_upstream(t1)

    t4 = TriggerDagRunOperator(
        task_id="trigger_monthly_stock_dag",
        trigger_dag_id="Kopen_Monthly_Stock_Daily_db2postgres_dag",
        wait_for_completion=True
    )
    t4.set_upstream(t2)

    t_end = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )
    t_end.set_upstream(t4)

    AllTaskSuccess = PythonOperator(
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        python_callable=send_mail
    )
    AllTaskSuccess.set_upstream([t_start,t1,t2,t4,t_end])
    
    t1Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t1Failed",
        python_callable=send_mail
    )
    t1Failed.set_upstream([t1])
