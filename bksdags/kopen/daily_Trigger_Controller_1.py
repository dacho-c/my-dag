import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timezone, timedelta
import pendulum
import sys, os
sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from Class import common

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    '0530-daily-trigger-dag',
    #start_date= airflow.utils.dates.days_ago(0),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    default_view='graph',
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

    t3 = TriggerDagRunOperator(
        task_id="trigger_monthly_stock_dag1",
        trigger_dag_id="Kopen_Monthly_Stock_Daily_db2postgres_dag",
        wait_for_completion=True
    )

    t4 = TriggerDagRunOperator(
        task_id="trigger_monthly_stock_dag",
        trigger_dag_id="Kopen_Monthly_Stock_Daily_db2postgres_dag",
        wait_for_completion=True
    )

    t_end = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

####################################################################################################################################################
    AllTaskSuccess = PythonOperator(
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'success', 'msubject': 'ETL AllTaskSuccess 05.30 (Daily)', 'text': 'AllTaskSuccess 05.30 (Daily Kopen) Part, Master Table, Monthly Stock'}
    )
    AllTaskSuccess.set_upstream([t1,t2,t3,t4,t_end])
####################################################################################################################################################
    join_t1 = DummyOperator(
        task_id='join_t1',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    join_t1.set_upstream(t1)
    t2.set_upstream(join_t1)

    join_t2 = DummyOperator(
        task_id='join_t2',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    join_t2.set_upstream(t2)
    t3.set_upstream(join_t2)

    join_t3 = DummyOperator(
        task_id='join_t3',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    join_t3.set_upstream(t3)
    t4.set_upstream(join_t3)

    join_t4 = DummyOperator(
        task_id='join_t4',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    join_t4.set_upstream(t4)
    t_end.set_upstream(join_t4)
#####################################################################################################################################################
    t1Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t1Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Part Task Error 05.30 (Daily)', 'text': 'Part Task Error 05.30 (Daily)'}
    )
    t1Failed.set_upstream(t1)
    join_t1.set_upstream(t1Failed)

    t2Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t2Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Master Task Error 05.30 (Daily)', 'text': 'Master Table Task Error 05.30 (Daily)'}
    )
    t2Failed.set_upstream(t2)
    join_t2.set_upstream(t2Failed)

    t3Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t3Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Monthly Stock Task Error 05.30 (Daily)', 'text': 'Monthly Stock Task Error 05.30 (Daily)'}
    )
    t3Failed.set_upstream(t3)
    join_t3.set_upstream(t3Failed)

    t4Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t4Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Monthly Stock Task Error 05.30 (Daily)', 'text': 'Monthly Stock Task Error 05.30 (Daily)'}
    )
    t4Failed.set_upstream(t4)
    join_t4.set_upstream(t4Failed)
