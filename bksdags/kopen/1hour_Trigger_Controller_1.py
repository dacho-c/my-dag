import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
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
    '0700-1hour-trigger-dagrun-dag',
    #start_date=datetime(2021, 1, 1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    max_active_runs=1,
    default_view='graph',
    schedule_interval='0 7-19/1 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    t_start = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    t1 = TriggerDagRunOperator(
        task_id="trigger_Part_Quote_dag",
        trigger_dag_id="Kopen_PartAndService_Quote_db2postgres_dag",
        wait_for_completion=True
    )
    t1.set_upstream(t_start)

    t2 = TriggerDagRunOperator(
        task_id="trigger_Part_Internal_Supply_dag",
        trigger_dag_id="Kopen_Part_Internal_Supply_db2postgres_dag",
        wait_for_completion=True
    )
    t2.set_upstream(t1)

    t3 = TriggerDagRunOperator(
        task_id="trigger_Part_Picking_dag",
        trigger_dag_id="Kopen_Part_Picking_db2postgres_dag",
        wait_for_completion=True
    )
    t3.set_upstream(t2)

    t4 = TriggerDagRunOperator(
        task_id="trigger_Invoice_dag",
        trigger_dag_id="Kopen_Invoice_db2postgres_dag",
        wait_for_completion=True
    )
    t4.set_upstream(t3)

    t5 = TriggerDagRunOperator(
        task_id="trigger_Service_job_S3_dag",
        trigger_dag_id="Kopen_Service_job_db2pgS3_dag",
        wait_for_completion=True
    )
    t5.set_upstream(t4)

    t6 = TriggerDagRunOperator(
        task_id="trigger_Stock_S3_dag",
        trigger_dag_id="Kopen_Stock_1Hour_db2S3minio_dag",
        wait_for_completion=True
    )
    t6.set_upstream(t5)

    t7 = TriggerDagRunOperator(
        task_id="trigger_Part_Sale_S3_dag",
        trigger_dag_id="Kopen_Part_sale_1Hour_db2pgS3_dag",
        wait_for_completion=True
    )
    t7.set_upstream(t6)

    t_end = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )
    t_end.set_upstream(t7)

    AllTaskSuccess = PythonOperator(
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'success', 'msubject': 'ETL AllTaskSuccess 07.00 (Hourly)', 'text': 'AllTaskSuccess 07.00 (Hourly Kopen) Quotation, Internal Supply, Part Picking, Invoice, Service Job, Stock'}
    )
    AllTaskSuccess.set_upstream([t_start,t1,t2,t3,t4,t5,t6,t7,t_end])
    
    t1Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t1Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Quotation Task Error 07.00 (Hourly)', 'text': 'Quotation Task Error 07.00 (Hourly)'}
    )
    t1Failed.set_upstream([t1])

    t2Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t2Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Part Internal Supply Task Error 07.00 (Hourly)', 'text': 'Part Internal Supply Task Error 07.00 (Hourly)'}
    )
    t2Failed.set_upstream([t2])

    t3Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t3Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Part Picking Task Error 07.00 (Hourly)', 'text': 'Part Picking Task Error 07.00 (Hourly)'}
    )
    t3Failed.set_upstream([t3])

    t4Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t4Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Invoice Task Error 07.00 (Hourly)', 'text': 'Invoice Task Error 07.00 (Hourly)'}
    )
    t4Failed.set_upstream([t4])

    t5Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t5Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Serivce Job Task Error 07.00 (Hourly)', 'text': 'Service Job Task Error 07.00 (Hourly)'}
    )
    t5Failed.set_upstream([t5])

    t6Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t6Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Stock Task Error 07.00 (Hourly)', 'text': 'Stock Task Error 07.00 (Hourly)'}
    )
    t6Failed.set_upstream([t6])

    t7Failed = PythonOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="t7Failed",
        python_callable=common.send_mail,
        op_kwargs={'mtype': 'err', 'msubject': 'ETL Part Sale Task Error 07.00 (Hourly)', 'text': 'Part Sale Task Error 07.00 (Hourly)'}
    )
    t7Failed.set_upstream([t7])
