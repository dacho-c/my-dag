from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pendulum

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
    print(f"The {kwargs['task_type']} task has completed.")

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    '0700-1hour-trigger-dagrun-dag',
    #start_date=datetime(2021, 1, 1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    max_active_runs=1,
    schedule_interval='0 7-20/1 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    trigger_Part_Quote_dag = TriggerDagRunOperator(
        task_id="trigger_Part_Quote_dag",
        trigger_dag_id="Kopen_PartAndService_Quote_db2postgres_dag",
        wait_for_completion=True
    )

    trigger_Part_Internal_Supply_dag = TriggerDagRunOperator(
        task_id="trigger_Part_Internal_Supply_dag",
        trigger_dag_id="Kopen_Part_Internal_Supply_db2postgres_dag",
        wait_for_completion=True
    )

    trigger_Part_Picking_dag = TriggerDagRunOperator(
        task_id="trigger_Part_Picking_dag",
        trigger_dag_id="Kopen_Part_Picking_db2postgres_dag",
        wait_for_completion=True
    )

    trigger_Invoice_dag = TriggerDagRunOperator(
        task_id="trigger_Invoice_dag",
        trigger_dag_id="Kopen_Invoice_db2postgres_dag",
        wait_for_completion=True
    )

    trigger_Service_job_dag = TriggerDagRunOperator(
        task_id="trigger_Service_job_dag",
        trigger_dag_id="Kopen_Service_job_db2postgres_dag",
        wait_for_completion=True
    )

    trigger_Stock_dag = TriggerDagRunOperator(
        task_id="trigger_Stock_dag",
        trigger_dag_id="Kopen_Stock_db2postgres_dag",
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    start_task >> trigger_Part_Quote_dag >> trigger_Part_Internal_Supply_dag >> trigger_Part_Picking_dag >> trigger_Invoice_dag >> trigger_Service_job_dag >> trigger_Stock_dag >> end_task