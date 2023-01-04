from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

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
    '0500-daily-trigger-dagrun-dag',
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval='0 5 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    trigger_master_dag = TriggerDagRunOperator(
        task_id="trigger_master_dag",
        trigger_dag_id="Kopen_Main_Daily_db2postgres_dag",
        wait_for_completion=True
    )

    trigger_part_dag = TriggerDagRunOperator(
        task_id="trigger_part_dag",
        trigger_dag_id="Kopen_Part_Daily_db2postgres_dag",
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    start_task >> trigger_master_dag >> trigger_part_dag >> end_task