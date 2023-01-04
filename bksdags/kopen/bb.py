from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'dependent-dag',
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='starting_task1',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    end_task = PythonOperator(
        task_id='end_task1',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    start_task >> end_task