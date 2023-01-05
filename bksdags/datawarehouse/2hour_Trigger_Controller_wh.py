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
    '0730-2hour-trigger-dagrun-dag',
    #start_date=datetime(2021, 1, 1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    max_active_runs=1,
    schedule_interval='30 7-21/2 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    trigger_SalesByItem_dag = TriggerDagRunOperator(
        task_id="trigger_SalesByItem_dag",
        trigger_dag_id="DWH_ETL_SalesByItem_dag",
        wait_for_completion=True
    )

    trigger_Machine_Delivery_dag = TriggerDagRunOperator(
        task_id="trigger_Machine_Delivery_dag",
        trigger_dag_id="DWH_ETL_Machine_Delivery_dag",
        wait_for_completion=True
    )

    trigger_customer_rank_dag = TriggerDagRunOperator(
        task_id="trigger_customer_rank_dag",
        trigger_dag_id="DWH_ETL_customer_rank_dag",
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    start_task >> trigger_SalesByItem_dag >> trigger_Machine_Delivery_dag >> trigger_customer_rank_dag >> end_task