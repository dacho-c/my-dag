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
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    '0715-3hour-trigger-dagrun-dag',
    #start_date=datetime(2021, 1, 1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    max_active_runs=1,
    schedule_interval='15 7-21/3 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    trigger_Salesforce_Account_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_Account_dag",
        trigger_dag_id="Salesforce_Account_ETL_dag",
        wait_for_completion=True
    )

    trigger_Salesforce_Asset_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_Asset_dag",
        trigger_dag_id="Salesforce_Asset_ETL_dag",
        wait_for_completion=True
    )

    trigger_Salesforce_Cases_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_Cases_dag",
        trigger_dag_id="Salesforce_Cases_ETL_dag",
        wait_for_completion=True
    )

    trigger_Salesforce_opportunity_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_opportunity_dag",
        trigger_dag_id="Salesforce_opportunity_ETL_dag",
        wait_for_completion=True
    )

    trigger_Salesforce_Order_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_Order_dag",
        trigger_dag_id="Salesforce_Order_ETL_dag",
        wait_for_completion=True
    )

    trigger_Salesforce_Quote_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_Quote_dag",
        trigger_dag_id="Salesforce_Quote_ETL_dag",
        wait_for_completion=True
    )

    trigger_Salesforce_User_dag = TriggerDagRunOperator(
        task_id="trigger_Salesforce_User_dag",
        trigger_dag_id="Salesforce_User_ETL_dag",
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    start_task >> trigger_Salesforce_Account_dag >> trigger_Salesforce_Asset_dag >> trigger_Salesforce_Cases_dag >> trigger_Salesforce_opportunity_dag >> trigger_Salesforce_Order_dag >> trigger_Salesforce_Quote_dag >> trigger_Salesforce_User_dag >> end_task