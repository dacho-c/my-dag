
from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from Class import read_load_save_data

with DAG(
    dag_id='Kopen_Master_db2postgres_dag',
    schedule_interval='0 6-18/2 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Machine data from a table in Kopen DB2
    task_EL_Kopen_Machine_data = PythonOperator(
        task_id='el_kopen_machine_data',
        provide_context=True,
        python_callable=read_load_save_data,
        op_kwargs={'From_Table': "unit_retain", 'To_Table': "kp_machine", 'Chunk_Size': 50000}
    )

    # 3. Get the Customer data from a table in Kopen DB2
    task_EL_Kopen_Customer_data = PythonOperator(
        task_id='el_kopen_customer_data',
        provide_context=True,
        python_callable=read_load_save_data,
        op_kwargs={'From_Table': "customer", 'To_Table': "kp_customer", 'Chunk_Size': 50000}
    )

    task_EL_Kopen_Machine_data >> task_EL_Kopen_Customer_data
