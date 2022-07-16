from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname('common')))
#sys.path.append('/opt/airflow/dags/repo/bksdags/common/Class')
from Class import read_load_save_data

with DAG(
    dag_id='Kopen_Part_Daily_db2postgres_dag',
    schedule_interval='0 5 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Get the Machine data from a table in Kopen DB2
    task_EL_Kopen_Part_data = PythonOperator(
        task_id='el_kopen_part_data',
        provide_context=True,
        python_callable=read_load_save_data,
        op_kwargs={'From_Table': "product", 'To_Table': "kp_part", 'Chunk_Size': 50000}
    )

    task_EL_Kopen_Part_data
