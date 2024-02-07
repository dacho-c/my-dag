from datetime import datetime, timedelta
import pendulum
import json
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy_operator import DummyOperator

import sys, os
sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from function import get_today

def set_delay(index):
    switcher = {
        "loadsaps3_original_master": 1,
        "loadsaps3_customer_master": 20,
        "loadsaps3_customer_address": 60,
    }
    return switcher.get(index, 0)

default_args = {'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_EL_Daily_Load_SAP_Master_DataLake_dag',
    tags=['Auto_Daily'],
    schedule_interval='1 1/3 * * 1-5',
    default_args=default_args,
    catchup=False
) as dag:

    def xcom_push(val):
        return val

    def choose(val):
        return f"task_{val}"

    def check_xcom_output_from_first(val, expected_val):
        assert val == expected_val


    tasks = ["loadsaps3_original_master","loadsaps3_customer_master","loadsaps3_customer_address"]

    for i in tasks:
        
        first = PythonOperator(task_id=f"first_task_pg_{i}", python_callable=xcom_push, op_kwargs={"val": i})
        check_is_api_active = HttpSensor(
            task_id=f'is_api_active_{i}',
            http_conn_id='data_pipeline',
            endpoint='etl/sap/',
            execution_timeout=timedelta(seconds=60),
            timeout=200,
            retries=3,
            mode="reschedule",
        )
        call_api_auto_el_sap_data = SimpleHttpOperator(
            task_id=f'auto_EL_{i}',
            http_conn_id='data_pipeline',
            method='POST',
            endpoint=f'etl/sap/{i}',
            data=json.dumps({
                    "etl_id": f"el_{i}",
                    "to_table": "",
                    "from_table": "",
                    "chunk_size": 500000,
                    "condition": "",
                    "primary_key": "",
                    "last_days": 1,
                    "fiscal_year": "",
                    "Client": "900"
                }),
            headers={"accept": "application/json"},
        )
        time_wait = BashOperator(task_id=f"wait_for_execute_{i}", bash_command=f"sleep {set_delay(i)};")
        check_process = PythonOperator(
            task_id=f"check_{i}",
            trigger_rule=TriggerRule.ALL_DONE,
            python_callable=check_xcom_output_from_first,
            op_kwargs={"val": first.output, "expected_val": i},
        )

        time_wait >> first >> check_is_api_active >> call_api_auto_el_sap_data >> check_process


    # 2.1 Wait_file_export
    #twait = TimeDeltaSensor(task_id="wait_file_export_arealdy", delta=timedelta(seconds=3000))
    #sensor = ExternalTaskSensor(task_id='dag_sensor', external_dag_id = 'another_dag_id', external_task_id = None, dag=dag, mode = 'reschedule')



