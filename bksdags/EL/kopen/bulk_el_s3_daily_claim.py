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
        "warranty_claim": 1,
        "warranty_claim_detail2": 31,
        "claim_result": 61,
    }
    return switcher.get(index, 0)

default_args = {'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_Get_Daily_Claim_db2_S3_dag',
    tags=['Auto_Daily'],
    schedule_interval='21 21 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    def xcom_push(val):
        return val

    def choose(val):
        return f"task_{val}"

    def check_xcom_output_from_first(val, expected_val):
        assert val == expected_val


    tables = ["warranty_claim", "warranty_claim_detail2", "claim_result"]

    for i in tables:
        
        first = PythonOperator(task_id=f"first_task_{i}", python_callable=xcom_push, op_kwargs={"val": i})
        check_is_api_active = HttpSensor(
            task_id=f'is_api_active_{i}',
            http_conn_id='data_api',
            endpoint='etl/db2/',
            execution_timeout=timedelta(seconds=60),
            timeout=200,
            retries=3,
            mode="reschedule",
        )
        call_api_auto_get_db2data = SimpleHttpOperator(
            task_id=f'auto_get_{i}',
            http_conn_id='data_api',
            method='POST',
            endpoint='etl/db2/loaddb2s3',
            data=json.dumps({
                    "etl_id": f"etl_{i}",
                    "to_table": f"kp_{i}",
                    "from_table": f"{i}",
                    "chunk_size": 50000,
                    "condition": "",
                    "primary_key": ""
                }),
            headers={"accept": "application/json"},
        )
        time_wait = BashOperator(task_id=f"wait_for_export_{i}", bash_command=f"sleep {set_delay(i)};")
        check_process = PythonOperator(
            task_id=f"check_{i}",
            trigger_rule=TriggerRule.ALL_DONE,
            python_callable=check_xcom_output_from_first,
            op_kwargs={"val": first.output, "expected_val": i},
        )

        time_wait >> first >> check_is_api_active >> call_api_auto_get_db2data >> check_process


    # 2.1 Wait_file_export
    #twait = TimeDeltaSensor(task_id="wait_file_export_arealdy", delta=timedelta(seconds=3000))
    #sensor = ExternalTaskSensor(task_id='dag_sensor', external_dag_id = 'another_dag_id', external_task_id = None, dag=dag, mode = 'reschedule')



