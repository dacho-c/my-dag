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
        "loadsaps3_notifications": 1,
        "loadsaps3_Service_order": 8,
        "loadsaps3_Sales_order": 68,
        "loadsaps3_Billing": 148
        # + 120
    }
    return switcher.get(index, 0)

default_args = {'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_EL_Daily_Load_SAP_Oitems_DataLake_dag',
    tags=['Auto_Daily'],
    schedule_interval='38 0-16/1 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    def xcom_push(val):
        return val

    def check_xcom_output_from_first(val, expected_val):
        assert val == expected_val


    tasks = ["loadsaps3_notifications","loadsaps3_Service_order","loadsaps3_Sales_order","loadsaps3_Billing"]

    first = PythonOperator(task_id="first_task_sap_el", python_callable=xcom_push, op_kwargs={"val": 'pass'})
    check_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='data_pipeline',
        endpoint='etl/sap/',
        execution_timeout=timedelta(seconds=60),
        timeout=200,
        retries=3,
        mode="reschedule",
    )
    check_process = PythonOperator(
        task_id="check_all_process",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        python_callable=check_xcom_output_from_first,
        op_kwargs={"val": first.output, "expected_val": 'pass'},
    )

    first >> check_is_api_active

    for i in tasks:

        time_wait = BashOperator(task_id=f"wait_for_execute_{i}", bash_command=f"sleep {set_delay(i)};")   

        op = SimpleHttpOperator(
            task_id=f'auto_EL_{i}',
            http_conn_id='data_pipeline',
            method='POST',
            endpoint=f'etl/sap/{i}',
            data=json.dumps({
                    "etl_id": f"el_{i}",
                    "to_table": "",
                    "from_table": "",
                    "chunk_size": 50000,
                    "condition": "",
                    "primary_key": "",
                    "last_days": 1,
                    "fiscal_year": "",
                    "Client": "900"
                }),
            headers={"accept": "application/json"},
        )

        check_is_api_active >> time_wait >> op >> check_process


    # 2.1 Wait_file_export
    #twait = TimeDeltaSensor(task_id="wait_file_export_arealdy", delta=timedelta(seconds=3000))
    #sensor = ExternalTaskSensor(task_id='dag_sensor', external_dag_id = 'another_dag_id', external_task_id = None, dag=dag, mode = 'reschedule')



