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
        "etl_customer_master": 5,
        "etl_table_master": 10,
        "etl_employee_master": 15,
        "etl_equipment_master": 20,
        "etl_machinestock_master": 25,
        "etl_cspm_service_order": 30,
        "etl_quotation_header": 35,
        "etl_quotation_details": 40,
        "etl_sales_order_header": 45,
        "etl_sales_order_details": 50,
        "etl_billing_header": 55,
        "etl_billing_details": 60
    }
    return switcher.get(index, 0)

default_args = {'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Bangkok"),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'email': ['dacho-c@bangkokkomatsusales.com'],
                'email_on_failure': True}
with DAG(
    dag_id='Auto_ETL_Daily_DBcenter_dag',
    tags=['Auto_Daily'],
    schedule_interval='45 1-22/1 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    def xcom_push(val):
        return val

    def check_xcom_output_from_first(val, expected_val):
        assert val == expected_val


    tasks = ["etl_customer_master","etl_table_master","etl_employee_master","etl_equipment_master","etl_machinestock_master","etl_cspm_service_order","etl_quotation_header","etl_quotation_details","etl_sales_order_header","etl_sales_order_details","etl_billing_header","etl_billing_details"]

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
            task_id=f'auto_ETL_{i}',
            http_conn_id='data_pipeline',
            method='POST',
            endpoint=f'etl/dbcenter/{i}',
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



