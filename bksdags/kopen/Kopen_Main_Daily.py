from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

import time
import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common

args = {
        'owner': 'airflow',    
        #'start_date': airflow.utils.dates.days_ago(2),
        # 'end_date': datetime(),
        'depends_on_past': False,
        #'email': ['airflow@example.com'],
        #'email_on_failure': False,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id='0505_Kopen_Main_Daily_db2postgres_dag',
    default_args=args,
    schedule_interval=None,
    #schedule_interval='5 5 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    dagrun_timeout=timedelta(minutes=20),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    ################### DUMMY TABLE ##########################################################################################################################
    # 1. Get the revenue type data from a table in Kopen DB2
    task_EL_Kopen_Revenue_Type_data = PythonOperator(
        task_id='el_kopen_revenue_type_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "BKS_REVENUE_TYPE", 'To_Table': "kp_revenue_type", 'Chunk_Size': 50000}
    )

    # 2. Get the part_monitor data from a table in Kopen DB2
    task_EL_Kopen_Part_Monitor_data = PythonOperator(
        task_id='el_kopen_part_monitor_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "BKS_PART_MONITOR", 'To_Table': "kp_bks_part_monitor", 'Chunk_Size': 50000}
    )

    # 3. Get the sok part data from a table in Kopen DB2
    task_EL_Kopen_SOK_Part_data = PythonOperator(
        task_id='el_kopen_sok_part_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "BKS_SOK_PART", 'To_Table': "kp_bks_sok_part", 'Chunk_Size': 50000}
    )

    # 4. Get the all main filter data from a table in Kopen DB2
    task_EL_Kopen_All_Main_Filter_data = PythonOperator(
        task_id='el_kopen_all_main_filter_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "bks_all_main_filter", 'To_Table': "kp_bks_all_main_filter", 'Chunk_Size': 50000}
    )

    # 4. Get the part demand data from a table in Kopen DB2
    task_EL_Kopen_Part_Demand_data = PythonOperator(
        task_id='el_kopen_part_demand_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "bks_part_demand", 'To_Table': "kp_bks_part_demand", 'Chunk_Size': 50000}
    )
    ################### SMALL MASTER ##########################################################################################################################
    # 1. Get the Machine Model data from a table in Kopen DB2
    task_EL_Kopen_Machine_Model_data = PythonOperator(
        task_id='el_kopen_machine_model_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "unit_basic", 'To_Table': "kp_machine_model", 'Chunk_Size': 50000}
    )
    
    # 2. Get the Customer data from a table in Kopen DB2
    task_EL_Kopen_Branch_data = PythonOperator(
        task_id='el_kopen_branch_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "branch", 'To_Table': "kp_branch", 'Chunk_Size': 50000}
    )

    # 3. Get the Customer Address data from a table in Kopen DB2
    task_EL_Kopen_CustAddress_data = PythonOperator(
        task_id='el_kopen_cust_address_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "customer_address", 'To_Table': "kp_customer_address", 'Chunk_Size': 50000}
    )

    # 4. Get the Part Class data from a table in Kopen DB2
    task_EL_Kopen_Part_Class_data = PythonOperator(
        task_id='el_kopen_part_class_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "product_class", 'To_Table': "kp_part_class", 'Chunk_Size': 50000}
    )

    # 5. Get the Department data from a table in Kopen DB2
    task_EL_Kopen_Department_data = PythonOperator(
        task_id='el_kopen_department_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "department", 'To_Table': "kp_department", 'Chunk_Size': 50000}
    )

    # 6. Get the Employee data from a table in Kopen DB2
    task_EL_Kopen_Employee_data = PythonOperator(
        task_id='el_kopen_employee_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "employee", 'To_Table': "kp_employee", 'Chunk_Size': 50000}
    )

    # 7. Get the ID data from a table in Kopen DB2
    task_EL_Kopen_IDbooks_data = PythonOperator(
        task_id='el_kopen_idbooks_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "idbooks", 'To_Table': "kp_idbooks", 'Chunk_Size': 50000}
    )

    # 8. Get the service code data from a table in Kopen DB2
    task_EL_Kopen_Service_code_data = PythonOperator(
        task_id='el_kopen_service_code_data',
        provide_context=True,
        python_callable=common.read_load_save_data,
        op_kwargs={'From_Table': "SERV_BUSINESS_CODE", 'To_Table': "kp_service_code", 'Chunk_Size': 50000}
    )

    #t1 = PythonOperator(task_id="delay_python_task",
        #python_callable=lambda: time.sleep(60)
    #)

    task_EL_Kopen_Revenue_Type_data >> task_EL_Kopen_Part_Monitor_data >> task_EL_Kopen_SOK_Part_data \
    >> task_EL_Kopen_All_Main_Filter_data >> task_EL_Kopen_Part_Demand_data \
    \
    >> task_EL_Kopen_Machine_Model_data >> task_EL_Kopen_Branch_data >> task_EL_Kopen_CustAddress_data >> task_EL_Kopen_Part_Class_data \
    >> task_EL_Kopen_Department_data >> task_EL_Kopen_Employee_data >> task_EL_Kopen_IDbooks_data >> task_EL_Kopen_Service_code_data
