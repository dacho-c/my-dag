import time
import ibm_db as db
import pandas as pd
import ibm_db_dbi as dbi
import configparser
import pendulum
from sqlalchemy import create_engine
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common


with DAG(
    dag_id='505_Kopen_Dummy_Daily_db2postgres_dag',
    schedule_interval='5 5 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

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

    task_EL_Kopen_Revenue_Type_data >> task_EL_Kopen_Part_Monitor_data >> task_EL_Kopen_SOK_Part_data \
    >> task_EL_Kopen_All_Main_Filter_data >> task_EL_Kopen_Part_Demand_data
