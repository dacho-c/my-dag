from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from pgsql_machinedelivery import sql_ET_machine_cust, sql_ET_machine, sql_ET_invoice

def ETL_process(**kwargs):
    import pandas as pd
    import sqlalchemy

    whstrcon = common.get_wh_connection('')
    dlstrcon = common.get_dl_connection('')
    # Create SQLAlchemy engine
    engine_dl = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    engine_wh = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")
    conn_dl = engine_dl.connect().execution_options(stream_results=True)

    sqlstr_main = sql_ET_machine_cust()
    sqlstr_mc = sql_ET_machine()
    sqlstr_inv = sql_ET_invoice()

    df_mc = pd.read_sql_query(sqlstr_mc, con=engine_dl)
    df_inv = pd.read_sql_query(sqlstr_inv, con=engine_dl)

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']

    for df_main in pd.read_sql_query(sql=sqlalchemy.text(sqlstr_main), con=engine_dl, chunksize=c_size):
        rows += len(df_main)
        print(f"Got dataframe w/{rows} rows")
        # Load & transfrom
        df_miam = df_main.reset_index()
        for index, row in df_main.iterrows():
            rslt_df = df_mc.loc[(df_mc['ur_cus'] == row['cus_id']) & (df_mc['mcfy'] < row['mc_fy'])]
            df_main.loc[index,'mc_bfp'] = len(rslt_df)
            if len(rslt_df) == 0:
                rslt_df1 = df_inv.loc[(df_inv['pih_cus_id'] == row['cus_id']) & (df_inv['invfy'] < row['mc_fy'])]
                df_main.loc[index,'inv_bfp'] = len(rslt_df1)
                if len(rslt_df1) > 0:
                    df_main.loc[index,'cus_group'] = 'Old'
                else:
                    df_main.loc[index,'cus_group'] = 'New'
            else:
                df_main.loc[index,'cus_group'] = 'Old'
        ##################
        if n == 0:
            df_main.to_sql(tb_to, engine_wh, index=False, if_exists='replace')
            n = n + 1
        else:
            df_main.to_sql(tb_to, engine_wh, index=False, if_exists='append')
            n = n + 1
        print(f"Save to data W/H {rows} rows")
    print("ETL Process finished")

with DAG(
    dag_id='DWH_ETL_Machine_Delivery_dag',
    schedule_interval='30 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Machine Delivery from a DATA LAKE To DATA Warehouse
    task_ETL_WH_MachineDelivery = PythonOperator(
        task_id='etl_machinedelivery_data',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "machine_delivery", 'Chunk_Size': 2000}
    )

    task_ETL_WH_MachineDelivery