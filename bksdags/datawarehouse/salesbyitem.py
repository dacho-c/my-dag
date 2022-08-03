from datetime import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_last_ym
from pgsql_salesbyitem import sql_ET_salesbyitem, sql_DEL_salesbyitem

def ETL_process(**kwargs):
    import pandas as pd
    import sqlalchemy

    whstrcon = common.get_wh_connection('')
    dlstrcon = common.get_dl_connection('')
    # Create SQLAlchemy engine
    engine_dl = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    engine_wh = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")
    conn_dl = engine_dl.connect().execution_options(stream_results=True)

    sqlstr = sql_ET_salesbyitem(get_last_ym())

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']

    for chunk_df in pd.read_sql_query(sql=sqlalchemy.text(sqlstr), con=engine_dl, chunksize=c_size):
        rows += len(chunk_df)
        print(f"Got dataframe w/{rows} rows")
        # Load to DB-LAKE not transfrom
        if n == 0:
            ######
            with engine_wh.connect().execution_options(autocommit=True) as conn:
                conn.execute(sqlalchemy.text(sql_DEL_salesbyitem(get_last_ym())))
            chunk_df.to_sql(tb_to, engine_wh, index=False, if_exists='append')
            n = n + 1
        else:
            chunk_df.to_sql(tb_to, engine_wh, index=False, if_exists='append')
        print(f"Save to data W/H {rows} rows")
    print("ETL Process finished")

with DAG(
    dag_id='DWH_ETL_SalesByItem_dag',
    schedule_interval='25 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Sales By Item from a DATA LAKE To DATA Warehouse
    task_ETL_WH_SalesByItem = PythonOperator(
        task_id='etl_salesbyitem_data',
        provide_context=True,
        python_callable=ETL_process,
        op_kwargs={'To_Table': "sales_by_item", 'Chunk_Size': 50000}
    )

    task_ETL_WH_SalesByItem