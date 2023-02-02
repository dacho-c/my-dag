#from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
#from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator
#from airflow.utils.trigger_rule import TriggerRule

import pandas as pd
import sys, os, glob
import gc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sqlalchemy

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_last_m_datetime

def Del_File(tables):
    if os.path.exists(tables + ".parquet"):
        os.remove(tables + '.parquet')
    if os.path.exists(tables):
        filelist = glob.glob(os.path.join(tables, "*"))
        for f in filelist:
            os.remove(f)

def combine_parquet_files(input_folder, target_path):
    try:
        files = []
        for file_name in os.listdir(input_folder):
            files.append(pq.read_table(os.path.join(input_folder, file_name)))
        with pq.ParquetWriter(target_path,
                files[0].schema,
                version='2.6',
                compression='gzip',
                use_dictionary=True,
                data_page_size=2097152, #2MB
                write_statistics=True) as writer:
            for f in files:
                writer.write_table(f)
    except Exception as e:
        print(e)

def EL_process(**kwargs):

    db2strcon = common.get_db2_connection('')
    # Create SQLAlchemy engine
    engine_db2 = sqlalchemy.create_engine(db2strcon)
    conn_db2 = engine_db2.connect().execution_options(stream_results=True)

    rows = 0
    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']
    C_condition = kwargs['Condition']
    
    Del_File(tb_to)

    sqlstr = "SELECT * FROM db2admin." + tb_from + C_condition

    for chunk_df in pd.read_sql(sqlstr, conn_db2 ,chunksize=c_size):
        rows += len(chunk_df)
        # Load to DB-LAKE not transfrom
        table = pa.Table.from_pandas(chunk_df)
        pq.write_to_dataset(table ,root_path=tb_to)
        pq.ParquetDataset(tb_to + '/', use_legacy_dataset=False).files
        print(f"Save to S3 data lake {rows} rows")
        del chunk_df
        gc.collect()
    print("ETL Process finished")
    conn_db2.close()

    combine_parquet_files(tb_to, tb_to + '.parquet')

    return True

with DAG(
    'Kopen_Part_Daily_db2S3minio_dag',
    owner='airflow',
    schedule_interval=None,
    #dagrun_timeout=timedelta(minutes=120),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    
    ################### PART #############################################################################################################
    t1 = PythonOperator(
        task_id='el_kopen_part_data',
        provide_context=True,
        python_callable=EL_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': ""}
    )

    t2 = PythonOperator(
        task_id='copy_part_to_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_to_minio,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': ""}
    )
    t2.set_upstream(t1)
    
