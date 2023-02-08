from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
#from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator
from airflow import AirflowException
#from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sys, os
import gc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sqlalchemy

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
#from function import get_last_m_datetime

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
    
    common.Del_File(**kwargs)

    sqlstr = "SELECT * FROM db2admin." + tb_from + C_condition

    for chunk_df in pd.read_sql(sqlstr, conn_db2 ,chunksize=c_size):
        rows += len(chunk_df)
        # Load to DB-LAKE not transfrom
        table = pa.Table.from_pandas(chunk_df)
        pq.write_to_dataset(table ,root_path=tb_to)
        pq.ParquetDataset(tb_to + '/', use_legacy_dataset=False).files
        print(f"Save to Airflow storage {rows} rows")
        del chunk_df
    print("ETL Process finished")
    conn_db2.close()

    common.combine_parquet_files(**kwargs)
    gc.collect()
    return True

def PP_process(**kwargs):

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']

    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    ########################################################################
    result_state = True
    c_rows = 0
    strexec = ''
    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        ctable = "SELECT count(*) as c FROM %s;" % (tb_to)
        result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
        c_rows = result.loc[0,'c']
        if os.path.exists(tb_to + ".parquet"):
            table = pq.read_table(tb_to + ".parquet", columns=[])
            print(table.num_rows)
            if table.num_rows <= (c_rows * 0.8):
                os.remove(tb_to + '.parquet')
                result_state = False
                del table
                raise ValueError('New DATA ROWS are less then 80% of exiting tables') 
    else:
        strexec = """CREATE TABLE IF NOT EXISTS public.kp_part
            (
                pro_komcode text COLLATE pg_catalog."default",
                pro_komcode_o text COLLATE pg_catalog."default",
                pro_classid text COLLATE pg_catalog."default",
                pro_name text COLLATE pg_catalog."default",
                pro_last_saledate date,
                pro_lastuserid text COLLATE pg_catalog."default",
                pro_lasttime timestamp without time zone,
                pro_status text COLLATE pg_catalog."default",
                pro_model_code text COLLATE pg_catalog."default",
                pro_last_purdate date,
                pro_cate text COLLATE pg_catalog."default",
                pro_sales_type text COLLATE pg_catalog."default"
            );"""
        strexec += (" ALTER TABLE %s ADD PRIMARY KEY (%s);" % (tb_to, primary_key))
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            conn.close()
    ###############################################################################
    return result_state

def ETL_process(**kwargs):

    tb_to = kwargs['To_Table']

    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    ########################################################################
    c_columns = 0
    # ETL ################################################################
    col = ['pro_komcode',
        'pro_komcode_o',
        'pro_classid',
        'pro_name',
        'pro_last_saledate',
        'pro_lastuserid',
        'pro_lasttime',
        'pro_status',
        'pro_model_code',
        'pro_last_purdate',
        'pro_cate',
        'pro_sales_type']
    df = pd.read_parquet(tb_to + '.parquet', columns=col)
    #######################################################################
    df.pro_name = df.pro_name.str.replace(",", " ")
    df.pro_komcode_o = df.pro_komcode_o.str.replace(",", " ")
    df.pro_komcode = df.pro_komcode.str.replace(",", "")
    df = df.drop_duplicates(subset=['pro_komcode'])
    #######################################################################
    ctable = "SELECT count(*) as c FROM information_schema.columns WHERE table_name = '%s';" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    c_columns = result.loc[0,'c']
    print(f"DF (rows, col) :  {df.shape}")
    if c_columns == df.shape[1]:
        # execute
        with engine.connect() as conn:
            conn.execute("DELETE FROM %s;" % (tb_to))
            conn.close()
        print(f"Save to Postgres {df.shape}")
        if common.copy_from_dataFile(df,tb_to):
            del df
            print("ETL Process finished")
    else:
        raise ValueError('New DATA Columns are not same of exiting tables') 
    ########################################################################        
    common.Del_File(**kwargs)
    if os.path.exists(tb_to + ".parquet"):
        os.remove(tb_to + '.parquet')
    gc.collect()
    return True

with DAG(
    'Kopen_Part_Daily_db2S3minio_dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    
    ################### PART #############################################################################################################
    #t1 = PythonOperator(
    #    task_id='el_kopen_part_data',
    #    provide_context=True,
    #    python_callable=EL_process,
    #    op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': ""}
    #)

    t2 = PythonOperator(
        task_id='prepare_kopen_part',
        provide_context=True,
        python_callable= PP_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': ""}
    )
    #t2.set_upstream(t1)

    t3 = PythonOperator(
        task_id='copy_part_to_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_to_minio,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': ""}
    )
    t3.set_upstream(t2)

    t4 = PythonOperator(
        task_id='etl_kopen_part_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'From_Table': "PRODUCT", 'To_Table': "kp_part", 'Chunk_Size': 50000, 'Key': 'pro_komcode', 'Condition': ""}
    )
    t4.set_upstream(t3)
    
