from datetime import datetime, timedelta
import math
import pendulum
from airflow.models import DAG
#from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator
from airflow import AirflowException
from airflow.utils.trigger_rule import TriggerRule
import sys, os
import gc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sqlalchemy

sys.path.insert(0,os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])
from Class import common
from function import get_fisical_year
from sql import sql_service_job, sql_create_service_job

def EL_process(**kwargs):

    db2strcon = common.get_db2_connection('')
    # Create SQLAlchemy engine
    engine_db2 = sqlalchemy.create_engine(db2strcon)
    conn_db2 = engine_db2.connect().execution_options(stream_results=True)

    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    c_size = kwargs['Chunk_Size']
    C_condition = kwargs['Condition']

    #to_file = tb_to + ".parquet"
    #output = os.getcwd() + "/" + to_file
    
    common.Del_File(**kwargs)

    rows = 0
    
    sqlstr = sql_service_job() + C_condition
    my_schema = ''
    for chunk_df in pd.read_sql(sqlstr, conn_db2 ,chunksize=c_size):
        rows += len(chunk_df)
        # Load to DB-LAKE not transfrom
        common.toparquet(chunk_df,tb_to,my_schema)
        print(f"Save to Airflow storage {rows} rows")
        del chunk_df
    print("ETL Process finished")
    conn_db2.close()

    common.combine_parquet_files(**kwargs)
    gc.collect()
    return True

def PP_process(**kwargs):

    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    C_condition = kwargs['Condition']

    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    ########################################################################
    result_state = True
    c_rows = 0
    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        ctable = "SELECT count(*) as c FROM %s %s;" % (tb_to, C_condition)
        result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
        c_rows = result.loc[0,'c']
        if os.path.exists(tb_to + ".parquet"):
            table = pq.read_table(tb_to + ".parquet", columns=[])
            print(table.num_rows)
            if table.num_rows <= (c_rows * 0.9):
                os.remove(tb_to + '.parquet')
                result_state = False
                del table
                raise ValueError('New DATA ROWS are less then 90% of exiting tables') 
        else:
            result_state = False
            raise ValueError('Not exiting Parquet files') 
    ###############################################################################
    return result_state

def ETL_process(**kwargs):

    tb_from = kwargs['From_Table']
    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    C_condition = kwargs['Condition']

    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    ########################################################################
    c_columns = 0
    # ETL ##################################################################
    df = pd.read_parquet(tb_to + '.parquet')
    strexec = sql_create_service_job(tb_to, primary_key)
    ########################################################################
    #df.pro_name = df.pro_name.str.replace(",", " ")
    #df = df.drop_duplicates(subset=['pro_komcode'])
    ########################################################################
    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        ctable = "SELECT count(*) as c FROM information_schema.columns WHERE table_name = '%s';" % (tb_to)
        result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
        c_columns = result.loc[0,'c']
        ########################################################################
        print(f"DF (rows, col) :  {df.shape}")
        if c_columns == df.shape[1]:
            # execute
            with engine.connect() as conn:
                conn.execute("DELETE FROM public.%s %s;" % (tb_to, C_condition))
                conn.close()
        else:
            raise ValueError('New DATA Columns are not same of exiting tables') 
    else:
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            conn.close()
    ########################################################################        
    print(f"Save to Postgres {df.shape}")
    try:
        rows = df.shape[0]
        n = 1
        if rows > 20000:
            n = math.ceil(rows / 20000)
        for i in range(n):
            r0 = i * 20000
            r1 = ((i + 1) * 20000)
            df_1 = df.iloc[r0:r1,:]
            df_1.to_sql(tb_to, engine, index=False, if_exists='append')
            print(f"ETL Process Loop : {i} Rows : {df_1.shape[0]}")
            del df_1
        print("ETL Process finished")
    except Exception as err:
        raise ValueError(err)
    ########################################################################
    common.Del_File(**kwargs)
    if os.path.exists(tb_to + ".parquet"):
        os.remove(tb_to + '.parquet')
    del df
    gc.collect()
    return True
    
with DAG(
    'Daily_Kopen_Service_job_db2pgS3_dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    ################### Service Job - 2 FY #############################################################################################################
    t001 = PythonOperator(
        task_id='el_kopen_service_job_data1',
        provide_context=True,
        python_callable=EL_process,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': " where smm_account_month >= '%s04' and smm_account_month < '%s04'" % (int(get_fisical_year()) - 2, int(get_fisical_year()) - 1)}
    )

    t002 = PythonOperator(
        task_id='prepare_kopen_service_job_data1',
        provide_context=True,
        python_callable=PP_process,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': " where smm_account_month >= '%s04' and smm_account_month < '%s04'" % (int(get_fisical_year()) - 2, int(get_fisical_year()) - 1)}
    )
    t002.set_upstream(t001)

    t003 = PythonOperator(
        task_id='copy_service_job_to_s3_data_lake1',
        provide_context=True,
        python_callable= common.copy_to_minio,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': "", 'Last_Days': 366, 'FY': str(int(get_fisical_year()) - 2)}
    )
    t003.set_upstream(t002)

    t004 = PythonOperator(
        task_id='copy_service_job_to_s3sl_data_lake1',
        provide_context=True,
        python_callable= common.copy_to_minio_sl,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': "", 'Last_Days': 366, 'FY': str(int(get_fisical_year()) - 2)}
    )
    t004.set_upstream(t003)

    t005 = PythonOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id='etl_kopen_service_job_data_lake1',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': " where smm_account_month >= '%s04' and smm_account_month < '%s04'" % (int(get_fisical_year()) - 2, int(get_fisical_year()) - 1)}
    )
    t005.set_upstream(t004)

    ################### Service Job - 1 FY #############################################################################################################
    t01 = PythonOperator(
        task_id='el_kopen_service_job_data2',
        provide_context=True,
        python_callable=EL_process,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': " where smm_account_month >= '%s04' and smm_account_month < '%s04'" % (int(get_fisical_year()) - 1, get_fisical_year())}
    )
    t01.set_upstream(t005)

    t02 = PythonOperator(
        task_id='prepare_kopen_service_job_data2',
        provide_context=True,
        python_callable=PP_process,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': " where smm_account_month >= '%s04' and smm_account_month < '%s04'" % (int(get_fisical_year()) - 1, get_fisical_year())}
    )
    t02.set_upstream(t01)

    t03 = PythonOperator(
        task_id='copy_service_job_to_s3_data_lake2',
        provide_context=True,
        python_callable= common.copy_to_minio,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': "", 'Last_Days': 366, 'FY': str(int(get_fisical_year()) - 1)}
    )
    t03.set_upstream(t02)

    t04 = PythonOperator(
        task_id='copy_service_job_to_s3sl_data_lake2',
        provide_context=True,
        python_callable= common.copy_to_minio_sl,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': "", 'Last_Days': 366, 'FY': str(int(get_fisical_year()) - 1)}
    )
    t04.set_upstream(t03)

    t05 = PythonOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id='etl_kopen_service_job_data_lake2',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'From_Table': "SERV_MISSION_MIND", 'To_Table': "kp_service_job", 'Chunk_Size': 50000, 'Key': 'smm_ticket_id', 'Condition': " where smm_account_month >= '%s04' and smm_account_month < '%s04'" % (int(get_fisical_year()) - 1, get_fisical_year())}
    )
    t05.set_upstream(t04)

    