from datetime import datetime, timedelta
import math
import pendulum
from airflow.models import DAG
#from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator
from airflow import AirflowException
#from airflow.utils.trigger_rule import TriggerRule
import sys, os
import gc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sqlalchemy

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common
from function import get_first_ym_fisical_year
from sql import sql_part_sale_head, schema_part_sale_head, columns_part_sale_head

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

    my_schema = schema_part_sale_head()
    rows = 0

    sqlstr = sql_part_sale_head() + C_condition

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
            #table = pq.read_table(tb_to + ".parquet", columns=[])
            ds_parquet = pq.ParquetDataset(
                '/opt/airflow/' + tb_to + '.parquet',
                filters=[('psh_account_month','>=', get_first_ym_fisical_year())]
            )
            table = ds_parquet.read(columns=columns_part_sale_head())
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

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    C_condition = kwargs['Condition']

    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    ########################################################################
    c_columns = 0
    # ETL ##################################################################
    #df = pd.read_parquet(tb_to + '.parquet')
    ds_parquet = pq.ParquetDataset(
                '/opt/airflow/' + tb_to + '.parquet',
                filters=[('psh_account_month','>=', get_first_ym_fisical_year())]
            )
    table = ds_parquet.read(columns=columns_part_sale_head())
    df = table.to_pandas()
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
                conn.execute("DELETE %s %s;" % (tb_to, C_condition))
                conn.close()
        else:
            raise ValueError('New DATA Columns are not same of exiting tables') 
    else:
        # execute
        with engine.connect() as conn:
            strexec = """CREATE TABLE IF NOT EXISTS public.kp_part_sale_head (
	            psh_ticket_id text NULL,
	            psh_s_tktid text NULL,
	            psh_org_id text NULL,
	            psh_dep_id text NULL,
	            psh_finance_date date NULL,
	            psh_account_month text NULL,
	            psh_wh_id text NULL,
	            psh_cus_id text NULL,
	            psh_valid_date date NULL,
	            psh_out_taxrate float8 NULL,
	            psh_discount float8 NULL,
	            psh_wh_tktid text NULL,
	            psh_wh_status text NULL,
	            psh_rec_date date NULL,
	            psh_passed text NULL,
	            psh_is_pur text NULL,
	            psh_remark text NULL,
	            psh_creater_id text NULL,
	            psh_create_date timestamp NULL,
	            psh_checker_id text NULL,
	            psh_check_date timestamp NULL,
	            psh_lastuserid text NULL,
	            psh_lasttime timestamp NULL,
	            psh_status text NULL,
	            psh_pay_day date NULL,
	            psh_sales text NULL,
	            psh_old_ticket_id text NULL,
	            psh_old_ticketid text NULL,
	            psh_invcus text NULL,
	            psh_is_int text NULL,
	            psh_pz_type text NULL,
	            psh_sale_type text NULL,
	            psh_unit_model text NULL,
	            psh_unit_id text NULL,
	            psh_currency text NULL,
	            psh_exchange_rate float8 NULL,
	            psh_basic_ttlmoney float8 NULL,
	            psh_basic_ttlamount float8 NULL,
	            psh_basic_ttltax float8 NULL,
	            psh_order_type text NULL,
	            psh_lp_info text NULL,
	            psh_pay_terms text NULL,
	            psh_usance_days int8 NULL,
	            psh_credit_type text NULL,
	            psh_ttltax_new float8 NULL,
                CONSTRAINT %s_pkey PRIMARY KEY (%s)
            );""" % (tb_to, primary_key)
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
            r1 = ((i + 1) * 20000) - 1
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
    'Kopen_Part_sale_1Hour_db2pgS3_dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:
    
    ################### Part_Sales #############################################################################################################
    t1 = PythonOperator(
        task_id='el_kopen_part_sale_data',
        provide_context=True,
        python_callable=EL_process,
        op_kwargs={'From_Table': "PART_SALE_HEAD", 'To_Table': "kp_part_sale_head", 'Chunk_Size': 50000, 'Key': 'psh_ticket_id', 'Condition': " where psh_account_month >= '%s'" % (get_first_ym_fisical_year())}
    )

    t2 = PythonOperator(
        task_id='prepare_kopen_part_sale_data',
        provide_context=True,
        python_callable=PP_process,
        op_kwargs={'From_Table': "PART_SALE_HEAD", 'To_Table': "kp_part_sale_head", 'Chunk_Size': 50000, 'Key': 'psh_ticket_id', 'Condition': " where psh_account_month >= '%s'" % (get_first_ym_fisical_year())}
    )
    t2.set_upstream(t1)

    t3 = PythonOperator(
        task_id='copy_part_sale_to_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_to_minio,
        op_kwargs={'From_Table': "PART_SALE_HEAD", 'To_Table': "kp_part_sale_head", 'Chunk_Size': 50000, 'Key': 'psh_ticket_id', 'Condition': "", 'Last_Days': 1}
    )
    t3.set_upstream(t2)

    t4 = PythonOperator(
        task_id='copy_part_sale_to_s3sl_data_lake',
        provide_context=True,
        python_callable= common.copy_to_minio_sl,
        op_kwargs={'From_Table': "PART_SALE_HEAD", 'To_Table': "kp_part_sale_head", 'Chunk_Size': 50000, 'Key': 'psh_ticket_id', 'Condition': "", 'Last_Days': 1}
    )
    t4.set_upstream(t3)

    t5 = PythonOperator(
        task_id='etl_kopen_part_sale_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'From_Table': "PART_SALE_HEAD", 'To_Table': "kp_part_sale_head", 'Chunk_Size': 50000, 'Key': 'psh_ticket_id', 'Condition': " where psh_account_month >= '%s'" % (get_first_ym_fisical_year())}
    )
    t5.set_upstream(t4)
