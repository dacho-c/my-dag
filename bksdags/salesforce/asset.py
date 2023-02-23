from datetime import datetime, timedelta
import json
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect
import sys, os
import gc
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from Class import common

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
        strexec = """CREATE TABLE IF NOT EXISTS public.sf_asset (
	        att_hour__c text NULL,
	        att_ratio__c text NULL,
	        accountid text NULL,
	        assetid__c text NULL,
	        assetlevel int8 NULL,
	        assetprovidedbyid text NULL,
	        assetservicedbyid text NULL,
	        asset_status__c text NULL,
	        breaker_period__c text NULL,
	        contactid text NULL,
	        contract__c text NULL,
	        createdbyid text NULL,
	        createddate text NULL,
	        customerbranch__c text NULL,
	        customer_kopen_id__c text NULL,
	        deliverdate__c text NULL,
	        deliveryyear__c float8 NULL,
	        description text NULL,
	        enginemodel__c text NULL,
	        engineserial__c text NULL,
	        fuel_burn__c text NULL,
	        fullserial__c text NULL,
	        gcpsdate__c text NULL,
	        id text NOT NULL,
	        installdate text NULL,
	        iscompetitorproduct bool NULL,
	        isdeleted bool NULL,
	        isinternal bool NULL,
	        komtraxid__c text NULL,
	        lastmodifiedbyid text NULL,
	        lastmodifieddate text NULL,
	        lastreferenceddate text NULL,
	        lastvieweddate text NULL,
	        machinebranch__c text NULL,
	        machine_branch_code__c text NULL,
	        machine_brand__c text NULL,
	        machine_status__c text NULL,
	        machine_type__c text NULL,
	        migrate__c bool NULL,
	        model_serial_case__c text NULL,
	        model_serial__c text NULL,
	        model__c text NULL,
	        model_for_pssr__c text NULL,
	        "name" text NULL,
	        newused__c text NULL,
	        opportunity__c text NULL,
	        other_model__c text NULL,
	        ownerid text NULL,
	        parentid text NULL,
	        price text NULL,
	        product2id text NULL,
	        productcode text NULL,
	        purchasedate text NULL,
	        purchasedate__c text NULL,
	        quantity text NULL,
	        recordtypeid text NULL,
	        remark__c text NULL,
	        rootassetid text NULL,
	        smrdate__c text NULL,
	        smrsource__c text NULL,
	        salesroute__c text NULL,
	        search_key__c text NULL,
	        serialnumber text NULL,
	        servicedescription__c text NULL,
	        servicetype__c text NULL,
	        specification__c text NULL,
	        status text NULL,
	        stockkeepingunit text NULL,
	        systemmodstamp text NULL,
	        testdate__c text NULL,
	        travel_ratio__c text NULL,
	        travel_time__c text NULL,
	        usageenddate text NULL,
	        warrantyenddate__c text NULL,
	        workinghourf__c float8 NULL,
	        workinghour__c float8 NULL,
	        genopty12000machine__c bool NULL,
	        genopty12000__c bool NULL,
	        genopty7000__c bool NULL,
	        genoptyfuelburn_200000__c bool NULL,
	        genoptyfuelburn__c bool NULL,
	        genoptynosales1year_nopmcontract__c bool NULL,
	        genoptynosales1year__c bool NULL,
	        genoptysmr_7000_nosalesrecord2years__c bool NULL,
	        genoptysmr_7000_travalratio_10__c bool NULL,
	        genoptytravelratio__c bool NULL,
	        isupdated__c bool NULL,
	        lastoptynotify__c text NULL,
            CONSTRAINT %s_pkey PRIMARY KEY (%s)
        );""" % (tb_to, primary_key)
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            conn.close()
    ###############################################################################
    return result_state

def ETL_process(**kwargs):

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']

    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    ########################################################################
    c_columns = 0
    # ETL ##################################################################
    df = pd.read_parquet(tb_to + '.parquet')
    ########################################################################
    #df['st_adjustment_date'] = df['st_adjustment_date'].fillna('1999-01-01')
    #df.pro_name = df.pro_name.str.replace(",", " ")
    #df = df.drop_duplicates(subset=['pro_komcode'])
    ########################################################################
    ctable = "SELECT count(*) as c FROM information_schema.columns WHERE table_name = '%s';" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    c_columns = result.loc[0,'c']
    ########################################################################
    ctable = "SELECT count(*) as c FROM %s;" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    c_rows = result.loc[0,'c']
    ########################################################################
    print(f"DF (rows, col) :  {df.shape}")
    if c_columns == df.shape[1]:
        if c_rows > 0:
            # execute
            with engine.connect() as conn:
                conn.execute("DROP TABLE IF EXISTS %s;" % (tb_to))
                strexec = """CREATE TABLE IF NOT EXISTS public.sf_asset (
	            att_hour__c text NULL,
	            att_ratio__c text NULL,
	            accountid text NULL,
	            assetid__c text NULL,
	            assetlevel int8 NULL,
	            assetprovidedbyid text NULL,
	            assetservicedbyid text NULL,
	            asset_status__c text NULL,
	            breaker_period__c text NULL,
	            contactid text NULL,
	            contract__c text NULL,
	            createdbyid text NULL,
	            createddate text NULL,
	            customerbranch__c text NULL,
	            customer_kopen_id__c text NULL,
	            deliverdate__c text NULL,
	            deliveryyear__c float8 NULL,
	            description text NULL,
	            enginemodel__c text NULL,
	            engineserial__c text NULL,
	            fuel_burn__c text NULL,
	            fullserial__c text NULL,
	            gcpsdate__c text NULL,
	            id text NOT NULL,
	            installdate text NULL,
	            iscompetitorproduct bool NULL,
	            isdeleted bool NULL,
	            isinternal bool NULL,
	            komtraxid__c text NULL,
	            lastmodifiedbyid text NULL,
	            lastmodifieddate text NULL,
	            lastreferenceddate text NULL,
	            lastvieweddate text NULL,
	            machinebranch__c text NULL,
	            machine_branch_code__c text NULL,
	            machine_brand__c text NULL,
	            machine_status__c text NULL,
	            machine_type__c text NULL,
	            migrate__c bool NULL,
	            model_serial_case__c text NULL,
	            model_serial__c text NULL,
	            model__c text NULL,
	            model_for_pssr__c text NULL,
	            "name" text NULL,
	            newused__c text NULL,
	            opportunity__c text NULL,
	            other_model__c text NULL,
	            ownerid text NULL,
	            parentid text NULL,
	            price text NULL,
	            product2id text NULL,
	            productcode text NULL,
	            purchasedate text NULL,
	            purchasedate__c text NULL,
	            quantity text NULL,
	            recordtypeid text NULL,
	            remark__c text NULL,
	            rootassetid text NULL,
	            smrdate__c text NULL,
	            smrsource__c text NULL,
	            salesroute__c text NULL,
	            search_key__c text NULL,
	            serialnumber text NULL,
	            servicedescription__c text NULL,
	            servicetype__c text NULL,
	            specification__c text NULL,
	            status text NULL,
	            stockkeepingunit text NULL,
	            systemmodstamp text NULL,
	            testdate__c text NULL,
	            travel_ratio__c text NULL,
	            travel_time__c text NULL,
	            usageenddate text NULL,
	            warrantyenddate__c text NULL,
	            workinghourf__c float8 NULL,
	            workinghour__c float8 NULL,
	            genopty12000machine__c bool NULL,
	            genopty12000__c bool NULL,
	            genopty7000__c bool NULL,
	            genoptyfuelburn_200000__c bool NULL,
	            genoptyfuelburn__c bool NULL,
	            genoptynosales1year_nopmcontract__c bool NULL,
	            genoptynosales1year__c bool NULL,
	            genoptysmr_7000_nosalesrecord2years__c bool NULL,
	            genoptysmr_7000_travalratio_10__c bool NULL,
	            genoptytravelratio__c bool NULL,
	            isupdated__c bool NULL,
	            lastoptynotify__c text NULL,
                CONSTRAINT %s_pkey PRIMARY KEY (%s)
                );""" % (tb_to, primary_key)
                conn.execute(strexec)
                conn.close()
        print(f"Save to Postgres {df.shape}")
        try:
            df.to_sql(tb_to, engine, index=False, if_exists='append')
            del df
            print("ETL Process finished")
        except Exception as err:
            raise ValueError(err)
    else:
        raise ValueError('New DATA Columns are not same of exiting tables') 
    ########################################################################        
    common.Del_File(**kwargs)
    if os.path.exists(tb_to + ".parquet"):
        os.remove(tb_to + '.parquet')
    gc.collect()
    return True

def upsert(session, table, update_cols, rows):

    stmt = insert(table).values(rows)

    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=table.primary_key.columns,
        set_={k: getattr(stmt.excluded, k) for k in update_cols}
        #,
        #index_where= ( as_of_date_col < getattr(stmt.excluded, as_of_date_col))
        )
    session.execute(on_conflict_stmt)

def UPSERT_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")
    # Start Session
    Base = declarative_base()
    session = sessionmaker(bind=engine)
    session = session()
    Base.metadata.create_all(engine)

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    schema = 'public'
    no_update_cols = []

    print('Initial state:\n')
    df_main = pd.read_sql_query(sql=sqlalchemy.text("select * from %s_tmp" % (tb_to)), con=engine)
    rows = len(df_main)
    if (rows > 0):
        metadata = MetaData(schema=schema)
        metadata.bind = engine
        table = Table(tb_to, metadata, schema=schema, autoload=True)
        update_cols = [c.name for c in table.c
            if c not in list(table.primary_key.columns)
            and c.name not in no_update_cols]

        for index, row in df_main.iterrows():
            print(f"Upsert progress {index + 1}/{rows}")
            upsert(session,table,update_cols,row)
    
    print(f"Upsert Completed {rows} records.\n")
    session.commit()
    session.close()
    print('Upsert session commit')

def INSERT_bluk(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    strexec = ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
    strexec += ("ALTER TABLE %s ADD PRIMARY KEY (%s);" % (tb_to, primary_key))
    # execute
    with engine.connect() as conn:
        conn.execute(strexec)
        print("ETL DL Process finished")
        conn.close()

def Cleansing_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    C_condition = kwargs['Condition']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        strexec = ("DELETE FROM %s WHERE NOT (EXISTS (SELECT %s FROM %s WHERE %s.%s = %s.%s)) %s;") % (tb_to, primary_key, tb_to + '_tmp',tb_to,primary_key,tb_to + '_tmp',primary_key,C_condition)
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            print("Drop Temp Table.")
            conn.execute("DROP TABLE IF EXISTS %s;" % (tb_to + '_tmp'))
            conn.close()

def Check_exiting(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        return True
    else:
        return False   

def branch_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="check_exit_sf_asset_data", key='return_value'))
    if xcom_value:
        return "upsert_sf_asset_on_data_lake"
    else:
        return "create_new_sf_asset_table"

with DAG(
    'Salesforce_Asset_ETLfromS3_dag',
    tags=['Salesforce'],
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    ################### Salesforce Asset ######################################################################################################
    t1 = HttpSensor(
        task_id='is_api_active',
        http_conn_id='bks_api',
        endpoint='etl/sf/',
        execution_timeout=timedelta(seconds=120),
        timeout=300,
        retries=3,
        mode="reschedule",
    )

    t2 = SimpleHttpOperator(
        task_id='get_salesforce_asset_object',
        http_conn_id='bks_api',
        method='POST',
        endpoint='etl/sf/sfasset',
        #data=json.dumps({"q": 0}),
        #headers={"Content-Type": "application/json"},
        log_response=True
    )
    t2.set_upstream(t1)

    t3 = PythonOperator(
        task_id='copy_sf_asset_from_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_from_minio,
        op_kwargs={'To_Table': "sf_asset", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': "", 'Last_Days': 0}
    )
    t3.set_upstream(t2)

    t4 = PythonOperator(
        task_id='prepare_salesforce_asset',
        provide_context=True,
        python_callable= PP_process,
        op_kwargs={'To_Table': "sf_asset", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t4.set_upstream(t3)

    t5 = PythonOperator(
        task_id='etl_sf_asset_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "sf_asset", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t5.set_upstream(t4)