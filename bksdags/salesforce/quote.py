from datetime import datetime, timedelta
import json
import math
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
from function import get_fisical_year

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
        ctable = "SELECT count(*) as c FROM %s where fy ='%s';" % (tb_to,get_fisical_year())
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
            result_state = False
            raise ValueError('Not exiting Parquet files') 
    else:
        strexec = """CREATE TABLE IF NOT EXISTS public.sf_quote (
	        accountid text NULL,
	        account_source__c text NULL,
	        account__c text NULL,
	        actual_revenue__c float8 NULL,
	        additionaladdress text NULL,
	        additionalcity text NULL,
	        additionalcountry text NULL,
	        additionalgeocodeaccuracy text NULL,
	        additionallatitude text NULL,
	        additionallongitude text NULL,
	        additionalname text NULL,
	        additionalpostalcode text NULL,
	        additionalstate text NULL,
	        additionalstreet text NULL,
	        addresslookup__c text NULL,
	        address__c text NULL,
	        approve_date_layout__c text NULL,
	        approve_date__c text NULL,
	        approve_id__c text NULL,
	        approver__c text NULL,
	        bks_signature__c text NULL,
	        billto_district__c text NULL,
	        billto_province__c text NULL,
	        billto_tumbol__c text NULL,
	        bill_to_address__c text NULL,
	        billingaddress text NULL,
	        billingcity text NULL,
	        billingcountry text NULL,
	        billinggeocodeaccuracy text NULL,
	        billinglatitude text NULL,
	        billinglongitude text NULL,
	        billingname text NULL,
	        billingpostalcode text NULL,
	        billingstate text NULL,
	        billingstreet text NULL,
	        branch__c text NULL,
	        branch_by_user__c text NULL,
	        cus_id__c text NULL,
	        cancreatequotelineitems bool NULL,
	        cash_price__c float8 NULL,
	        contactid text NULL,
	        contractid text NULL,
	        contract_amount__c float8 NULL,
	        createdatefortest__c text NULL,
	        createyear__c text NULL,
	        create_date__c text NULL,
	        createdbyid text NULL,
	        createddate text NULL,
	        creater_id__c text NULL,
	        creater__c text NULL,
	        currency__c text NULL,
	        customer_name__c text NULL,
	        customer_site__c text NULL,
	        customer_title__c text NULL,
	        date__c text NULL,
	        delivery_within_day__c float8 NULL,
	        department_by_user__c text NULL,
	        description text NULL,
	        discount float8 NULL,
	        discount_bath__c float8 NULL,
	        discount_rate__c float8 NULL,
	        down_payment__c float8 NULL,
	        due_date__c text NULL,
	        email text NULL,
	        enable_print_lv1__c bool NULL,
	        enable_print_lv2__c bool NULL,
	        enable_print_lv3__c bool NULL,
	        enable_print_lv4__c bool NULL,
	        enable_print_lv5__c bool NULL,
	        exchange_rate__c float8 NULL,
	        expirationdate text NULL,
	        fax text NULL,
	        finance_amount__c float8 NULL,
	        free_item_details__c text NULL,
	        free_item_values__c float8 NULL,
	        grandtotal float8 NULL,
	        grand_total__c float8 NULL,
	        id text NULL,
	        installment_type__c text NULL,
	        insurance_interest__c float8 NULL,
	        insurance_method__c text NULL,
	        insurance_vat__c float8 NULL,
	        insurance_type__c text NULL,
	        interest_amount__c float8 NULL,
	        interest_period__c float8 NULL,
	        interest_type__c text NULL,
	        interest__c float8 NULL,
	        isdeleted bool NULL,
	        issyncing bool NULL,
	        job_type__c text NULL,
	        lastmodifiedbyid text NULL,
	        lastmodifieddate text NULL,
	        lastreferenceddate text NULL,
	        lastvieweddate text NULL,
	        last_edit__c text NULL,
	        lineitemcount int8 NULL,
	        machine_interest__c float8 NULL,
	        model__c text NULL,
	        "name" text NULL,
	        needcustomersign__c bool NULL,
	        needsalessignature__c bool NULL,
	        opportunityid text NULL,
	        opportunity_code__c text NULL,
	        order_type__c text NULL,
	        original_quote__c text NULL,
	        ownerid text NULL,
	        payment_type__c text NULL,
	        period__c float8 NULL,
	        phone text NULL,
	        premium__c float8 NULL,
	        premium_rate__c float8 NULL,
	        price_code__c text NULL,
	        pricebook2id text NULL,
	        quotation_amount__c float8 NULL,
	        quotation_id__c text NULL,
	        quotenumber text NULL,
	        quotepdf__c text NULL,
	        quotetoaddress text NULL,
	        quotetocity text NULL,
	        quotetocountry text NULL,
	        quotetogeocodeaccuracy text NULL,
	        quotetolatitude text NULL,
	        quotetolongitude text NULL,
	        quotetoname text NULL,
	        quotetopostalcode text NULL,
	        quotetostate text NULL,
	        quotetostreet text NULL,
	        quote_numbers__c text NULL,
	        recordtypeid text NULL,
	        region_by_user__c text NULL,
	        remark_section__c text NULL,
	        sales_contact_name__c text NULL,
	        sales_id__c text NULL,
	        sales_man__c text NULL,
	        serial__c text NULL,
	        shipto_district__c text NULL,
	        shipto_province__c text NULL,
	        shipto_tumbol__c text NULL,
	        ship_to_address__c text NULL,
	        shippingaddress text NULL,
	        shippingcity text NULL,
	        shippingcountry text NULL,
	        shippinggeocodeaccuracy text NULL,
	        shippinghandling text NULL,
	        shippinglatitude text NULL,
	        shippinglongitude text NULL,
	        shippingname text NULL,
	        shippingpostalcode text NULL,
	        shippingstate text NULL,
	        shippingstreet text NULL,
	        site_address__c text NULL,
	        stamp_duty_0_4__c float8 NULL,
	        status text NULL,
	        status_desc__c text NULL,
	        status__c text NULL,
	        subtotal float8 NULL,
	        sum_insured__c float8 NULL,
	        sync_to_opportunity__c bool NULL,
	        systemmodstamp text NULL,
	        system_test__c text NULL,
	        tax text NULL,
	        tel__c text NULL,
	        to__c text NULL,
	        totalprice float8 NULL,
	        total_insured_money__c float8 NULL,
	        total_before_vat__c float8 NULL,
	        vat_7_total__c float8 NULL,
	        vat_7__c float8 NULL,
	        warranty_year__c float8 NULL,
	        x1st_installment__c float8 NULL,
	        x2nd_installment__c text NULL,
	        x2nd_installment_and_after__c float8 NULL,
	        isapproved__c bool NULL,
	        isbelowlevel5__c bool NULL,
	        isclone__c bool NULL,
	        isnotified__c bool NULL,
	        isrejected__c bool NULL,
            fy text NULL,
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
    df['fy'] = get_fisical_year()
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
                conn.execute("DELETE FROM %s WHERE fy = '%s';" % (tb_to, get_fisical_year()))
                conn.close()
        print(f"Save to Postgres {df.shape}")
        try:
            rows = df.shape[0]
            n = 1
            if rows > 20000:
                n = math.ceil(rows / 20000)
            for i in range(n):
                r0 = i * 20000
                if i > 0:
                    r0 = r0 + 1
                r1 = ((i + 1) * 20000)
                df_1 = df.iloc[r0:r1,:]
                df_1.to_sql(tb_to, engine, index=False, if_exists='append')
                print(f"ETL Process Loop : {i} Rows : {df_1.shape[0]}")
                del df_1
            print("ETL Process finished")
        except Exception as err:
            raise ValueError(err)
    else:
        raise ValueError('New DATA Columns are not same of exiting tables') 
    ########################################################################        
    common.Del_File(**kwargs)
    if os.path.exists(tb_to + ".parquet"):
        os.remove(tb_to + '.parquet')
    del df
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
    xcom_value = bool(ti.xcom_pull(task_ids="check_exit_sf_quote_data", key='return_value'))
    if xcom_value:
        return "upsert_sf_quote_on_data_lake"
    else:
        return "create_new_sf_quote_table"

with DAG(
    'Salesforce_Quote_ETLfromS3_dag',
    tags=['Salesforce'],
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    ################### Salesforce Quote ######################################################################################################
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
        task_id='get_salesforce_quote_object',
        http_conn_id='bks_api',
        method='POST',
        endpoint='etl/sf/sfquote?fy=' + get_fisical_year(),
        log_response=True
    )
    t2.set_upstream(t1)

    t3 = PythonOperator(
        task_id='copy_sf_quote_from_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_from_minio,
        op_kwargs={'To_Table': "sf_quote", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': "", 'Last_Days': 365}
    )
    t3.set_upstream(t2)

    t4 = PythonOperator(
        task_id='prepare_salesforce_quote',
        provide_context=True,
        python_callable= PP_process,
        op_kwargs={'To_Table': "sf_quote", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t4.set_upstream(t3)

    t5 = PythonOperator(
        task_id='etl_sf_quote_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "sf_quote", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t5.set_upstream(t4)