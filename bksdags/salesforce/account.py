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
            result_state = False
            raise ValueError('Not exiting Parquet files') 
    else:
        strexec = """CREATE TABLE IF NOT EXISTS public.sf_account (
	        ac_reference__c text NULL,
	        accountsource text NULL,
	        account_active_status__c text NULL,
	        account_name_eng__c text NULL,
	        account_profile_type__c text NULL,
	        account_type__c text NULL,
	        address_branch__c text NULL,
	        address__c text NULL,
	        billingaddress text NULL,
	        billingcity text NULL,
	        billingcountry text NULL,
	        billinggeocodeaccuracy text NULL,
	        billinglatitude text NULL,
	        billinglongitude text NULL,
	        billingpostalcode text NULL,
	        billingstate text NULL,
	        billingstreet text NULL,
	        billing_rate__c text NULL,
	        branch_id__c text NULL,
	        branch_name__c text NULL,
	        branch__c text NULL,
	        business_types__c text NULL,
	        ca_address_contact__c text NULL,
	        ca_address__c text NULL,
	        ca_contact_zip__c text NULL,
	        ca_fax__c text NULL,
	        ca_tel__c text NULL,
	        cusid_addrid__c text NULL,
	        cus_idcard__c text NULL,
	        cus_status__c text NULL,
	        cus_tax_id__c text NULL,
	        createdbyid text NULL,
	        createddate text NULL,
	        credit_limit__c text NULL,
	        cus_branch__c text NULL,
	        cus_city__c text NULL,
	        cus_country__c text NULL,
	        cus_created_time__c text NULL,
	        cus_id__c text NULL,
	        cus_industry__c text NULL,
	        cus_lp_category__c text NULL,
	        cus_lprice_no__c text NULL,
	        cus_province__c text NULL,
	        cus_sales__c text NULL,
	        cus_type__c text NULL,
	        cust_tax_no__c text NULL,
	        customer_crediblility_weight__c text NULL,
	        customer_crediblility__c float8 NULL,
	        customer_grade__c text NULL,
	        customer_grading__c text NULL,
	        customer_rank__c float8 NULL,
	        customer_type__c text NULL,
	        department_by_user__c text NULL,
	        description text NULL,
	        district__c text NULL,
	        eg_kopen_address_id__c text NULL,
	        eg_kopen_id__c text NULL,
	        emp_name__c text NULL,
	        first_contact_channel__c text NULL,
	        first_contact_date__c text NULL,
	        formula_customer_grading__c float8 NULL,
	        id__c text NULL,
	        id text NOT NULL,
	        industry text NULL,
	        isdeleted bool NULL,
	        jigsaw text NULL,
	        jigsawcompanyid text NULL,
	        kbl_good_customers__c bool NULL,
	        kopen_id__c text NULL,
	        lastactivitydate text NULL,
	        lastmodifiedbyid text NULL,
	        lastmodifieddate text NULL,
	        lastreferenceddate text NULL,
	        lastvieweddate text NULL,
	        last_contact_date__c text NULL,
	        mc_cust__c text NULL,
	        mc_pop__c text NULL,
	        machine_population_point__c float8 NULL,
	        machine_population_rank__c float8 NULL,
	        machine_population_score__c text NULL,
	        machine_population_weight__c text NULL,
	        machine_purchase_point__c float8 NULL,
	        machine_purchase_profit_point__c float8 NULL,
	        machine_purchase_profit_rank__c float8 NULL,
	        machine_purchase_profit_score__c text NULL,
	        machine_purchase_profit__c text NULL,
	        machine_purchase_score__c text NULL,
	        machine_purchase_weight__c text NULL,
	        machine_purchase__c float8 NULL,
	        machine_qty__c float8 NULL,
	        machine_retention_point__c float8 NULL,
	        machine_retention_weight__c text NULL,
	        machine_retention__c float8 NULL,
	        major_brand_2__c text NULL,
	        major_brand_3__c text NULL,
	        major_brand_4__c text NULL,
	        major_brand_5__c text NULL,
	        major_brand__c text NULL,
	        masterrecordid text NULL,
	        "name" text NULL,
	        no_reminders__c text NULL,
	        noti_acc_create_date__c text NULL,
	        numberofemployees text NULL,
	        oracle_cust_code__c text NULL,
	        outstanding_amount__c text NULL,
	        overdue_times__c float8 NULL,
	        ownerid text NULL,
	        pssr_sales__c text NULL,
	        paid_amount__c text NULL,
	        parentid text NULL,
	        parent_id__c text NULL,
	        part_service_margin_point__c float8 NULL,
	        part_service_margin_ranking__c float8 NULL,
	        part_service_margin_score__c text NULL,
	        part_service_margin_weight__c text NULL,
	        part_service_r_m_point__c float8 NULL,
	        part_service_r_m_rank__c float8 NULL,
	        part_service_r_m_score__c text NULL,
	        part_service_r_m_weight__c text NULL,
	        part_service_revenue_point__c float8 NULL,
	        part_service_revenue_ranking__c float8 NULL,
	        part_service_revenue_score__c text NULL,
	        part_service_revenue_weight__c text NULL,
	        pay_terms__c text NULL,
	        phone text NULL,
	        photourl text NULL,
	        prospect_id__c text NULL,
	        province__c text NULL,
	        prts_cust__c text NULL,
	        recordtypeid text NULL,
	        region_by_user__c text NULL,
	        svc_cust__c text NULL,
	        sales_amount__c text NULL,
	        servicerevpermachine__c float8 NULL,
	        shippingaddress text NULL,
	        shippingcity text NULL,
	        shippingcountry text NULL,
	        shippinggeocodeaccuracy text NULL,
	        shippinglatitude text NULL,
	        shippinglongitude text NULL,
	        shippingpostalcode text NULL,
	        shippingstate text NULL,
	        shippingstreet text NULL,
	        sicdesc text NULL,
	        status__c text NULL,
	        sub_district__c text NULL,
	        systemmodstamp text NULL,
	        tempvalue__c text NULL,
	        total_machine_profit__c float8 NULL,
	        total_part_service_margin__c float8 NULL,
	        total_part_service_revenue__c float8 NULL,
	        travel_fee__c text NULL,
	        tumbol__c text NULL,
	        "type" text NULL,
	        website text NULL,
	        zip_code__c text NULL,
	        lastpurchase__c text NULL,
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
                strexec = """CREATE TABLE IF NOT EXISTS public.sf_account (
	            ac_reference__c text NULL,
	            accountsource text NULL,
	            account_active_status__c text NULL,
	            account_name_eng__c text NULL,
	            account_profile_type__c text NULL,
	            account_type__c text NULL,
	            address_branch__c text NULL,
	            address__c text NULL,
	            billingaddress text NULL,
	            billingcity text NULL,
	            billingcountry text NULL,
	            billinggeocodeaccuracy text NULL,
	            billinglatitude text NULL,
	            billinglongitude text NULL,
	            billingpostalcode text NULL,
	            billingstate text NULL,
	            billingstreet text NULL,
	            billing_rate__c text NULL,
	            branch_id__c text NULL,
	            branch_name__c text NULL,
	            branch__c text NULL,
	            business_types__c text NULL,
	            ca_address_contact__c text NULL,
	            ca_address__c text NULL,
	            ca_contact_zip__c text NULL,
	            ca_fax__c text NULL,
	            ca_tel__c text NULL,
	            cusid_addrid__c text NULL,
	            cus_idcard__c text NULL,
	            cus_status__c text NULL,
	            cus_tax_id__c text NULL,
	            createdbyid text NULL,
	            createddate text NULL,
	            credit_limit__c text NULL,
	            cus_branch__c text NULL,
	            cus_city__c text NULL,
	            cus_country__c text NULL,
	            cus_created_time__c text NULL,
	            cus_id__c text NULL,
	            cus_industry__c text NULL,
	            cus_lp_category__c text NULL,
	            cus_lprice_no__c text NULL,
	            cus_province__c text NULL,
	            cus_sales__c text NULL,
	            cus_type__c text NULL,
	            cust_tax_no__c text NULL,
	            customer_crediblility_weight__c text NULL,
	            customer_crediblility__c float8 NULL,
	            customer_grade__c text NULL,
	            customer_grading__c text NULL,
	            customer_rank__c float8 NULL,
	            customer_type__c text NULL,
	            department_by_user__c text NULL,
	            description text NULL,
	            district__c text NULL,
	            eg_kopen_address_id__c text NULL,
	            eg_kopen_id__c text NULL,
	            emp_name__c text NULL,
	            first_contact_channel__c text NULL,
	            first_contact_date__c text NULL,
	            formula_customer_grading__c float8 NULL,
	            id__c text NULL,
	            id text NOT NULL,
	            industry text NULL,
	            isdeleted bool NULL,
	            jigsaw text NULL,
	            jigsawcompanyid text NULL,
	            kbl_good_customers__c bool NULL,
	            kopen_id__c text NULL,
	            lastactivitydate text NULL,
	            lastmodifiedbyid text NULL,
	            lastmodifieddate text NULL,
	            lastreferenceddate text NULL,
	            lastvieweddate text NULL,
	            last_contact_date__c text NULL,
	            mc_cust__c text NULL,
	            mc_pop__c text NULL,
	            machine_population_point__c float8 NULL,
	            machine_population_rank__c float8 NULL,
	            machine_population_score__c text NULL,
	            machine_population_weight__c text NULL,
	            machine_purchase_point__c float8 NULL,
	            machine_purchase_profit_point__c float8 NULL,
	            machine_purchase_profit_rank__c float8 NULL,
	            machine_purchase_profit_score__c text NULL,
	            machine_purchase_profit__c text NULL,
	            machine_purchase_score__c text NULL,
	            machine_purchase_weight__c text NULL,
	            machine_purchase__c float8 NULL,
	            machine_qty__c float8 NULL,
	            machine_retention_point__c float8 NULL,
	            machine_retention_weight__c text NULL,
	            machine_retention__c float8 NULL,
	            major_brand_2__c text NULL,
	            major_brand_3__c text NULL,
	            major_brand_4__c text NULL,
	            major_brand_5__c text NULL,
	            major_brand__c text NULL,
	            masterrecordid text NULL,
	            "name" text NULL,
	            no_reminders__c text NULL,
	            noti_acc_create_date__c text NULL,
	            numberofemployees text NULL,
	            oracle_cust_code__c text NULL,
	            outstanding_amount__c text NULL,
	            overdue_times__c float8 NULL,
	            ownerid text NULL,
	            pssr_sales__c text NULL,
	            paid_amount__c text NULL,
	            parentid text NULL,
	            parent_id__c text NULL,
	            part_service_margin_point__c float8 NULL,
	            part_service_margin_ranking__c float8 NULL,
	            part_service_margin_score__c text NULL,
	            part_service_margin_weight__c text NULL,
	            part_service_r_m_point__c float8 NULL,
	            part_service_r_m_rank__c float8 NULL,
	            part_service_r_m_score__c text NULL,
	            part_service_r_m_weight__c text NULL,
	            part_service_revenue_point__c float8 NULL,
	            part_service_revenue_ranking__c float8 NULL,
	            part_service_revenue_score__c text NULL,
	            part_service_revenue_weight__c text NULL,
	            pay_terms__c text NULL,
	            phone text NULL,
	            photourl text NULL,
	            prospect_id__c text NULL,
	            province__c text NULL,
	            prts_cust__c text NULL,
	            recordtypeid text NULL,
	            region_by_user__c text NULL,
	            svc_cust__c text NULL,
	            sales_amount__c text NULL,
	            servicerevpermachine__c float8 NULL,
	            shippingaddress text NULL,
	            shippingcity text NULL,
	            shippingcountry text NULL,
	            shippinggeocodeaccuracy text NULL,
	            shippinglatitude text NULL,
	            shippinglongitude text NULL,
	            shippingpostalcode text NULL,
	            shippingstate text NULL,
	            shippingstreet text NULL,
	            sicdesc text NULL,
	            status__c text NULL,
	            sub_district__c text NULL,
	            systemmodstamp text NULL,
	            tempvalue__c text NULL,
	            total_machine_profit__c float8 NULL,
	            total_part_service_margin__c float8 NULL,
	            total_part_service_revenue__c float8 NULL,
	            travel_fee__c text NULL,
	            tumbol__c text NULL,
	            "type" text NULL,
	            website text NULL,
	            zip_code__c text NULL,
	            lastpurchase__c text NULL,
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
    xcom_value = bool(ti.xcom_pull(task_ids="check_exit_sf_account_data", key='return_value'))
    if xcom_value:
        return "upsert_sf_account_on_data_lake"
    else:
        return "create_new_sf_account_table"

with DAG(
    'Salesforce_Account_ETLfromS3_dag',
    tags=['Salesforce'],
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    ################### Salesforce Account ######################################################################################################
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
        task_id='get_salesforce_account_object',
        http_conn_id='bks_api',
        method='POST',
        endpoint='etl/sf/sfaccount',
        #data=json.dumps({"q": 0}),
        #headers={"Content-Type": "application/json"},
        log_response=True
    )
    t2.set_upstream(t1)

    t3 = PythonOperator(
        task_id='copy_sf_account_from_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_from_minio,
        op_kwargs={'To_Table': "sf_account", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': "", 'Last_Days': 0}
    )
    t3.set_upstream(t2)

    t4 = PythonOperator(
        task_id='prepare_salesforce_account',
        provide_context=True,
        python_callable= PP_process,
        op_kwargs={'To_Table': "sf_account", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t4.set_upstream(t3)

    t5 = PythonOperator(
        task_id='etl_sf_account_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "sf_account", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t5.set_upstream(t4)