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
        strexec = """CREATE TABLE IF NOT EXISTS public.sf_opportunity (
	        accountid text NULL,
	        account_created_by__c text NULL,
	        account_source__c text NULL,
	        actual_visited__c text NULL,
	        amount float8 NULL,
	        announce_date__c text NULL,
	        asset_serial_number__c text NULL,
	        asset__c text NULL,
	        branch_by_user__c text NULL,
	        branch_return__c text NULL,
	        budget_confirmed__c bool NULL,
	        budget_year__c text NULL,
	        b_mode_contract_status__c text NULL,
	        b_mode_status__c text NULL,
	        campaignid text NULL,
	        case__c text NULL,
	        cash_price_include_vat_1__c float8 NULL,
	        cash_price_include_vat_2__c float8 NULL,
	        cash_price_include_vat_3__c text NULL,
	        checkactualvisit__c text NULL,
	        closedate text NULL,
	        competitor_brand_1__c text NULL,
	        competitor_brand_2__c text NULL,
	        competitor_brand_3__c text NULL,
	        competitor_model_1__c text NULL,
	        competitor_model_2__c text NULL,
	        competitor_model_3__c text NULL,
	        competitor_proposal__c text NULL,
	        contactid text NULL,
	        contractid text NULL,
	        contract_date__c text NULL,
	        contract_finish_date__c text NULL,
	        contract_no__c text NULL,
	        contract_type__c text NULL,
	        contract__c text NULL,
	        createdbyid text NULL,
	        createddate text NULL,
	        created_delivery_order__c bool NULL,
	        create_delivery_document_date__c text NULL,
	        create_order__c bool NULL,
	        customer_sign_date__c text NULL,
	        cus_id__c text NULL,
	        department_by_user__c text NULL,
	        dept_name__c text NULL,
	        dept_sub_name__c text NULL,
	        description text NULL,
	        discovery_completed__c bool NULL,
	        district__c text NULL,
	        down_payment_1__c float8 NULL,
	        down_payment_2__c text NULL,
	        down_payment_3__c text NULL,
	        factory_return__c text NULL,
	        file_attachment_check__c bool NULL,
	        fiscal text NULL,
	        fiscalquarter int8 NULL,
	        fiscalyear int8 NULL,
	        focus_activity_type__c text NULL,
	        forecastcategory text NULL,
	        forecastcategoryname text NULL,
	        hasopenactivity bool NULL,
	        hasopportunitylineitem bool NULL,
	        hasoverduetask bool NULL,
	        id text NULL,
	        installment_period_month_1__c float8 NULL,
	        installment_period_month_2__c text NULL,
	        installment_period_month_3__c text NULL,
	        int_rate_1__c float8 NULL,
	        int_rate_2__c text NULL,
	        int_rate_3__c text NULL,
	        isclosed bool NULL,
	        isdeleted bool NULL,
	        isexcludedfromterritory2filter bool NULL,
	        iswon bool NULL,
	        job__c text NULL,
	        komatsu_cash_price__c float8 NULL,
	        komatsu_down_payment__c float8 NULL,
	        komatsu_installment_period__c float8 NULL,
	        komatsu_int_rate__c float8 NULL,
	        komatsu_special_condition__c text NULL,
	        komtrax_4g__c text NULL,
	        lastactivitydate text NULL,
	        lastamountchangedhistoryid text NULL,
	        lastclosedatechangedhistoryid text NULL,
	        lastmodifiedbyid text NULL,
	        lastmodifieddate text NULL,
	        lastreferenceddate text NULL,
	        laststagechangedate text NULL,
	        lastvieweddate text NULL,
	        leadsource text NULL,
	        loss_reason__c text NULL,
	        loss_to_competitor_1__c bool NULL,
	        loss_to_competitor_2__c bool NULL,
	        loss_to_competitor_3__c bool NULL,
	        lost_reason_pm_contract__c text NULL,
	        lost_reason_pssr__c text NULL,
	        lost_reason__c text NULL,
	        machine_brand_1__c text NULL,
	        machine_brand_2__c text NULL,
	        machine_brand_3__c text NULL,
	        machine_inspection_history__c text NULL,
	        machine_model_1__c text NULL,
	        machine_model_2__c text NULL,
	        machine_model_3__c text NULL,
	        machine_unit_1__c float8 NULL,
	        machine_unit_2__c float8 NULL,
	        machine_unit_3__c text NULL,
	        manual_create_opportunity__c bool NULL,
	        model__c text NULL,
	        "name" text NULL,
	        nextstep text NULL,
	        next_create_opty_date__c text NULL,
	        noti_create_date__c text NULL,
	        number_of_competitor_pssr__c text NULL,
	        number_of_competitor__c text NULL,
	        opportunity_name_type__c text NULL,
	        opportunity_number__c text NULL,
	        opportunity_rate__c text NULL,
	        opportunity_source__c text NULL,
	        opportunity_suggest__c bool NULL,
	        oppty_group_pssr__c text NULL,
	        oppty_priority_pssr__c text NULL,
	        optyfrominspection__c bool NULL,
	        opty_group_pssr__c text NULL,
	        opty_loss_create_asset__c bool NULL,
	        opty_priority_pssr__c text NULL,
	        other_competitor_brand__c text NULL,
	        other_competitor_model__c text NULL,
	        other_lost_reason__c text NULL,
	        ownerid text NULL,
	        pm_contract_number__c text NULL,
	        pm_duration_breaker_end__c text NULL,
	        pm_duration_breaker_start__c text NULL,
	        pm_duration_breaker__c text NULL,
	        pm_duration_end__c text NULL,
	        pm_duration_start__c text NULL,
	        pm_duration__c text NULL,
	        pricebook2id text NULL,
	        price_build__c text NULL,
	        probability float8 NULL,
	        product_brand_1__c text NULL,
	        product_brand_2__c text NULL,
	        product_brand_3__c text NULL,
	        projects_value_baht__c text NULL,
	        project_id__c text NULL,
	        project_money__c text NULL,
	        project_name__c text NULL,
	        project_type_name__c text NULL,
	        province__c text NULL,
	        purchase_method_group_name__c text NULL,
	        purchase_method_name__c text NULL,
	        pushcount int8 NULL,
	        recordtypeid text NULL,
	        region_by_user__c text NULL,
	        roi_analysis_completed__c bool NULL,
	        sale_period_end__c text NULL,
	        sale_period_start__c text NULL,
	        serial__c text NULL,
	        service_type__c text NULL,
	        smr__c text NULL,
	        special_condition_promotion_1__c text NULL,
	        special_condition_promotion_2__c text NULL,
	        special_condition_promotion_3__c text NULL,
	        stagename text NULL,
	        sub_district__c text NULL,
	        suggested_b_mode_period__c text NULL,
	        suggest_product__c text NULL,
	        sum_price_agree__c text NULL,
	        syncedquoteid text NULL,
	        systemmodstamp text NULL,
	        term_of_payment_1__c text NULL,
	        term_of_payment_2__c text NULL,
	        term_of_payment_3__c text NULL,
	        territory2id text NULL,
	        transaction_date__c text NULL,
	        "type" text NULL,
	        winner_name__c text NULL,
	        winner_tin__c text NULL,
	        won_reason__c text NULL,
	        x1_1lost__c float8 NULL,
	        x1_1won__c float8 NULL,
	        x1_2lost__c float8 NULL,
	        x1_2won__c float8 NULL,
	        x1_3lost__c float8 NULL,
	        x1_3won__c float8 NULL,
	        x1_4lost__c float8 NULL,
	        x1_4won__c float8 NULL,
	        x1_5lost__c float8 NULL,
	        x1_5won__c float8 NULL,
	        x1_6lost__c float8 NULL,
	        x1_6won__c float8 NULL,
	        x1_8lost__c float8 NULL,
	        x1_8won__c float8 NULL,
	        x1_9lost__c float8 NULL,
	        x1_9won__c float8 NULL,
	        x1_10lost__c float8 NULL,
	        x1_10won__c float8 NULL,
	        x2_1lost__c float8 NULL,
	        x2_1won__c float8 NULL,
	        x2_2lost__c float8 NULL,
	        x2_2won__c float8 NULL,
	        x3_1lost__c float8 NULL,
	        x3_1won__c float8 NULL,
	        x3_2lost__c float8 NULL,
	        x3_2won__c float8 NULL,
	        x3_3lost__c float8 NULL,
	        x3_3won__c float8 NULL,
	        x3_4lost__c float8 NULL,
	        x3_4won__c float8 NULL,
	        x3_5lost__c float8 NULL,
	        x3_5won__c float8 NULL,
	        x3_6lost__c float8 NULL,
	        x3_6won__c float8 NULL,
	        x3_7lost__c float8 NULL,
	        x3_7won__c float8 NULL,
	        x3_8lost__c float8 NULL,
	        x3_8won__c float8 NULL,
	        x3_9lost__c float8 NULL,
	        x3_9won__c float8 NULL,
	        x4_1lost__c float8 NULL,
	        x4_1won__c float8 NULL,
	        x4_2lost__c float8 NULL,
	        x4_2won__c float8 NULL,
	        x4_3lost__c float8 NULL,
	        x4_3won__c float8 NULL,
	        x4_4lost__c float8 NULL,
	        x4_4won__c float8 NULL,
	        x5_1lost__c float8 NULL,
	        x5_1won__c float8 NULL,
	        x5_2lost__c float8 NULL,
	        x5_2won__c float8 NULL,
	        x6_1lost__c float8 NULL,
	        x6_1won__c float8 NULL,
	        x6_2lost__c float8 NULL,
	        x6_2won__c float8 NULL,
	        x6_3lost__c float8 NULL,
	        x6_3won__c float8 NULL,
	        x6_4lost__c float8 NULL,
	        x6_4won__c float8 NULL,
	        x6_5lost__c float8 NULL,
	        x6_5won__c float8 NULL,
	        x7_1lost__c float8 NULL,
	        x7_1won__c float8 NULL,
	        x7_2lost__c float8 NULL,
	        x7_2won__c float8 NULL,
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

def Append_process(**kwargs):
    
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']
    primary_key = kwargs['Key']
    c_size = kwargs['Chunk_Size']
    C_condition = kwargs['Condition']

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        strexec = ("DELETE FROM %s WHERE (EXISTS (SELECT %s FROM %s WHERE %s.%s = %s.%s));") % (tb_to, primary_key, tb_to + '_tmp',tb_to,primary_key,tb_to + '_tmp',primary_key)
        # execute
        with engine.connect() as conn:
            conn.execute(strexec)
            conn.close()
        for df_main in pd.read_sql_query(sql=sqlalchemy.text("select * from %s_tmp" % (tb_to)), con=engine, chunksize=c_size):
            rows = len(df_main)
            if (rows > 0):
                print(f"Got dataframe w/{rows} rows")
                # Load & transfrom
                ##################
                df_main.to_sql(tb_to, engine, index=False, if_exists='append')
                print(f"Save to data W/H {rows} rows")
            del df_main
            gc.collect()
        print("Append Process finished")
        
def branch_func(ti):
    xcom_value = bool(ti.xcom_pull(task_ids="check_exit_sf_opportunity_data", key='return_value'))
    if xcom_value:
        return "check_row_oppty_on_data_lake"
    else:
        return "create_new_sf_opportunity_table"

def branch_select_func(**kwargs):
    dlstrcon = common.get_pg_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(dlstrcon,client_encoding="utf8")

    tb_to = kwargs['To_Table']

    # check exiting table
    ctable = "SELECT COUNT(*) as c FROM %s;" % (tb_to + '_tmp')
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    print(result)
    row = result.loc[0,'c']
    if row < 300000:
        return "upsert_sf_opportunity_on_data_lake"
    else:
        return "append_oppty_on_data_lake"
    
with DAG(
    'Salesforce_opportunity_ETLfromS3_dag',
    tags=['Salesforce'],
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    ################### Salesforce Opportunity ######################################################################################################
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
        task_id='get_salesforce_opportunity_object',
        http_conn_id='bks_api',
        method='POST',
        endpoint='etl/sf/sfopportunity?fy=' + get_fisical_year(),
        log_response=True
    )
    t2.set_upstream(t1)

    t3 = PythonOperator(
        task_id='copy_sf_opportunity_from_s3_data_lake',
        provide_context=True,
        python_callable= common.copy_from_minio,
        op_kwargs={'To_Table': "sf_opportunity", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': "", 'Last_Days': 365}
    )
    t3.set_upstream(t2)

    t4 = PythonOperator(
        task_id='prepare_salesforce_opportunity',
        provide_context=True,
        python_callable= PP_process,
        op_kwargs={'To_Table': "sf_opportunity", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t4.set_upstream(t3)

    t5 = PythonOperator(
        task_id='etl_sf_opportunity_data_lake',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "sf_opportunity", 'Chunk_Size': 50000, 'Key': 'id', 'Condition': ""}
    )
    t5.set_upstream(t4)