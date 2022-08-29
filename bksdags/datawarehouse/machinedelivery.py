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
    #conn_dl = engine_dl.connect().execution_options(stream_results=True)

    sqlstr_main = sql_ET_machine_cust()
    sqlstr_mc = sql_ET_machine()
    sqlstr_inv = sql_ET_invoice()

    df_mc = pd.read_sql_query(sqlstr_mc, con=engine_dl)
    df_inv = pd.read_sql_query(sqlstr_inv, con=engine_dl)

    n = 0
    rows = 0
    tb_to = kwargs['To_Table'] + "_tmp"
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

def upsert(engine, schema, table_name, records=[]):

    from sqlalchemy import MetaData, Table
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.inspection import inspect


    metadata = MetaData(schema=schema)
    metadata.bind = engine

    table = Table(table_name, metadata, schema=schema, autoload=True)

    # get list of fields making up primary key
    primary_keys = [key.name for key in inspect(table).primary_key]

    # assemble base statement
    stmt = postgresql.insert(table).values(records)

    # define dict of non-primary keys for updating
    update_dict = {
        c.name: c
        for c in stmt.excluded
        if not c.primary_key
    }

    # cover case when all columns in table comprise a primary key
    # in which case, upsert is identical to 'on conflict do nothing.
    if update_dict == {}:
        warnings.warn('no updateable columns found for table')
        # we still wanna insert without errors
        insert_ignore(table_name, records)
        return None
    print(update_dict)
    # assemble new statement with 'on conflict do update' clause
    update_stmt = stmt.on_conflict_do_update(
        index_elements=primary_keys,
        set_=update_dict,
    )
    print(update_stmt)
    # execute
    with engine.connect() as conn:
        result = conn.execute(update_stmt)
        return result

def UPSERT_process(**kwargs):
    import pandas as pd
    import sqlalchemy
    
    whstrcon = common.get_wh_connection('')
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(whstrcon,client_encoding="utf8")

    n = 0
    rows = 0
    tb_to = kwargs['To_Table']
    strexec = ""

    # check exiting table
    ctable = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s');" % (tb_to)
    result = pd.read_sql_query(sql=sqlalchemy.text(ctable), con=engine)
    if result.loc[0,'exists']:
        tmptable = "SELECT * FROM %s limit 10" % (tb_to + '_tmp')
        df = pd.read_sql_query(sql=sqlalchemy.text(tmptable), con=engine)
        rows += len(df)
        print(f"Got dataframe to Upsert {rows} rows")
        # Load & transfrom
        #df = df.reset_index()
        for row in df.iterrows():
            n += 1
            print(row)
            print(f"Process {n}/{rows} rows")
            rs = upsert(engine,'public',tb_to,row)
            print(rs)
    #else:
        #strexec += ("ALTER TABLE IF EXISTS %s RENAME TO %s;" % (tb_to + '_tmp', tb_to))
        #strexec += ("ALTER TABLE %s ADD PRIMARY KEY (item_id);" % (tb_to))
    #strexec += ("DROP TABLE IF EXISTS %s;" % (tb_to + '_tmp'))
    # execute
    #with engine.connect() as conn:
        #conn.execute(strexec)
    print("ETL WH Process finished")

with DAG(
    dag_id='DWH_ETL_Machine_Delivery_dag',
    schedule_interval='30 7-20/1 * * *',
    #start_date=datetime(year=2022, month=6, day=1),
    start_date=pendulum.datetime(2022, 6, 1, tz="Asia/Bangkok"),
    catchup=False
) as dag:

    # 1. Generate Machine Delivery from a DATA LAKE To DATA Warehouse
    task_ET_WH_MachineDelivery = PythonOperator(
        task_id='et_machinedelivery_data',
        provide_context=True,
        python_callable= ETL_process,
        op_kwargs={'To_Table': "machine_delivery", 'Chunk_Size': 2000}
    )

    # 2. Upsert Machine Delivery To DATA Warehouse
    task_L_WH_MachineDelivery = PythonOperator(
        task_id='l_machinedelivery_data',
        provide_context=True,
        python_callable= UPSERT_process,
        op_kwargs={'To_Table': "machine_delivery"}
    )

    task_ET_WH_MachineDelivery >> task_L_WH_MachineDelivery