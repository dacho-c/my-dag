import time
import pandas as pd
import configparser
from sqlalchemy import create_engine, delete
#import datetime
from airflow.models import Variable

from pgsql import sql_detail_select, sql_detail_delete
from function import get_last_ym

class common(object):

    def get_dl_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        pgdatabase = config['PG']['database']
        pghost = config['PG']['host']
        pgport = config['PG']['port']
        pguid = config['PG']['uid']
        pgpwd = config['PG']['pwd']
        # Connection String to Postgres DATA Lake
        pgstrcon = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
        return pgstrcon

    def get_wh_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        pgdatabase = Variable.get('wh_database')
        pghost = Variable.get('wh_host')
        pgport = Variable.get('wh_port')
        pguid = config['PG']['uid']
        pgpwd = config['PG']['pwd']
        # Connection String to Postgres DATA Warehouse
        pgstrcon = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
        return pgstrcon

    def read_load_save_data(**kwargs): 

        whstrcon = common.get_wh_connection('')

        dlstrcon = common.get_dl_connection('')

        # Create SQLAlchemy engine
        engine_dl = create_engine(dlstrcon,client_encoding="utf8")
        engine_wh = create_engine(whstrcon,client_encoding="utf8")
        conn_dl = engine_dl.connect().execution_options(stream_results=True)

        tb_from = kwargs['From_Table']
        tb_to = kwargs['To_Table']
        c_size = kwargs['Chunk_Size']

        start_time = time.time()
        n = 0
        rows = 0
        sqlstr = "SELECT * FROM " + tb_from #########

        for chunk_df in pd.read_sql(sqlstr, conn_dl, chunksize=c_size):
            rows += len(chunk_df)
            print(f"Extract dataframe {rows}/All rows")
            # Load to DB-LAKE not transfrom
            if n == 0:
                chunk_df.to_sql(tb_to, engine_wh, index=False, if_exists='replace')
                n = n + 1
            else:
                chunk_df.to_sql(tb_to, engine_wh, index=False, if_exists='append')
                print(f"Already transfrom to data lake {rows} rows")
        print("ETL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True

    def delete_before_append(**kwargs):

        whstrcon = common.get_wh_connection('')

        # Create SQLAlchemy engine
        engine_wh = create_engine(whstrcon,client_encoding="utf8")

        pgtb = kwargs['To_Table']
        pgcondition = kwargs['Condition']

        sqlstr = "DELETE FROM %s WHERE %s" % (pgtb, pgcondition) #############
        print(sqlstr)
        engine_wh.execute(sqlstr)
        return True

    def read_load_update_data(**kwargs): 

        dlstrcon = common.get_dl_connection('')

        whstrcon = common.get_wh_connection('')

        # Create SQLAlchemy engine
        engine_wh = create_engine(whstrcon,client_encoding="utf8")
        engine_dl = create_engine(dlstrcon,client_encoding="utf8")
        conn_dl = engine_dl.connect().execution_options(stream_results=True)

        tb_from = kwargs['From_Table']
        condition = kwargs['Condition']
        tb_to = kwargs['To_Table']
        c_size = kwargs['Chunk_Size']

        start_time = time.time()
        n = 0
        rows = 0
        sqlstr = "SELECT * FROM %s WHERE %s" % (tb_from, condition) ##################
        print(sqlstr)

        for chunk_df in pd.read_sql(sqlstr, conn_dl, chunksize=c_size):
            rows += len(chunk_df)
            print(f"Extract dataframe {rows}/All rows")
            # Load to DB-LAKE not transfrom
            if n == 0:
                common.delete_before_append(**kwargs)
                chunk_df.to_sql(tb_to, engine_wh, index=False, if_exists='append')
                print(f"Already transfrom to data lake {rows} rows")
                n = n + 1
            else:
                chunk_df.to_sql(tb_to, engine_wh, index=False, if_exists='append')
                print(f"Already transfrom to data lake {rows} rows")
        print("ETL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True