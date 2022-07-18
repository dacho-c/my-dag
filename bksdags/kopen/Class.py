import time
import pandas as pd
import configparser
from sqlalchemy import create_engine, delete
#import datetime
from airflow.models import Variable

from sql import sql_detail_select, sql_detail_delete
from function import get_last_ym

class common(object):

    def get_pg_connection(self):
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

    def get_db2_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        db2database = config['DB2']['database']
        db2host = config['DB2']['host']
        db2port = config['DB2']['port']
        db2protocal = config['DB2']['protocal']
        db2uid = config['DB2']['uid']
        db2pwd = config['DB2']['pwd']
        # Connection String to EGKopen db2
        db2strcon = "db2://%s:%s@%s:%s/%s" % (db2uid, db2pwd, db2host, db2port, db2database)
        return db2strcon

    def read_load_save_data(**kwargs): 

        db2strcon = common.get_db2_connection('')

        pgstrcon = common.get_pg_connection('')

        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")
        engine_db2 = create_engine(db2strcon)
        conn_db2 = engine_db2.connect().execution_options(stream_results=True)

        tb_from = kwargs['From_Table']
        tb_to = kwargs['To_Table']
        c_size = kwargs['Chunk_Size']

        start_time = time.time()
        n = 0
        rows = 0
        sqlstr = "SELECT * FROM db2admin." + tb_from

        for chunk_df in pd.read_sql(sqlstr, conn_db2, chunksize=c_size):
            rows += len(chunk_df)
            print(f"Got dataframe {rows}/All rows")
            # Load to DB-LAKE not transfrom
            if n == 0:
                chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='replace')
                n = n + 1
            else:
                chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
                print(f"Already Save to data lake {rows} rows")
        print("EL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True

    def delete_before_append(**kwargs):

        pgstrcon = common.get_pg_connection('')

        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")

        pgtb = kwargs['To_Table']
        pgcondition = kwargs['Condition']

        sqlstr = "DELETE FROM %s WHERE %s" % (pgtb, pgcondition)
        print(sqlstr)
        engine_pg.execute(sqlstr)
        return True

    def delete_before_append_detail(**kwargs):

        pgstrcon = common.get_pg_connection('')

        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")

        pgtb = kwargs['To_Table']

        sqlstr = sql_detail_delete(pgtb, get_last_ym())
        print(sqlstr)
        engine_pg.execute(sqlstr)
        return True

    def read_load_update_data(**kwargs): 

        db2strcon = common.get_db2_connection('')

        pgstrcon = common.get_pg_connection('')

        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")
        engine_db2 = create_engine(db2strcon)
        conn_db2 = engine_db2.connect().execution_options(stream_results=True)

        tb_from = kwargs['From_Table']
        condition = kwargs['Condition']
        tb_to = kwargs['To_Table']
        c_size = kwargs['Chunk_Size']

        start_time = time.time()
        n = 0
        rows = 0
        sqlstr = "SELECT * FROM db2admin.%s WHERE %s" % (tb_from, condition)
        print(sqlstr)

        for chunk_df in pd.read_sql(sqlstr, conn_db2, chunksize=c_size):
            rows += len(chunk_df)
            print(f"Got dataframe {rows}/All rows")
            # Load to DB-LAKE not transfrom
            if n == 0:
                common.delete_before_append(**kwargs)
                chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
                print(f"Already Update to data lake {rows} rows")
                n = n + 1
            else:
                chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
                print(f"Already Update to data lake {rows} rows")
        print("EL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True

    def read_load_update_detail_data(**kwargs): 

        db2strcon = common.get_db2_connection('')

        pgstrcon = common.get_pg_connection('')

        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")
        engine_db2 = create_engine(db2strcon)
        conn_db2 = engine_db2.connect().execution_options(stream_results=True)

        tb_from = kwargs['From_Table']
        tb_to = kwargs['To_Table']
        c_size = kwargs['Chunk_Size']

        start_time = time.time()
        n = 0
        rows = 0
        sqlstr = sql_detail_select(tb_from, get_last_ym())
        print(sqlstr)

        for chunk_df in pd.read_sql(sqlstr, conn_db2, chunksize=c_size):
            rows += len(chunk_df)
            print(f"Got dataframe {rows}/All rows")
            # Load to DB-LAKE not transfrom
            if n == 0:
                common.delete_before_append_detail(**kwargs)
                chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
                print(f"Already Update to data lake {rows} rows")
                n = n + 1
            else:
                chunk_df.to_sql(tb_to, engine_pg, index=False, if_exists='append')
                print(f"Already Update to data lake {rows} rows")
        print("EL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True