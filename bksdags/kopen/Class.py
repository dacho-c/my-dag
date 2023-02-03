import os
import sys
# import the connect library for psycopg2
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
import numpy as np
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt

import time
import pandas as pd
import pyarrow.parquet as pq
#import pyarrow as pa
import configparser
from sqlalchemy import create_engine, delete
#import datetime
from airflow.models import Variable
import minio
import gc
import os, glob
from sql import sql_detail_select, sql_detail_delete
from function import get_last_ym

class common(object):

    def get_pg_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        pgdatabase = Variable.get('dl_database')
        pghost = Variable.get('db_host')
        pgport = Variable.get('pg_port')
        pguid = Variable.get('uid')
        pgpwd = Variable.get('pwd')
        # Connection String to Postgres DATA Lake
        pgstrcon = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
        return pgstrcon

    def get_db2_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        db2database = Variable.get('db2_database')
        db2host = Variable.get('db_host')
        db2port = Variable.get('db2_port')
        db2uid = Variable.get('db2uid')
        db2pwd = Variable.get('db2pwd')
        # Connection String to EGKopen db2
        db2strcon = "db2://%s:%s@%s:%s/%s" % (db2uid, db2pwd, db2host, db2port, db2database)
        return db2strcon

    def copy_to_minio(**kwargs):
        tb_to = kwargs['To_Table']
        targetfile = tb_to + '.parquet'
        s3_endpoint = Variable.get('s3_endpoint')
        s3_access_key = Variable.get('s3_access_key')
        s3_secret_key = Variable.get('s3_secret_key')
        # Create the client
        client = minio.Minio(endpoint=s3_endpoint,access_key=s3_access_key,secret_key=s3_secret_key,secure=False)
        # Put the object into minio
        client.fput_object("datalake",targetfile,targetfile )
        return True

    def copy_from_minio(**kwargs):
        tb_to = kwargs['To_Table']
        targetfile = tb_to + '.parquet'
        s3_endpoint = Variable.get('s3_endpoint')
        s3_access_key = Variable.get('s3_access_key')
        s3_secret_key = Variable.get('s3_secret_key')
        # Create the client
        client = minio.Minio(endpoint=s3_endpoint,access_key=s3_access_key,secret_key=s3_secret_key,secure=False)
        # Put the object into minio
        client.fget_object("datalake",targetfile,targetfile )
        return True

    def Del_File(**kwargs):
        tables = kwargs['To_Table']
        if os.path.exists(tables + ".parquet"):
            os.remove(tables + '.parquet')
        if os.path.exists(tables):
            filelist = glob.glob(os.path.join(tables, "*"))
            for f in filelist:
                os.remove(f)

    def combine_parquet_files(**kwargs):
        input_folder = kwargs['To_Table']
        target_path = input_folder + ".parquet"
        try:
            files = []
            for file_name in os.listdir(input_folder):
                files.append(pq.read_table(os.path.join(input_folder, file_name)))
            with pq.ParquetWriter(target_path,
                files[0].schema,
                version='2.6',
                compression='gzip',
                use_dictionary=True,
                data_page_size=2097152, #2MB
                write_statistics=True) as writer:
                for f in files:
                    writer.write_table(f)
        except Exception as e:
            print(e)

    # Define a function that handles and parses psycopg2 exceptions
    def show_psycopg2_exception(err):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()    
        # get the line number when exception occured
        line_n = traceback.tb_lineno    
        # print the connect() error
        print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
        # psycopg2 extensions.Diagnostics object attribute
        print ("\nextensions.Diagnostics:", err.diag)    
        # print the pgcode and pgerror exceptions
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")

    # Define function using copy_from_dataFile to insert the dataframe.
    def copy_from_dataFile(df, table):
        #  Here we are going save the dataframe on disk as a csv file, load # the csv file and use copy_from() to copy it to the table
        conn_string = common.get_pg_connection('')
        conn = psycopg2.connect(conn_string)

        tmp_df = table + '_temp.csv'
        df.to_csv(tmp_df, header=False,index = False)
        f = open(tmp_df, 'r')
        cursor = conn.cursor()
        try:
            cursor.copy_from(f, table, sep=",")
            print("Data inserted using copy_from_datafile() successfully....")
        except (Exception, psycopg2.DatabaseError) as err:
            os.remove(tmp_df)
            # pass exception to function
            common.show_psycopg2_exception(err)
        cursor.close()
        conn.close()

    # Define function using copy_from() with StringIO to insert the dataframe
    def copy_from_dataFile_StringIO(conn, datafrm, table):
    
        # save dataframe to an in memory buffer
        buffer = StringIO()
        datafrm.to_csv(buffer, header=False, index = False)
        buffer.seek(0)
    
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep=",")
            print("Data inserted using copy_from_datafile_StringIO() successfully....")
        except (Exception, psycopg2.DatabaseError) as err:
            # pass exception to function
            common.show_psycopg2_exception(err)
        cursor.close()

    def read_load_save_data(**kwargs): 
        try:
            db2strcon = common.get_db2_connection('')
            # Create SQLAlchemy engine
            engine_db2 = create_engine(db2strcon)
            conn_db2 = engine_db2.connect().execution_options(stream_results=True)
        except:
            time.sleep(5)
            print("DB2 Connect Error Sleep 5s")
            db2strcon = common.get_db2_connection('')
            # Create SQLAlchemy engine
            engine_db2 = create_engine(db2strcon)
            conn_db2 = engine_db2.connect().execution_options(stream_results=True)
        
        pgstrcon = common.get_pg_connection('')
        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")

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
            del chunk_df
            gc.collect()
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
        try:
            db2strcon = common.get_db2_connection('')
            # Create SQLAlchemy engine
            engine_db2 = create_engine(db2strcon)
            conn_db2 = engine_db2.connect().execution_options(stream_results=True)
        except:
            time.sleep(5)
            print("DB2 Connect Error Sleep 5s")
            db2strcon = common.get_db2_connection('')
            # Create SQLAlchemy engine
            engine_db2 = create_engine(db2strcon)
            conn_db2 = engine_db2.connect().execution_options(stream_results=True)

        pgstrcon = common.get_pg_connection('')
        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")

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
            del chunk_df
            gc.collect()
        print("EL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True

    def read_load_update_detail_data(**kwargs): 
        try:
            db2strcon = common.get_db2_connection('')
            # Create SQLAlchemy engine
            engine_db2 = create_engine(db2strcon)
            conn_db2 = engine_db2.connect().execution_options(stream_results=True)
        except:
            time.sleep(5)
            print("DB2 Connect Error Sleep 5s")
            db2strcon = common.get_db2_connection('')
            # Create SQLAlchemy engine
            engine_db2 = create_engine(db2strcon)
            conn_db2 = engine_db2.connect().execution_options(stream_results=True)

        pgstrcon = common.get_pg_connection('')
        # Create SQLAlchemy engine
        engine_pg = create_engine(pgstrcon,client_encoding="utf8")

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
            del chunk_df
            gc.collect()
        print("EL Process finished")
        print(f"Time to process {tb_from} : {time.time() - start_time} Sec.")
        return True