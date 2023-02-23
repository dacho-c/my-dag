import os
import sys
# import the connect library for psycopg2
import psycopg2
# import the error handling libraries for psycopg2
#from psycopg2 import OperationalError, errorcodes, errors
#import psycopg2.extras as extras
from io import StringIO

import time
import pandas as pd
import pyarrow.parquet as pq
#import pyarrow as pa
import configparser
from sqlalchemy import create_engine, delete
#import datetime
from airflow.models import Variable
import minio
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import gc
import os, glob
from function import get_today, get_lastday, get_fisical_year

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
    
    def get_mailto(self):
        return Variable.get('mailto')
    
    def send_mail(**kwargs):
        TYPE = kwargs['mtype'] 
        FROM = Variable.get('mailfrom') 
        if TYPE == 'err':
            TO = Variable.get('mailto_err')
        else:
            TO = Variable.get('mailto')
        
        SUBJECT = kwargs['msubject']
        TEXT = kwargs['text']
    
        # Prepare actual message
        msg = MIMEText(TEXT)
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = TO
        smtp_server = smtplib.SMTP_SSL(Variable.get('mailhost'), Variable.get('mailport'))
        smtp_server.login(FROM, Variable.get('mail_secret'))
        smtp_server.sendmail(FROM, TO, msg.as_string())
        smtp_server.quit()
        # Send the mail
        return True

    def copy_to_minio(**kwargs):
        tb_to = kwargs['To_Table']
        ld = kwargs['Last_Days']
        targetfile = tb_to + '.parquet'
        s3_endpoint = Variable.get('s3_endpoint')
        s3_access_key = Variable.get('s3_access_key')
        s3_secret_key = Variable.get('s3_secret_key')
        # Create the client
        client = minio.Minio(endpoint=s3_endpoint,access_key=s3_access_key,secret_key=s3_secret_key,secure=False)
        # Put the object into minio
        client.fput_object("datalake",get_today() + '-' + targetfile,'/opt/airflow/' + targetfile)
        client.remove_object("datalake",get_lastday(ld) + '-' + targetfile)
        return True

    def copy_from_minio(**kwargs):
        tb_to = kwargs['To_Table']
        ld = kwargs['Last_Days']
        targetfile = tb_to + '.parquet'
        s3_endpoint = Variable.get('s3_endpoint')
        s3_access_key = Variable.get('s3_access_key')
        s3_secret_key = Variable.get('s3_secret_key')
        # Create the client
        client = minio.Minio(endpoint=s3_endpoint,access_key=s3_access_key,secret_key=s3_secret_key,secure=False)
        # Put the object into minio
        if ld == 365:
            client.fget_object("datalake",get_fisical_year() + '-' + targetfile,'/opt/airflow/' + targetfile )
        else:
            client.fget_object("datalake",get_lastday(ld) + '-' + targetfile,'/opt/airflow/' + targetfile )
        return True
    
    def copy_to_minio_sl(**kwargs):
        tb_to = kwargs['To_Table']
        ld = kwargs['Last_Days']
        targetfile = tb_to + '.parquet'
        s3_endpoint = Variable.get('s3_endpoint_bp')
        s3_access_key = Variable.get('s3_access_key')
        s3_secret_key = Variable.get('s3_secret_key')
        # Create the client
        client = minio.Minio(endpoint=s3_endpoint,access_key=s3_access_key,secret_key=s3_secret_key,secure=False)
        # Put the object into minio
        client.fput_object("datalake",get_today() + '-' + targetfile,'/opt/airflow/' + targetfile)
        client.remove_object("datalake",get_lastday(ld) + '-' + targetfile)
        return True

    def copy_from_minio_sl(**kwargs):
        tb_to = kwargs['To_Table']
        ld = kwargs['Last_Days']
        targetfile = tb_to + '.parquet'
        s3_endpoint = Variable.get('s3_endpoint_bp')
        s3_access_key = Variable.get('s3_access_key')
        s3_secret_key = Variable.get('s3_secret_key')
        # Create the client
        client = minio.Minio(endpoint=s3_endpoint,access_key=s3_access_key,secret_key=s3_secret_key,secure=False)
        # Put the object into minio
        if ld == 365:
            client.fget_object("datalake",get_fisical_year() + '-' + targetfile,'/opt/airflow/' + targetfile )
        else:
            client.fget_object("datalake",get_lastday(ld) + '-' + targetfile,'/opt/airflow/' + targetfile )
        return True

    def Del_File(**kwargs):
        tables = kwargs['To_Table']
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
        conn_string = conn_string.replace('+psycopg2','')
        conn = psycopg2.connect(conn_string)

        tmp_df = table + '_tmp.csv'
        df.to_csv(tmp_df, header=False,index = False)
        f = open(tmp_df, 'r')
        cursor = conn.cursor()
        try:
            cursor.copy_from(f, table, sep=",")
            print("Data inserted using copy_from_datafile() successfully....")
        except (Exception, psycopg2.DatabaseError) as err:
            # pass exception to function
            common.show_psycopg2_exception(err)
        conn.commit()
        cursor.close()
        conn.close()
        os.remove(tmp_df)
        return True

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

