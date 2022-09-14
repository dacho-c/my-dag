import time
import pandas as pd
import configparser
from sqlalchemy import create_engine, delete
#import datetime
from airflow.models import Variable

from function import get_last_ym

class common(object):

    def get_dl_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        pgdatabase = Variable.get('dl_database')
        pghost = Variable.get('db_host')
        pgport = Variable.get('pgsl2_port')
        pguid = Variable.get('uid')
        pgpwd = Variable.get('pwd')
        # Connection String to Postgres DATA Lake
        pgstrcon = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
        return pgstrcon

    def get_wh_connection(self):
        config = configparser.ConfigParser()
        config.read(Variable.get('db2pg_config'))
        pgdatabase = Variable.get('wh_database')
        pghost = Variable.get('db_host')
        pgport = Variable.get('pg_port')
        pguid = Variable.get('uid')
        pgpwd = Variable.get('pwd')
        # Connection String to Postgres DATA Warehouse
        pgstrcon = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (pguid,pgpwd,pghost,pgport,pgdatabase)
        return pgstrcon