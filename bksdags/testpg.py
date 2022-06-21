import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
#from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import csv

def get_iris_data():
    sql_stmt = "SELECT * FROM iris"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='dblake'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

def save_iris_data():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='dblake'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    with open(Variable.get('tmp_iris_csv_location'), 'r') as f:
        # Notice that we don't need the `csv` module.
        next(f) # Skip the header row.
        cursor.copy_from(f, 'iris_tgt', sep=',')

    pg_conn.commit()
    return True

def process_iris_data(ti):
    iris = ti.xcom_pull(task_ids=['get_iris_data'])
    if not iris:
        raise Exception('No data.')

    iris = pd.DataFrame(
        data=iris[0],
        columns=['id', 'w', 'l', 'd']
    )
    iris = iris[
        (iris['l'] > 5)
    ]
    #iris = iris.drop('id', axis=1)
    #iris.to_csv(Variable.get('tmp_iris_csv_location'), index=False)
    with open(Variable.get('tmp_iris_csv_location'), 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = '|')
            a.writerows(iris)


with DAG(
    dag_id='postgres_db_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
) as dag:

    # 1. Get the Iris data from a table in Postgres
    task_get_iris_data = PythonOperator(
        task_id='get_iris_data',
        python_callable=get_iris_data,
        do_xcom_push=True
    )

    # 2. Process the Iris data
    task_process_iris_data = PythonOperator(
        task_id='process_iris_data',
        python_callable=process_iris_data
    )

    # 3. Truncate table in Postgres
    task_truncate_table = PostgresOperator(
        task_id='truncate_tgt_table',
        postgres_conn_id='postgres',
        sql="TRUNCATE TABLE iris_tgt"
    )

    # 4. Save to Postgres
    task_load_iris_data = PythonOperator(
        task_id='load_iris_data',
        python_callable=save_iris_data
    )

    #task_load_iris_data = BashOperator(
        #task_id='load_iris_data',
        #bash_command=(
            #'psql -d dblake -U postgres -c "'
            #'COPY iris_tgt(iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety) '
            #"FROM '/opt/airflow/dags/test.csv' "
            #"DELIMITER ',' "
            #'CSV HEADER"'
        #)
    #)
    
    task_get_iris_data >> task_process_iris_data >> task_truncate_table >> task_load_iris_data
