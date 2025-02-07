from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow.models import Variable
import sys,os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from covid19_utils import extract_covid19_data,dataframe_process,create_table_sql,insert_data_into_postgres,export_to_csv

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'covid19_data',
    default_args=default_args,
    description='pipeline to fetch hospital bed details',
    schedule_interval='@daily',
    catchup=False,
)

          
extract_task = PythonOperator(
    task_id='extract_covid_data_task',
    python_callable=extract_covid19_data,
    dag=dag,
)    

dataframe_process_task=PythonOperator(
    task_id='dataframe_processing_task',
    python_callable=dataframe_process,
    dag=dag


)
create_table_task = PostgresOperator(
    task_id='creating_table_covid_data_task',
    postgres_conn_id=Variable.get("postgres_conn_id"),
    sql=create_table_sql,
    autocommit=True,
    dag=dag,
)

insert_table_task = PythonOperator(
    task_id ='inserting_dataframe_task',
    python_callable=insert_data_into_postgres,
    dag=dag,
    
)
export_to_csv_task = PythonOperator(
    task_id='export_to_csv_task',
    python_callable=export_to_csv,
    dag=dag,
)


extract_task>>dataframe_process_task>>create_table_task>>insert_table_task>>export_to_csv_task
