from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

from utils.exportcsv import export_to_csv
from utils.exportcsv import extract_weather_data
from utils.exportcsv import insert_weather_data
from utils.exportcsv import create_table_sql
from airflow.models import Variable


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hisotrical_weather_API_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract weather data',
    schedule_interval='@daily',
    catchup=False,
    params={
        "start_date": "2025-01-14",
        "end_date":"2025-01-31"
    },
)

extract_task = PythonOperator(
    task_id='extract_weather_data_task',
    python_callable=extract_weather_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='historical_weather_data',
    postgres_conn_id=Variable.get("postgres_conn_id"),
    sql=create_table_sql,
    autocommit=True,
    dag=dag,
)
 
insert_task = PythonOperator(
     task_id='insert_weather_data',
     python_callable =insert_weather_data,
     dag=dag,
)


export_task = PythonOperator(
    task_id='export_to_csv',
    python_callable=export_to_csv,
    op_args=['historical_weatherdata', '/opt/airflow/dags/csv/historical_data.csv'],
    dag=dag,
)

# Task dependencies
extract_task >> create_table_task >> insert_task >> export_task