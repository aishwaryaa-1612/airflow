from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

from utils.exportcsv import export_to_csv
from utils.exportcsv import extract_weather_data

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
    op_kwargs={'params': {'start_date': '2025-01-01', 'end_date': '2025-01-31'}},
    dag=dag,
)

# Create table inside postgres
create_table_sql = """
CREATE TABLE IF NOT EXISTS historical_weatherdata (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    date TIMESTAMP,
    max_temperature FLOAT,
    min_temperature FLOAT,
    pressure INT,
    timestamp TIMESTAMP
);
"""
create_table_task = PostgresOperator(
    task_id='historical_weather_data',
    postgres_conn_id='source_database',
    sql=create_table_sql,
    autocommit=True,
    dag=dag,
)
 

# Insert data from Api into the table
def insert_weather_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='source_database')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        weather_data = kwargs['task_instance'].xcom_pull(task_ids='extract_weather_data_task')
        for day in weather_data:
            cursor.execute("""
                INSERT INTO historical_weatherdata (city,date,max_temperature,min_temperature,pressure,timestamp )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (day['city'],day['Date'], day['max_temperature'], day['min_temperature'], day['pressure'], day['timestamp']))
        connection.commit()
        cursor.close()
        connection.close()

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