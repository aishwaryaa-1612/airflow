from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

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

# Task 1: Extract weather data from the API
def extract_weather_data(**kwargs):
    start_date=kwargs['params']['start_date']
    end_date=kwargs['params']['end_date']
    api_key = "QVCED9KWZTVFG3A2LHBSJYKUF"
    address = "BANGALORE"
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{address}/{start_date}/{end_date}?unitGroup=metric&include=days&key={api_key}&contentType=json"
    response = requests.get(url, timeout=10)
    data = response.json()


    weather_data =[]
    address=data['address']
    for d in data['days']:
       weather_data.append( {
        'city':address,
        'Date': d['datetime'],
        'max_temperature': d['tempmax'],
        'min_temperature': d['tempmin'],
        'pressure': d['pressure'],
        'timestamp': datetime.now()
   })
    print (weather_data)
    return weather_data



# Task 2: Create the weather_data table in Postgres
create_table_sql = """
CREATE TABLE IF NOT EXISTS historical_weather_data1 (
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

# Task 3: Insert weather data into the table
def insert_weather_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='source_database')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        weather_data = kwargs['task_instance'].xcom_pull(task_ids='extract_weather_data_task')
        for day in weather_data:
            cursor.execute("""
                INSERT INTO historical_weather_data1 (city,date,max_temperature,min_temperature,pressure,timestamp )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (day['city'],day['Date'], day['max_temperature'], day['min_temperature'], day['pressure'], day['timestamp']))
        connection.commit()
        cursor.close()
        connection.close()

# Task 4: Export data from Postgres to CSV
def export_to_csv():
    postgres_hook = PostgresHook(postgres_conn_id='source_database')
    conn = postgres_hook.get_conn()

    df = pd.read_sql_query("SELECT * FROM historical_weather_data1", conn)
    export_path = '/opt/airflow/dags/csv/exported_historical_data1.csv'
    df.to_csv(export_path, index=False)
    print(f"Data exported to {export_path}")


extract_task = PythonOperator(
    task_id='extract_weather_data_task',
    python_callable=extract_weather_data,
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
    dag=dag,
)

# Task dependencies
extract_task >> create_table_task >> insert_task >> export_task