import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime
from airflow.models import Variable

def export_to_csv(table_name, export_path, conn_id=Variable.get("postgres_conn_id")):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = postgres_hook.get_conn()

    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    df.to_csv(export_path, index=False)
    print(f"Data exported to {export_path}")


def extract_weather_data(**kwargs):
    start_date = kwargs['params']['start_date']
    end_date = kwargs['params']['end_date']
    api_key = "QVCED9KWZTVFG3A2LHBSJYKUF"
    address = "BANGALORE"
    
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{address}/{start_date}/{end_date}?unitGroup=metric&include=days&key={api_key}&contentType=json"
    response = requests.get(url, timeout=10)
    data = response.json()

    weather_data = []
    address = data['address']
    for d in data['days']:
        weather_data.append({
            'city': address,
            'Date': d['datetime'],
            'max_temperature': d['tempmax'],
            'min_temperature': d['tempmin'],
            'pressure': d['pressure'],
            'timestamp': datetime.now()
        })
    return weather_data

def insert_weather_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id= Variable.get("postgres_conn_id"))
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
