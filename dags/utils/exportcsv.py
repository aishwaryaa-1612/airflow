import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime

def export_to_csv(table_name, export_path, conn_id='source_database'):
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
