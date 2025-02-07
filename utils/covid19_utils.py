import requests
import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


#extracting covid19 data from the api that contains information about hospitals and beds for different regions
def extract_covid19_data(**kwargs):
    url = Variable.get("covid_api_url")
    response = requests.get(url, timeout=10)
    data = response.json()
    covid_data=data['data']["regional"]
    kwargs['ti'].xcom_push(key='raw_covid_data', value=covid_data)


#converting the data from api into a dataframe and performing transformations 
def dataframe_process(**kwargs):
    ti = kwargs['ti']
    covid_data = ti.xcom_pull(task_ids='extract_covid_data_task',key='raw_covid_data')
    df=pd.DataFrame(covid_data)
    df['ruralHospitalToBedRatio'] = df['ruralHospitals'] / df['ruralBeds']
    df['urbanHospitalToBedRatio'] = df['urbanHospitals'] / df['urbanBeds']
    df['totalHospitalToBedRatio'] = df['totalHospitals'] / df['totalBeds'] 
    df["asOn"] = pd.to_datetime(df["asOn"]).dt.date
    df["moreThan1000Hospitals"] = df["totalHospitals"].apply(lambda x: "Yes" if x > 1000 else "No")

    df.to_csv('/opt/airflow/dags/csv/covid.csv',index=False)
    ti.xcom_push(key='processed_covid_data', value=df.to_dict(orient='records'))


#creating a table to store the transformed data
table_name=Variable.get("covid_table")
create_table_sql = f"""    
CREATE TABLE IF NOT EXISTS {table_name} (
    state VARCHAR(255),
    ruralHospitals INT,
    ruralBeds INT,
    urbanHospitals INT,
    urbanBeds INT,
    totalHospitals INT,
    totalBeds INT,
    asOn DATE,
    ruralHospitalToBedRatio FLOAT,
    urbanHospitalToBedRatio FLOAT,
    totalHospitalToBedRatio FLOAT,
    moreThan1000Hospitals VARCHAR(3)  -- 'Yes' or 'No'
);
"""


#inserting data from dataframe into the table created
def insert_data_into_postgres(**kwargs):
    ti = kwargs['ti']
    covid_data = ti.xcom_pull(key='processed_covid_data',task_ids='dataframe_processing_task')
    df = pd.DataFrame(covid_data)
    table_name=Variable.get("covid_table")
    pg_hook = PostgresHook(postgres_conn_id=Variable.get("postgres_conn_id"))
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for _, row in df.iterrows():
        insert_query =f"""
            INSERT INTO {table_name} (state, ruralHospitals, ruralBeds, urbanHospitals, urbanBeds, totalHospitals, 
                                    totalBeds, asOn, ruralHospitalToBedRatio, urbanHospitalToBedRatio, 
                                    totalHospitalToBedRatio, moreThan1000Hospitals)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            row['state'], row['ruralHospitals'], row['ruralBeds'], row['urbanHospitals'], row['urbanBeds'], 
            row['totalHospitals'], row['totalBeds'], row['asOn'], row['ruralHospitalToBedRatio'], 
            row['urbanHospitalToBedRatio'], row['totalHospitalToBedRatio'], row['moreThan1000Hospitals']
        ))
    
    connection.commit()
    cursor.close()
    connection.close()


#exporting data from table to csv
def export_to_csv():
    postgres_hook = PostgresHook(postgres_conn_id=Variable.get("postgres_conn_id"))
    conn = postgres_hook.get_conn()
    table_name=Variable.get("covid_table")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    export_path=Variable.get("covid_csv_path")
    df.to_csv(export_path, index=False)
    print(f"Data exported to {export_path}")
