from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os,sys
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.empdetailsutils import check_file_format, validate_data, clean_data, load_data

FILE_PATH = Variable.get("emp_details_path")
# DAG definition
with DAG(
    dag_id="Employee_data_validation",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    wait_for_csv_task = FileSensor(
    task_id="wait_for_csv",
    filepath=FILE_PATH,
    fs_conn_id="fs_csv_files",
    poke_interval=10,
    timeout=300,
    mode="poke",
    dag=dag,
)

    check_format = PythonOperator(
        task_id="check_file_format",
        python_callable=check_file_format,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        retries=0,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

wait_for_csv_task >> check_format >> validate >> clean >> load
