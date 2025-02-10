import pandas as pd
import os
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


FILE_PATH = Variable.get("emp_details_path")

#Tasl to check for only .csv files in the folder
def check_file_format(**kwargs):
    files = os.listdir(FILE_PATH)
    if not files:
        raise ValueError(f"No files found in directory: {FILE_PATH}")

    csv_files = [file for file in files if file.endswith(".csv")]

    if not csv_files:
        raise ValueError("No CSV files found. Only CSV files are allowed.")

    csv_paths = [os.path.join(FILE_PATH, file) for file in csv_files]
    print(f"Valid CSV files found: {csv_paths}")

    kwargs["ti"].xcom_push(key="csv_paths", value=csv_paths)



#Task for validation
def validate_data(**kwargs):
    ti = kwargs["ti"]
    csv_paths = ti.xcom_pull(task_ids="check_file_format", key="csv_paths")
    if not csv_paths:
        raise ValueError("No CSV files received for validation.")
    
    for file_path in csv_paths:
        df = pd.read_csv(file_path) 
    
    #variables for validation rules
    employee_id_prefix = Variable.get("employee_id_prefix")
    name_validation = Variable.get("name_validation")
    hire_date_dob_check = Variable.get("hire_date_dob_check")
    date_format = Variable.get("date_format")

    #validation rule 1- converting date format to yy-mm-dd and Hire date to be greater than date of birth
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], dayfirst=True)
    df["hire_date"] = pd.to_datetime(df["hire_date"], dayfirst=True)

    df["date_of_birth"] = df["date_of_birth"].dt.strftime(date_format)
    df["hire_date"] = df["hire_date"].dt.strftime(date_format)

    
    if not df.eval(hire_date_dob_check).all():
        raise ValueError("Hire date must be greater than date of birth.")
    
    #validation rule2- Employee ID should start with E
    if not df["employee_id"].astype(str).str.startswith(employee_id_prefix).all():
        raise ValueError(f"All employee IDs must start with '{employee_id_prefix}'")
    if df["employee_id"].duplicated().any():
        raise ValueError("Employee ID must be unique.")

    #validation rule 3 - Name should be of the format first name and last name
    df["full_name"] = df["full_name"].str.strip()
    if not df["full_name"].str.match(name_validation).all():
        raise ValueError("Full name must contain at least two alphabetic words.")

    #validation rule 4 Salary should be entered only in numbers
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
    if df["salary"].isnull().any():
        raise ValueError("Salary should be numeric.")

    print("Validation passed.")

    ti.xcom_push(key="validated_df", value=df.to_json())


#Task for data cleanup
#clean  the data in the dataframe using the following conditions:
# 1.If duplicates exist, keep record with latest hire date
# 2.active Should be yes or no - Convert True,1 to yes and false,0 to no
# 3.convert email ids to all lowercase
def clean_data(**kwargs):
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="validate_data", key="validated_df")
    df = pd.read_json(df_json)

    # cleanup step1 - Keep the record with the latest hire date if duplicates exist
    df = df.sort_values(by="hire_date", ascending=False).drop_duplicates(subset="employee_id",keep="last")

    # cleanup step2 : Convert active column values
    df["active"] = df["active"].astype(str).str.lower().replace({"true": "yes","1": "yes","false": "no","0": "no"})

    # 3. Convert email to lowercase
    df["email"] = df["email"].str.lower()

    ti.xcom_push(key="cleaned_df", value=df.to_json())


#Task to create table dynamically and insert values from dataframe
def load_data(**kwargs):
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="clean_data", key="cleaned_df")
    df = pd.read_json(df_json)

    # Mapping pandas dtypes to PostgreSQL data types
    dtype_mapping = {
        "int64": "INTEGER",
        "float64": "NUMERIC",
        "object": "TEXT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
    }

    column_definitions = []
    for col, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), "TEXT")
        column_definitions.append(f'"{col}" {sql_type}')
    
    table_name = Variable.get("emp_table_name")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(column_definitions)},
        PRIMARY KEY ("employee_id")
    )
    """

    pg_hook = PostgresHook(postgres_conn_id=Variable.get("postgres_conn_id"))
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()

    # Insert Query
    columns = ', '.join([f'"{col}"' for col in df.columns])
    placeholders = ', '.join(['%s' for _ in df.columns])

    insert_query = f"""
    INSERT INTO {table_name} ({columns})
    VALUES ({placeholders})
    ON CONFLICT ("employee_id") DO UPDATE SET
    {", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != "employee_id"])};
    """

    # Insert data efficiently
    cur.executemany(insert_query, df.values.tolist())
    
    conn.commit()
    cur.close()
    conn.close()

