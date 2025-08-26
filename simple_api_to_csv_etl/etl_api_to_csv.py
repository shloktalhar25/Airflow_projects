from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta
import requests
import pandas as pd
import os

# output folder 
OUTPUT_DIR = "/opt/airflow/data"
os.makedirs(OUTPUT_DIR,exist_ok=True)


# EXAMPLE API

API_URL  = "https://jsonplaceholder.typicode.com/posts"


# ETL Script

def extract_api_data():
    """Here in we are gona extract the data from the sources"""
    # GET:This method is used to retrieve data or resources from a server.
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()


def transform_data(raw_data):
    df = pd.DataFrame(raw_data)


    # example transformation
    df = df[["userId","id","title"]]
    df = df.rename(columns={"userId":"user_id", "id":"post_id"})
    return df

def load_to_csv(transformed_df):
    filename = f"api_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(OUTPUT_DIR,filename)
    transformed_df.to_csv(filepath,index=False)
    print(f"csv saved to {filepath}")


def etl_task():
    raw = extract_api_data()
    df = transform_data(raw)
    load_to_csv(df)


# airflow dag 

default_args = {
    "owner": "airflow",
    "depends_on_past" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=2)
}

with DAG(
    dag_id= 'api_to_csv_etl',
    default_args=default_args,
    description="ETL pipelines :API-> transform-> csv",
    schedule_interval = "*/10 * * * *", # every 10 mins
    start_date=datetime(2023,1,1),
    catchup=False,
    tags = ['etl','api','csv']

) as dag:

    run_etl = PythonOperator(
        task_id = 'run_etl',
        python_callable=etl_task

    )

    run_etl


