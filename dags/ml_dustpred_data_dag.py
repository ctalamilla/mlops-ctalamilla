from datetime import datetime
import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
import io
import pandas as pd
import json

from modules.extract_dust_data_API import extract_dust_data
from modules.read_S3_process_dust_data import read_S3_and_process_data
from modules.read_historic_weatherdata_S3 import read_historic_weather_data
from modules.postgres_to_S3_aggData import get_postgres_data
from modules.extract_wheather_data_API import extract_weather_data
from ml_proccess.transform_preparation_data import transform_preparation_data
from ml_proccess.train_optimizeHP import train_model
from ml_proccess.evaluate_model import evaluate_model


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook("aws_conn")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)


def s3_to_local(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook("aws_conn")
    file = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file


with DAG(
    dag_id="ml_dust_prediction_",
    schedule_interval="@daily",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
) as dag:

    create_table_w = PostgresOperator(
        task_id="create_weather_table",
        postgres_conn_id="postgres_local",
        sql="""
            DROP TABLE IF EXISTS weather_table;
            CREATE TABLE IF NOT EXISTS weather_table (
            index SERIAL PRIMARY KEY,
            date_time TIMESTAMP,
            temp_out FLOAT,
            hum_out INTEGER,
            rainfall_mm FLOAT,
            wind_speed_avg INTEGER,
            wind_speed_hi INTEGER
            );
          """,
    )

    create_table_d = PostgresOperator(
        task_id="create_dust_table",
        postgres_conn_id="postgres_local",
        sql="""
            DROP TABLE IF EXISTS dust_table;
            CREATE TABLE IF NOT EXISTS dust_table (
            index SERIAL PRIMARY KEY,
            limite_deteccion VARCHAR,
            valor FLOAT,
            fecha DATE,
            sitio VARCHAR
            );
          """,
    )

    ingest_dust_data = PythonOperator(
        task_id="ingest_dustdata_from_API", python_callable=extract_dust_data
    )

    read_from_s3 = PythonOperator(
        task_id="read_S3_and_process_data",
        python_callable=read_S3_and_process_data,
        op_kwargs={
            "key": "rawdata/dust/dust_data_raw.json",
            "bucket_name": "bucket-csalinas",
        },
    )

    read_from_s3_weather = PythonOperator(
        task_id="read_S3_historic_weatherdata",
        python_callable=read_historic_weather_data,
        op_kwargs={
            "key": "rawdata/weather/alldatapir.json",
            "bucket_name": "bucket-csalinas",
        },
    )

    update_weatherdata_from_API = PythonOperator(
        task_id="update_weatherdata", python_callable=extract_weather_data
    )

    get_postgres_data = PythonOperator(
        task_id="get_postgres_Aggdata",
        python_callable=get_postgres_data,
        do_xcom_push=True,
    )

    dummy = DummyOperator(task_id="init_process")

    transform_preparation_data = PythonOperator(
        task_id="transform_prep_data",
        python_callable=transform_preparation_data,
        op_kwargs={
            "key": "input_to_model/input_to_model.json",
            "bucket_name": "bucket-csalinas",
        },
    )

    train_model = PythonOperator(task_id="training_model", python_callable=train_model)
    
    evaluate_model = PythonOperator(
        task_id = "evaluating_model",
        python_callable= evaluate_model
    )


(dummy >> create_table_d >> ingest_dust_data >> read_from_s3 >> get_postgres_data)

(
    dummy
    >> create_table_w
    >> read_from_s3_weather
 #   >> update_weatherdata_from_API
    >> get_postgres_data
    >> transform_preparation_data
    >> train_model
    >> evaluate_model
)
