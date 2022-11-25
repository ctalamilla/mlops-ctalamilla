import pandas as pd
import requests
import datetime as dt
import json
import boto3
from sklearn.model_selection import train_test_split
from modules.utils import upload_to_s3

# from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def transform_preparation_data(key: str, bucket_name: str):
    bucket = "bucket-csalinas"
    prefix = "practica"
    train_file = "vertebral_train.csv"
    test_file = "vertebral_test.csv"
    validate_file = "vertebral_validate.csv"

    hook = S3Hook("aws_conn")
    client = hook.get_conn()
    response = client.get_object(
        Bucket=bucket_name, Key=key
    )  # hook.read_key(key=key, bucket_name= bucket_name)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        file = response.get("Body")
        data = pd.read_json(file)
        print(data)
        print(data.info())
        X = pd.concat(
            [
                data.drop(["sitio", "fecha", "valor"], axis=1),
                pd.get_dummies(data["sitio"]),
            ],
            axis=1,
        )
        y = data["valor"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42
        )

        X_train.to_json(
            "/opt/airflow/datalake/X_train.json", date_format="iso", date_unit="s"
        )
        X_test.to_json(
            "/opt/airflow/datalake/X_test.json", date_format="iso", date_unit="s"
        )
        y_train.to_json(
            "/opt/airflow/datalake/y_train.json", date_format="iso", date_unit="s"
        )
        y_test.to_json(
            "/opt/airflow/datalake/y_test.json", date_format="iso", date_unit="s"
        )

        for file in ["X_train", "X_test", "y_train", "y_test"]:
            upload_to_s3(
                filename=f"/opt/airflow/datalake/{file}.json",
                key=f"input_to_model/modeling/{file}.json",
                bucket_name=bucket,
            )
