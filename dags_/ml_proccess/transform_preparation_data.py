import pandas as pd
import requests
import datetime as dt
import json
import boto3
from sklearn.model_selection import train_test_split
from modules.utils import upload_to_s3
import tempfile
from os import path
import logging

# from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def transform_preparation_data(key: str, bucket_name: str):
    bucket = "bucket-csalinas"

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
        data["target"] = data["valor"].apply(lambda x: 1 if x >= 150 else 0)
        data = data.drop(["sitio", "fecha", "valor"], axis=1)
        X = data.drop(["target"], axis=1)
        y = data["target"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.30, random_state=42, stratify=y
        )

        dicc = {
            "X_train": X_train,
            "X_test": X_test,
            "y_train": y_train,
            "y_test": y_test,
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            for file in dicc:
                tmp_path = path.join(tmp_dir, f"{file}.json")
                dicc[file].to_json(tmp_path, date_format="iso", date_unit="s")
                upload_to_s3(
                    filename=tmp_path,
                    key=f"input_to_model/modeling/{file}.json",
                    bucket_name=bucket,
                )
