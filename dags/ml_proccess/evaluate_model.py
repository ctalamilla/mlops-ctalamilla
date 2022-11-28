import pandas as pd
from datetime import datetime
import numpy as np
import warnings
import joblib
import tempfile
from os import path
import logging
import _pickle as cPickle
import sklearn.metrics as metrics
from airflow.hooks.S3_hook import S3Hook
import boto3


def evaluate_model():
    hook = S3Hook("aws_conn")
    client = hook.get_conn()
    bucket = "bucket-csalinas"
    for file in ["X_train", "X_test", "y_train", "y_test"]:
        key_ = f"input_to_model/modeling/{file}.json"
        response = client.get_object(Bucket=bucket, Key=key_)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            file_ = response.get("Body")
            if file == "X_train":
                X_train = pd.read_json(file_)
                #print(X_train.info())
            elif file == "X_test":
                X_test = pd.read_json(file_)
                #print(X_test)
            elif file == "y_train":
                y_train = pd.read_json(file_, typ="series")
                #print(y_train)
            elif file == "y_test":
                y_test = pd.read_json(file_, typ="series")
                #print(y_test)
    bucket = "bucket-csalinas"
    key_model = 'models/best_model.pkl'
    response_model = hook.get_key(bucket_name=bucket, key=key_model)
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = path.join(tmp_dir, "bestmodel_.pkl")
        with open(tmp_path, 'wb') as data:
            response_model.download_fileobj(data)
            model = joblib.load(tmp_path)
            #model = joblib.load(body_string)
            y_train_pred = model.predict(X_train)
            y_test_pred = model.predict(X_test)
            print(f'Accuracy Train {metrics.accuracy_score(y_train, y_train_pred)}')
            print(f'Accuracy Test {metrics.accuracy_score(y_test, y_test_pred)}')