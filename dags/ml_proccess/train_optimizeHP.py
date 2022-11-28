import pandas as pd
from datetime import datetime
import numpy as np
import warnings
import joblib
import tempfile
from os import path
import logging

warnings.filterwarnings("ignore")
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import sklearn.metrics as metrics

from airflow.hooks.S3_hook import S3Hook


def train_model():
    hook = S3Hook("aws_conn")
    client = hook.get_conn()
    bucket = "bucket-csalinas"

    for file in ["X_train", "y_train"]:
        key_ = f"input_to_model/modeling/{file}.json"
        response = client.get_object(Bucket=bucket, Key=key_)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            file_ = response.get("Body")
            if file == "X_train":
                X_train = pd.read_json(file_)
                print(X_train.info())
            else:
                y_train = pd.read_json(file_, typ="series")
                print(y_train)

    forest_params = [
        {"max_depth": list(range(10, 15)), "max_features": list(range(0, 5))}
    ]
    rfc = RandomForestClassifier()
    clf = GridSearchCV(rfc, forest_params, cv=10, scoring="accuracy")
    clf.fit(X_train, y_train)
    print(clf.best_params_)
    print(clf.best_score_)
    model_selected = clf.best_estimator_
    with tempfile.TemporaryDirectory() as tmp_dir:
        # logging.info(f"Extracting data from API.....")
        tmp_path = path.join(tmp_dir, "best_model.json")
        joblib.dump(model_selected, tmp_path)
        hook.load_file(
            filename=tmp_path,
            key="models/best_model.pkl",
            bucket_name="bucket-csalinas",
            replace=True,
        )
