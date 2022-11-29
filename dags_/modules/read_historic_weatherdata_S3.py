import pandas as pd
import requests
import datetime as dt
import json
import boto3
import tempfile
from os import path
import logging

from modules.psql_cli import psql_Client

from airflow.hooks.S3_hook import S3Hook


def read_historic_weather_data(key: str, bucket_name:str)-> str:
    # import and to instance psql_Cli
    db = "airflow:airflow@postgres:5432/weather_data"
    psql_cli = psql_Client(db)
    
        
    hook = S3Hook('aws_conn')
    client = hook.get_conn()
    response = client.get_object(Bucket = bucket_name, Key= key)#hook.read_key(key=key, bucket_name= bucket_name)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        file = response.get('Body')
        data_historic = pd.read_json(file)
        data_historic = data_historic.loc[data_historic['Date']<'2022-11-13']
        # print(data_historic)
        data_historic['Date'] = data_historic['Date'].astype('str')
        #print(data_historic['Date'])

        data_historic["date_time"] = pd.to_datetime(
            (data_historic["Date"] + " " + data_historic["Time"])
            .str.replace("p", "PM")
            .str.replace("a", "AM"),
            format="%Y-%m-%d %I:%M %p",
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
        
        data_historic = data_historic.loc[
            :, ["date_time", "Out", "Hum", "Rain", "Speed", "Speed.1"]
        ]

        data_historic.columns = [
            "date_time",
            "temp_out",
            "hum_out",
            "rainfall_mm",
            "wind_speed_avg",
            "wind_speed_hi",
        ]

        data_historic = data_historic.replace("---", None).astype(
            {
                "date_time": "datetime64",
                "temp_out": "float64",
                "hum_out": "int64",
                "rainfall_mm": "float64",
                "wind_speed_avg": "int64",
                "wind_speed_hi": "int64",
            }
        )
        print(data_historic.info())
        
        psql_cli.insert_from_frame(data_historic, "weather_table")
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            
            #logging.info(f"Extracting data from API.....")
            tmp_path = path.join(tmp_dir, "weatherdata_processed.json")
            data_historic.to_json(tmp_path,date_format='iso', date_unit='s')
            
            hook.load_file(
                filename = tmp_path,
                key='processed/weather/weatherdata_processed.json',
                bucket_name="bucket-csalinas",
                replace=True
        )
        
        
