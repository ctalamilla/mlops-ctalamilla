import boto3
import pandas as pd
import json
import datetime as dt

s3_client = boto3.client(
    "s3",
    aws_access_key_id='',
    aws_secret_access_key=''
)

response = s3_client.get_object(Bucket='bucket-csalinas', Key='rawdata/weather/alldatapir.json')

status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    data_historic = pd.read_json(response.get("Body"))
    print(data_historic)
    data_historic['Date'] = data_historic['Date'].astype('str')
    #dateUnion = (data_historic["Date"] + " " + data_historic["Time"])
    #print(dateUnion)
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
    data_historic_agg = (
        data_historic.groupby([data_historic["date_time"].dt.date])
        .agg(
            {
                "temp_out": "mean",
                "hum_out": "mean",
                "rainfall_mm": "sum",
                "wind_speed_avg": "mean",
                "wind_speed_hi": "max",
            }
        )
        .reset_index()
    )
    data_historic_agg["date_time"] = pd.to_datetime(data_historic_agg["date_time"])
    print(data_historic_agg)


