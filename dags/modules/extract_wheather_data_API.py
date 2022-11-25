import pandas as pd
import requests
from datetime import datetime
import hmac
import hashlib

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

# API parameters
weather_key = "rjfkvzaasee9hhcjawpay6osuvghc0fl"
wheather_secret = "oax5jv7eolavqw1txwzk8e2rnq7mnkv3"
id_station = 129681
api = "https://api.weatherlink.com/v2/historic/"
# API doc
# https://weatherlink.github.io/v2-api/tutorial#step-4---get-historic-data
# https://weatherlink.github.io/v2-api/api-signature-calculator
#####




def extract_weather_data():
    """extract 24 hours of data from the API to Dataframe"""
    
    ##last measure in db
    sql_stmt = "SELECT date(date_time) AS fecha2 from weather_table wt GROUP BY date(date_time) order by fecha2 desc limit 1;"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_local',
        schema='weather_data'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    lastDate = str(pd.read_sql(sql_stmt, pg_conn).iloc[0,0])
    print(lastDate)
    lastDate = datetime.strptime(lastDate, "%Y-%m-%d")
    lastDateTS = int(datetime.timestamp(lastDate))
    print(lastDateTS)
    
    datelist =[]
    for date in pd.date_range(lastDate, end= datetime.now()):
        #end_date = datetime.strptime(date, "%d/%m/%Y")
        #end_ts = int(datetime.timestamp(end_date))
        datelist.append(date)
        print(date)
    print(datelist)
    #     start_ts = end_ts - 86400

    #     now = int(datetime.timestamp(datetime.now()))
    #     mje = f"api-key{weather_key}end-timestamp{end_ts}start-timestamp{start_ts}station-id{id_station}t{now}"

    #     signature = hmac.new(
    #         bytes(wheather_secret, "latin-1"),
    #         msg=bytes(mje, "latin-1"),
    #         digestmod=hashlib.sha256,
    #     ).hexdigest()
    #     # print(signature)

    #     url = f"{api}{id_station}?api-key={weather_key}&t={now}&start-timestamp={start_ts}&end-timestamp={end_ts}&api-signature={signature}"
    #     # print(url)

    #     req_weatherlink = requests.get(url)

    #     data = req_weatherlink.json().get("sensors")[0].get("data")

    #     weatherdata = pd.DataFrame(data)

    #     weatherdata["date_time"] = weatherdata["ts"].apply(
    #         lambda x: datetime.fromtimestamp(x)
    #     )

    #     weatherdata = weatherdata.loc[
    #         :,
    #         [
    #             "date_time",
    #             "temp_out",
    #             "hum_out",
    #             "rainfall_mm",
    #             "wind_speed_avg",
    #             "wind_speed_hi",
    #         ],
    #     ]
    #     print(weatherdata)
    # return weatherdata
#extract_weather_data('20/11/2022')