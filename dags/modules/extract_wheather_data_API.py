import pandas as pd
import requests
from datetime import datetime
from datetime import timedelta
import hmac
import hashlib
import logging
from modules.psql_cli import psql_Client

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
    # import and to instance psql_Cli
    db = "airflow:airflow@postgres:5432/weather_data"
    psql_cli = psql_Client(db)
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
    #print(lastDate)
    lastDate = datetime.strptime(lastDate, "%Y-%m-%d")
    lastDateTS = int(datetime.timestamp(lastDate))
    #print(lastDateTS)
    
    datelist =[]
    gap_data =[]
    for date in pd.date_range(lastDate, end= (datetime.now()- timedelta(days=1))):
        try:
            #end_date = datetime.strptime(date, "%d/%m/%Y")
            start_ts = int(datetime.timestamp(date))
            datelist.append(date)
            end_ts = start_ts + 86400
            print(f'fecha {date},  start TS: {start_ts}, final TS: {end_ts},' )

            #API production URL
            logging.info(f"Extracting data from API for {date}")
            now = int(datetime.timestamp(datetime.now())) 
            mje = f"api-key{weather_key}end-timestamp{end_ts}start-timestamp{start_ts}station-id{id_station}t{now}"

            signature = hmac.new(
                bytes(wheather_secret, "latin-1"),
                msg=bytes(mje, "latin-1"),
                digestmod=hashlib.sha256,
            ).hexdigest()
            # print(signature)

            url = f"{api}{id_station}?api-key={weather_key}&t={now}&start-timestamp={start_ts}&end-timestamp={end_ts}&api-signature={signature}"
            print(url)

            req_weatherlink = requests.get(url)

            data = req_weatherlink.json().get("sensors")[0].get("data")

            weatherdata = pd.DataFrame(data)

            weatherdata["date_time"] = weatherdata["ts"].apply(
                lambda x: datetime.fromtimestamp(x)
            )

            weatherdata = weatherdata.loc[
                :,
                [
                    "date_time",
                    "temp_out",
                    "hum_out",
                    "rainfall_mm",
                    "wind_speed_avg",
                    "wind_speed_hi",
                ],
            ]
            print(weatherdata)
            gap_data.append(weatherdata)
        except TypeError:
            pass
    gap_df = pd.concat(gap_data).reset_index(drop=True)
    logging.info(f"Updating psql db from {lastDate} to {datetime.now()}")
    psql_cli.insert_from_frame(gap_df, "weather_table")
    print(gap_df)
    print(gap_df.info())
