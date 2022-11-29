from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import tempfile
from os import path
import logging

def get_postgres_data():
    sql_stmt = "select dt.fecha, b.temp_out, b.hum_out, b.rainfall_mm, b.wind_speed_avg, b.wind_speed_hi, dt.valor, dt.sitio from dust_table dt  inner join (SELECT date(date_time) AS fecha2 ,AVG(temp_out) AS temp_out, AVG(hum_out) as hum_out, sum(rainfall_mm) as rainfall_mm , avg(wind_speed_avg) as wind_speed_avg , max(wind_speed_hi) as wind_speed_hi from weather_table wt GROUP BY date(date_time)) as b  on dt.fecha = b.fecha2 order by fecha ;"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_local',
        schema='weather_data'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    input_to_model = pd.read_sql(sql_stmt, pg_conn)
    logging.info(f"Agregate table with{input_to_model.shape[0]} rows and {input_to_model.shape[1]} columns  ")
    print(input_to_model)
    input_to_model['fecha'] = input_to_model['fecha'].apply(lambda x: x.strftime('%Y-%m-%d')) 
    print(input_to_model.info())
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        #logging.info(f"Extracting data from API.....")
        tmp_path = path.join(tmp_dir, "input_to_model.json")
        input_to_model.to_json(tmp_path,date_format='iso', date_unit='s')
        cursor.fetchall()
        #logging.info(f"Writing results to S3 rawdata/dust/dust_data_raw.json")
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_hook.load_file(
            filename = tmp_path,
            key='input_to_model/input_to_model.json',
            bucket_name="bucket-csalinas",
            replace=True
    )

