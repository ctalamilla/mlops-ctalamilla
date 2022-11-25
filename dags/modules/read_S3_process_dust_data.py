import pandas as pd
import json 
from modules.psql_cli import psql_Client
from airflow.hooks.S3_hook import S3Hook


#from airflow.providers.amazon.aws.hooks.s3  import S3Hook

def read_S3_and_process_data(key: str, bucket_name:str) -> str:
    db = "airflow:airflow@postgres:5432/weather_data"
    psql_cli = psql_Client(db)
    hook = S3Hook('aws_conn')
    file = hook.read_key(key=key, bucket_name=bucket_name)
    print(type(file))

    pmdata = pd.DataFrame(json.loads(file).get("results"))
    pmdata["fecha"] = pmdata["muestra"].apply(lambda x: x.get("fecha"))
    pmdata["sitio"] = pmdata["muestra"].apply(
        lambda x: x.get("punto_de_muestreo_nombre")
    )
    pmdata.drop(
        ["id", "muestra", "parametro_por_matriz", "valor", "parametro_nombre"], axis=1
    )["sitio"]
    pmdata = pmdata.drop(
        ["id", "muestra", "parametro_por_matriz", "valor", "parametro_nombre"], axis=1
    )
    pmdata = pmdata.astype(
        {"representar_valor": "float", "fecha": "datetime64[ns]"}
    ).rename(columns={"representar_valor": "valor"})
    pmdata = pmdata.loc[pmdata["limite_deteccion"] == False].reset_index(drop=True)
    #print(pmdata)
    
    pmdata.to_json('/opt/airflow/datalake/dust_data_processed.json')
    print(pmdata)
    print(pmdata.info())
    psql_cli.insert_from_frame(pmdata, "dust_table")
    hook.load_file(
        filename = "/opt/airflow/datalake/dust_data_processed.json",
        key = "rawdata/dust/dust_data_processed.json",
        bucket_name = "bucket-csalinas",
        replace=True
    )