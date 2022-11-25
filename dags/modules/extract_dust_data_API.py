import pandas as pd
import requests
import json
from airflow.hooks.S3_hook import S3Hook

def extract_dust_data(fecha_de_inicio=None, fecha_de_fin=None):
    """Extract dust data from TCA"""
    
    headers = {"Authorization": "Token c6cc89f676d5f15805d892ec5611311fb01dd997"}

    api = "https://tca-ssrm.com/api/fisico-quimico/parametros-por-muestra"
    matriz = "matriz_id=8"
    parametro = "parametro_por_matriz_id=308"
    if (fecha_de_fin and fecha_de_fin) == None:
        r = requests.get(f"{api}?&{matriz}&{parametro}", headers=headers)
    else:
        r = requests.get(
            f"{api}?&{matriz}&{parametro}&fecha_de_inicio={fecha_de_inicio}&fecha_de_fin={fecha_de_fin}",
            headers=headers,
        )
    with open("/opt/airflow/datalake/dust_data_raw.json", "w") as outfile:
        json.dump(r.json(), outfile)
    
    
    with open("/opt/airflow/datalake/dust_data_raw.json", "r") as openfile:
        json_object = json.load(openfile)
    
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename = "/opt/airflow/datalake/dust_data_raw.json",
        key = "rawdata/dust/dust_data_raw.json",
        bucket_name = "bucket-csalinas",
        replace=True
    )