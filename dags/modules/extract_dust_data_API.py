import pandas as pd
import requests
import json
import logging
import tempfile
from os import path
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
    with tempfile.TemporaryDirectory() as tmp_dir:
            logging.info(f"Extracting data from API.....")
            tmp_path = path.join(tmp_dir, "dust_data_raw.json")
            with open(tmp_path, "w") as outfile:
                json.dump(r.json(), outfile)
            # Upload file to S3.
            logging.info(f"Writing results to S3 rawdata/dust/dust_data_raw.json")
            s3_hook = S3Hook(aws_conn_id="aws_conn")
            s3_hook.load_file(
                filename = tmp_path,
                key="rawdata/dust/dust_data_raw.json",
                bucket_name="bucket-csalinas",
                replace=True,
            )
