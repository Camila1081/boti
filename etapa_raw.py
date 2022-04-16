# -*- coding: utf-8 -*-
"""Boti_code.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1dtk8rQ8f_IVwfTlRzPwzHLXgo2GaqLHJ
"""

#!pip install gcsfs
#!pip install pandas_gbq

# Commented out IPython magic to ensure Python compatibility.
# %reset
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from google.oauth2 import service_account
import pathlib

SRC_DIR_PATH = pathlib.Path(__file__).parent.absolute()
CONFIG_FILE: str = f"{SRC_DIR_PATH}/key_file.json"


def etapa_raw(bucket_name_raw, bucket_name_staged):
    # CONFIG_FILE: str = 'gs://us-east1-vendas-boti-132a3e1b-bucket/dags/data/key_file.json'
    # create storage client
    storage_client = storage.Client.from_service_account_json(CONFIG_FILE)
    # storage_client = storage.Client.from_service_account_json('key_file.json')

    # LENDO DADAS RAW DO STORAGE

    bucket = storage_client.get_bucket(bucket_name_raw)  # get bucket data as blob
    stats = storage.Bucket.list_blobs(bucket)

    temp_file_name = "base.xls"
    i = 0
    stats = storage.Bucket.list_blobs(bucket)
    for f in stats:
        i = i + 1
        file_name = f.name
        a = file_name.split("_")[1].split(".")[0]
        source_blob_name = file_name
        blob = bucket.get_blob(source_blob_name)
        blob.download_to_filename(temp_file_name)
        file = pd.read_excel(temp_file_name)
        file["ANO_REF"] = a
        file["MES_VENDA"] = file["DATA_VENDA"].dt.month
        file["ANO_VENDA"] = file["DATA_VENDA"].dt.year
        if i == 1:
            df = file
        df = df.append(file)

    df

    # SENDING TO STAGED BUCKET
    destination_file_name = "consolidado.csv"
    df.to_csv(destination_file_name)
    bucket = storage_client.get_bucket(bucket_name_staged)
    bucket.blob(destination_file_name).upload_from_string(df.to_csv(), "text/csv")
