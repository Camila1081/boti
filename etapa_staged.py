from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from google.oauth2 import service_account
import pathlib
!pip uninstall openpyxl


def etapa_staged (bucket_name_staged,project_name):
    
  #CONFIG_FILE: str = 'gs://us-east1-vendas-boti-132a3e1b-bucket/dags/data/key_file.json'
  SRC_DIR_PATH = pathlib.Path(__file__).parent.absolute()
  CONFIG_FILE: str = f"{SRC_DIR_PATH}/key_file.json"
  # create storage client
  storage_client = storage.Client.from_service_account_json(CONFIG_FILE)# get bucket with name
  #storage_client = storage.Client.from_service_account_json('key_file.json')


  #MIGRANDO ARQUIVO MODIFICADO PARA BIGQUERY
  #Lendo do bucket staged
  #LENDO DADAS RAW DO STORAGE
  bucket = storage_client.get_bucket(bucket_name_staged) 
  stats = storage.Bucket.list_blobs(bucket)

  temp_file_name = 'base_staged.csv'
  i=0
  stats = storage.Bucket.list_blobs(bucket)
  for f in stats:
    i=i+1
    source_blob_name = f.name
    blob = bucket.get_blob(source_blob_name)
    blob.download_to_filename(temp_file_name)
    file = pd.read_csv(temp_file_name)
    if i==1: df_staged=file  
    df_staged = df_staged.append(file)

  df_staged

  df_staged=df_staged[['ID_MARCA','MARCA','ID_LINHA','LINHA','DATA_VENDA','QTD_VENDA','ANO_REF','ANO_VENDA','MES_VENDA']]
  df_staged['DATA_VENDA']=pd.to_datetime(df_staged['DATA_VENDA'], format='%Y-%m-%d')
  df_staged

  client = bigquery.Client()
  dataset_ref = client.dataset('dados_anos')

  df_staged.to_gbq(destination_table = 'dados_anos.base_consolidada', project_id=project_name,if_exists='replace' )
