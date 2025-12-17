from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime
import logging # Faltava importar
import sys     # Faltava importar
import os

BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'jospin-pierre-api-stg')
DATAFRAME = os.environ.get('BIGQUERY_DATAFRAME', 'staging')

### CONFIG LOGS
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler(sys.stdout) 

c_handler.setLevel(logging.INFO)  

c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

c_handler.setFormatter(c_format)

logger.addHandler(c_handler)

def salvar_gcs(df, file_name, path=""):

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob_name = f"{path}{file_name}"
    blob = bucket.blob(blob_name)

    df["ingestion_time"] = datetime.now()
    df_csv = df.to_csv(index=False)

    logger.info(f"Iniciando carga para gs://{BUCKET_NAME}/{blob_name}...")

    blob.upload_from_string(df_csv, content_type='text/csv')

    logger.info(f"Carregado em gs://{BUCKET_NAME}/{blob_name}...")

    
    return True

def salvar_bigquery(df, table_name, write_type='append'):
    client = bigquery.Client()
    table_id = f"{client.project}.{DATAFRAME}.{table_name}"

    df["ingestion_time"] = datetime.now()

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE" if write_type == 'replace' else "WRITE_APPEND",
    )

    logger.info(f"Iniciando carga para {table_id}...")

    job = client.load_table_from_dataframe(
        df, 
        table_id, 
        job_config=job_config
    )

    job.result()
    table = client.get_table(table_id)

    logger.info(f"Carregado {job.output_rows} linhas. Total na tabela: {table.num_rows}")

    return True
