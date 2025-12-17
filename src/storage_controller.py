from google.cloud import storage
import pandas as pd
from datetime import datetime
import os

BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'jospin-pierre-api-stg')


def salver_gcs(df, file_name, path=""):

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob_name = f"{path}{file_name}"
    blob = bucket.blob(blob_name)

    df["ingestion_time"] = datetime.now()
    df_csv = df.to_csv(index=False)

    blob.uplod_from_string(df_csv, content_type='text/csv')
    
    return True