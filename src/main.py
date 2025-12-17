import os
import sys  
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from flask import Flask, request
import json
from api_connector import *
from storage_controller import *

### CONFIG LOGS
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler(sys.stdout) 

c_handler.setLevel(logging.INFO)  

c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

c_handler.setFormatter(c_format)

logger.addHandler(c_handler)


### ROTAS DA API
app = Flask(__name__)



@app.route("/accounts/full/", methods=['GET'])
def full_accounts():
    logger.info("Process Account Full Started")
    
    data, metadata = get_accounts()
    logger.info("Get Requests", json.dumps(metadata, indent=4))

    if len(data) != metadata["count"]:
        raise ValueError("Missing Data in request")
    
    df = pd.DataFrame(data)
    salvar_gcs(df, "accounts_full.csv", path="accounts/")
    salvar_bigquery(df, "pierre_api_accounts", write_type="replace")
    
    return metadata 

@app.route("/transactions/full/", methods=['GET'])
def full_transactions():
    logger.info("Process Transactions Full Started")

    data, metadata = get_transactions()
    logger.info("Get Requests", json.dumps(metadata, indent=4))
    
    if len(data) != metadata["count"]:
        raise ValueError("Missing Data in request")
    
    df = pd.DataFrame(data)
    salvar_gcs(df, "transactions_full.csv", path="transactions/")
    salvar_bigquery(df, "pierre_api_transactions", write_type="replace")

    return metadata

@app.route("/transactions/incremental/", methods=['GET'])
def inc_transactions():
    logger.info("Process Transactions Incremental Started")

    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)

    filters = {
        "startDate":start_date,
        "endDate":end_date
    }

    logger.info("Filters:", json.dumps(filters, indent=4))

    data, metadata = get_transactions(filters=filters)
    logger.info("Get Requests", json.dumps(metadata, indent=4))
    
    if len(data) != metadata["count"]:
        raise ValueError("Missing Data in request")
    
    df = pd.DataFrame(data)
    salvar_gcs(df, f"transactions_inc_{start_date}_to_{end_date}.csv", path="transactions/inc/")
    salvar_bigquery(df, "pierre_api_transactions", write_type="append")

    return metadata

@app.route("/transactions/daily/", methods=['GET'])
def daily_transactions():
    logger.info("Process Transactions Incremental Started")

    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    filters = {
        "startDate":start_date,
        "endDate":end_date
    }

    logger.info("Filters:", json.dumps(filters, indent=4))

    data, metadata = get_transactions(filters=filters)
    logger.info("Get Requests", json.dumps(metadata, indent=4))
    
    if len(data) != metadata["count"]:
        raise ValueError("Missing Data in request")
    
    df = pd.DataFrame(data)
    salvar_gcs(df, f"transactions_inc_{start_date}_to_{end_date}.csv", path="transactions/inc/")
    salvar_bigquery(df, "pierre_api_transactions", write_type="append")

    return metadata

@app.route("/")
def hello_world():
    return "The App is up"

if __name__ == "__main__":
    try:
        # Pega a porta do ambiente ou usa 8080 como padrão
        port = int(os.environ.get("PORT", 8080))
        logger.info(f"Tentando iniciar na porta {port}")
        app.run(debug=False, host="0.0.0.0", port=port)
    except Exception as e:
        logger.critical(f"ERRO FATAL AO INICIAR O APP: {e}")
        raise e