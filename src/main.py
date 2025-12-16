import os
import sys  
import requests
import pandas as pd
import logging
from flask import Flask, request
import json
from api_connector import *

### CONFIG LOGS
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler(sys.stdout) 
f_handler = logging.FileHandler('erros.log')  

c_handler.setLevel(logging.INFO)  
f_handler.setLevel(logging.ERROR)

c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

logger.addHandler(c_handler)
logger.addHandler(f_handler)


### ROTAS DA API
app = Flask(__name__)



@app.route("/accounts/full/", methods=['GET'])
def full_accounts():
    logger.info("Process Account Full Started")
    data, metadata = get_accounts()
    logger.info("Get Requests", json.dumps(metadata, indent=4))
    return data

@app.route("/transactions/full/", methods=['GET'])
def full_transactions():
    logger.info("Process Transactions Full Started")
    data, metadata = get_transactions()
    logger.info("Get Requests", json.dumps(metadata, indent=4))
    return data

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
    
    return data

@app.route("/")
def hello_world():
    return "The App is up"

if __name__ == "__main__":
    # Pega a porta do ambiente ou usa 8080 como padrão
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)