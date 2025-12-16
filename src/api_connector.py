import os
import requests

API_KEY = os.getenv('SECRET_PIERRE_API_KEY') 
BASE_URL = 'https://www.pierre.finance/tools/api'

def get_connection(path, headers={}, params={}):
    response = requests.get(f'{BASE_URL}/{path}', headers=headers, params=params)
    data = response.json()
    if response.status_code == 200:
        metadata = {
            "success": data["success"],
            "count": data["count"],
            "timestamp": data["timestamp"]
        }
        return data["data"], metadata
    elif response.status_code == 401:
        raise ValueError("expired API Key")
    else:
        raise ValueError(response.json())

###### PUBLIC METHODS 

def get_accounts():
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    path = 'get-accounts'
    return get_connection(path, headers=headers)


def get_transactions(filters=None):
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    
    if filters==None:
        params={}
    else:
        params=filters
    
    path = 'get-transactions'
    return get_connection(path, headers=headers, params=params)

    
     