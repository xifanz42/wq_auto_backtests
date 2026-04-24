# src/auth.py
import os
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

def authenticate():
    load_dotenv()
    username = os.environ.get('BRAIN_USERNAME')
    password = os.environ.get('BRAIN_PASSWORD')
    if not username or not password:
        raise ValueError("Please provide BRAIN_USERNAME and BRAIN_PASSWORD in .env")
    
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    
    response = sess.post('https://api.worldquantbrain.com/authentication')
    if response.status_code != 201:
        print("Error authenticating:", response.status_code, response.text)
        return None
    return sess
