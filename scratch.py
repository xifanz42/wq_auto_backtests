import requests
import json
import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()
username = os.environ.get('BRAIN_USERNAME')
password = os.environ.get('BRAIN_PASSWORD')
sess = requests.Session()
sess.auth = HTTPBasicAuth(username, password)
res = sess.post('https://api.worldquantbrain.com/authentication')

# Inspect alpha endpoint
alpha_res = sess.get('https://api.worldquantbrain.com/alphas/gJorZdl0')
print("---ALPHA ENDPOINT---")
print(json.dumps(alpha_res.json(), indent=2))

