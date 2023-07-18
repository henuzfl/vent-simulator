import configparser
import json
import os
from urllib.parse import urljoin

import requests

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))


def callback(url, state, message):
    url = urljoin(config['vent']['app_url'], url)
    payload = {'status': state,
               'message': message}
    payload = json.dumps({
        "status": state,
        "message": message
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("PATCH", url, headers=headers, data=payload)
    print(response.text)
