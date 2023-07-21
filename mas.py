import configparser
import os
from urllib.parse import urljoin
import requests
import json
import uuid
from datetime import datetime


config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))

token = None


def login():
    global token
    url = urljoin(config['mas']['web_base_url'], "/User/Login")
    payload = {'username': config['mas']['username'],
               'password': config['mas']['password']}
    r = requests.post(url, data=payload)
    res = json.loads(r.text)
    if res['code'] == 200:
        token = res['data']
    else:
        raise requests.HTTPError


def get_playload_by_commands(commands):
    ans = []
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for command in commands:
        tmp = {
            "Id": str(uuid.uuid4()),
            "Type": 1,
            "CtrlType": command.ctrl_type,
            "Message": command.value,
            "PointList": command.point,
            "Time": ts,
            "Remark": ''
        }
        ans.append(tmp)
    return ans


def control(commands):
    global token
    if token is None:
        login()
    url = urljoin(
        config['mas']['iot_base_url'], '/XAYL_IotService/SystemInformation/SendLinkageControlMsg')
    headers = {'token': token, 'Content-Type': 'application/json'}
    command_dict = {}
    for command in commands:
        if command.system_code not in command_dict:
            command_dict[command.system_code] = []
        command_dict[command.system_code].append(command)
    for system_code in command_dict:
        url = url + '?SystemCode=' + str(system_code)
        payload = json.dumps(
            get_playload_by_commands(command_dict[system_code]))
        print(payload)
        r = requests.post(url, data=payload, headers=headers)
        print(r.text)
        respose = json.loads(r.text)
        if respose['code'] != 200:
            raise  requests.HTTPError()
