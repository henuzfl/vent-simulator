
from urllib.parse import urljoin
import requests
import json
import uuid
from datetime import datetime


web_base_url = 'http://192.168.1.230:6016'
iot_base_url = 'http://192.168.1.203:19239'
username = 'demo1'
password = '813446832bfa405428d7a5d4ffc366a8'

token = None


def login():
    global token
    url = urljoin(web_base_url, "/User/Login")
    payload = {'username': username, 'password': password}
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
            "Type": command.type,
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
        iot_base_url, '/XAYL_IotService/SystemInformation/SendLinkageControlMsg')
    headers = {'token': token, 'Content-Type': 'application/json'}
    command_dict = {}
    for command in commands:
        if command.system_code not in command_dict:
            command_dict[command.system_code] = []
        command_dict[command.system_code].append(command)
    for system_code in command_dict:
        headers["SystemCode"] = str(system_code)
        payload = json.dumps(
            get_playload_by_commands(command_dict[system_code]))
        print(payload)
        r = requests.post(url, data=payload, headers=headers)
        print(r.text)
