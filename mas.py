
from urllib.parse import urljoin
import requests
import json


web_base_url = 'http://192.168.1.230:6016'
iot_base_url = 'http://192.168.1.203:19239'
username = 'demo1'
password = '813446832bfa405428d7a5d4ffc366a8'

token = None


def login():
    url = urljoin(web_base_url, "/User/Login")
    payload = {'username': username, 'password': password}
    r = requests.post(url, data=payload)
    res = json.loads(r.text)
    if res['code'] == 200:
        return res['data']
    else:
        raise requests.HTTPError


def control():
    url = urljoin(
        iot_base_url, '/XAYL_IotService/SystemInformation/SendLinkageControlMsg')
    headers = {'token': login(),"SystemCode":"99"}
    payload = {
        "Id": "11111111",
        "Type": 1,
        "CtrlType": 1,
        "Message": "55",
        "PointList": "FJ1_PLSD",
        "Time": "2023-06-02 15:00:00",
        "Remark": ""
    }

    r = requests.post(url, data=payload, headers=headers)
    print(r.text)


control()
