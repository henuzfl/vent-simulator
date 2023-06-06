import configparser
import json
import os
from urllib.parse import urljoin
import psycopg2

import requests


base_url = 'http://localhost:8080/'

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))

pg_host = config['pg']['host']
pg_port = int(config['pg']['port'])
pg_database = config['pg']['database']
pg_user = config['pg']['user']
pg_password = config['pg']['password']

conn = psycopg2.connect(host=pg_host, port=pg_port,
                        database=pg_database, user=pg_user, password=pg_password)


def get_devices():
    url = urljoin(base_url, '/devices')
    res = requests.get(url)
    return json.loads(res.text)


devices = get_devices()


def get_lanes():
    cursor = conn.cursor()
    cursor.execute(
        "select str_id as scheme_id from vent_scheme where is_current = 1")
    scheme_id = cursor.fetchone()[0]
    cursor.execute(
        "select str_id as lane_id from vent_lane where str_scheme_id='{}'".format(scheme_id))
    ans = []
    for l in cursor.fetchall():
        ans.append(l[0])
    return ans


lane_ids = get_lanes()


def bind_fans():
    sql = "select * from vent_fan where str_lane_id in {}".format(
        tuple(lane_ids))
    cursor = conn.cursor()
    cursor.execute(sql)
    for fan in cursor.fetchall():
        device = find_device_by_name(fan[2])
        if None != device:
            bind(element_id=fan[1], element_type=0, device_id=device['id'])
    cursor.close()


def bind_wind_doors():
    sql = "select * from vent_structure where str_vs_type = '风门' and str_lane_id in {}".format(
        tuple(lane_ids))
    cursor = conn.cursor()
    cursor.execute(sql)
    for wind_door in cursor.fetchall():
        device = find_device_by_name(wind_door[2])
        if None != device:
            bind(element_id=wind_door[1],
                 element_type=2, device_id=device['id'])
    cursor.close()


def bind_wind_windows():
    sql = "select * from vent_structure where str_vs_type = '风窗' and str_lane_id in {}".format(
        tuple(lane_ids))
    cursor = conn.cursor()
    cursor.execute(sql)
    for wind_window in cursor.fetchall():
        device = find_device_by_name(wind_window[2])
        if None != device:
            bind(element_id=wind_window[1],
                 element_type=1, device_id=device['id'])
    cursor.close()


def bind_sensors():
    sql = "select * from vent_sensor where str_lane_id in {}".format(
        tuple(lane_ids))
    cursor = conn.cursor()
    cursor.execute(sql)
    for sensor in cursor.fetchall():
        device = find_device_by_name(sensor[2])
        if None != device:
            bind(element_id=sensor[1],
                 element_type=3, device_id=device['id'])
    cursor.close()


def find_device_by_name(name):
    for device in devices:
        if device['name'] == name:
            return device
    return None


def bind(element_id, element_type, device_id):
    url = urljoin(base_url, '/devices')
    playload = {
        "equipmentId": device_id,
        "elementId": element_id,
        "pointCodes": [],
        "type": element_type
    }
    headers = {'Content-Type': 'application/json'}
    requests.post(url, headers=headers, data=json.dumps(playload))


# bind_fans()
# bind_wind_doors()
# bind_wind_windows()
bind_sensors()