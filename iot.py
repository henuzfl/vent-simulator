
import configparser
import json
import os
import random
import threading
from datetime import datetime


import psycopg2
from paho.mqtt import client as mqtt_client

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))


point_type_relation = {
    "频率反馈": "freq",
    "开控制": "is_open",
    "关到位信号": "is_open",
    "正风信号": "is_anti_wind",
    "开度反馈": "open_angle",
    "温度": "temperature",
    "湿度": "humidity",
    "风速": "speed",
    "压差": "pressure",
    "甲烷": "gas",
    "频率给定": "set_freq",
    "反风控制": "set_anti_wind",
    "开控制": "set_open",
    "停控制": "set_stop"
}

attrs = {}


class DeviceAttr(object):

    code = None
    name = None
    system_code = None
    value = None

    def __init__(self, code, type, system_code):
        self.code = code
        self.name = point_type_relation[type]
        self.system_code = system_code

    def __str__(self) -> str:
        return "code:" + self.code + ",name:" + self.name + ",value:" + self.value


class Device(object):
    id = None
    name = None
    type = None
    attrs = []

    def __init__(self, id, name, type):
        self.id = id
        self.name = name
        self.type = type

    def __str__(self) -> str:
        return "id:" + self.id + ",name:" + self.name + ",attrs:" + self.attrs


class Meta(object):

    pg_host = config['pg']['host']
    pg_port = int(config['pg']['port'])
    pg_database = config['pg']['database']
    pg_user = config['pg']['user']
    pg_password = config['pg']['password']
    lane_ids = None

    def __init__(self):
        self.lane_ids = self.get_lanes()

    def connect_pg(self):
        conn = psycopg2.connect(host=self.pg_host, port=self.pg_port,
                                database=self.pg_database, user=self.pg_user, password=self.pg_password)
        return conn

    def load_devices(self):
        ans = []
        ans += self.load_fans()
        ans += self.load_wind_doors()
        ans += self.load_wind_windows()
        ans += self.load_sensors()
        return ans

    def get_lanes(self):
        conn = self.connect_pg()
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

    def load_fans(self):
        sql = "select * from vent_fan where str_lane_id in {}".format(
            tuple(self.lane_ids))
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(sql)
        ans = []
        for fan in cursor.fetchall():
            device = Device(fan[5], fan[2], "fan")
            device.attrs = self.get_device_attrs(0, fan[1])
            ans.append(device)
        cursor.close()
        return ans

    def load_sensors(self):
        sql = "select * from vent_sensor where str_lane_id in {}".format(
            tuple(self.lane_ids))
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(sql)
        ans = []
        for sensor in cursor.fetchall():
            device = Device(sensor[8], sensor[2], "sensor")
            device.attrs = self.get_device_attrs(3, sensor[1])
            ans.append(device)
        cursor.close()
        return ans

    def load_wind_doors(self):
        sql = "select * from vent_structure where str_vs_type = '风门' and str_lane_id in {}".format(
            tuple(self.lane_ids))
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(sql)
        ans = []
        for wind_door in cursor.fetchall():
            device = Device(wind_door[5], wind_door[2], "windDoor")
            device.attrs = self.get_device_attrs(1, wind_door[1])
            ans.append(device)
        cursor.close()
        return ans

    def load_wind_windows(self):
        sql = "select * from vent_structure where str_vs_type = '风窗' and str_lane_id in {}".format(
            tuple(self.lane_ids))
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(sql)
        ans = []
        for wind_window in cursor.fetchall():
            device = Device(wind_window[5], wind_window[2], "windWindow")
            device.attrs = self.get_device_attrs(1, wind_window[1])
            ans.append(device)
        cursor.close()
        return ans

    def get_device_attrs(self, deviceType, deviceId):
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(
            "select * from tbl_element_bind_info where element_type = %s and element_id = '%s'" % (deviceType, deviceId))
        device_info = cursor.fetchone()
        ans = []
        if None != device_info:
            cursor.execute(
                "select * from tbl_element_bind_point where element_bind_info_id = %s" % (device_info[0]))
            points = cursor.fetchall()
            for point in points:
                if point[3] not in attrs:
                    attrs[point[3]] = DeviceAttr(point[3], point[5], point[6])
                ans.append(attrs[point[3]])
        cursor.close()
        return ans


class Iot(object):

    broker = config['mqtt']['broker']
    port = int(config['mqtt']['port'])
    username = config['mqtt']['username']
    password = config['mqtt']['password']
    client_id = f'vent_simulator_' + str(random.randrange(0, 100))

    client = None

    def __init__(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        self.client = mqtt_client.Client(self.client_id)
        self.client.username_pw_set(
            username=self.username, password=self.password)
        self.client.on_connect = on_connect
        self.client.connect(self.broker, self.port)
        self.subscribe()
        t = threading.Thread(target=self.subscribe,  args=())
        t.start()

    def process_point_content(self, content):
        for tmp in content:
            if tmp["PointCode"] in attrs:
                attrs[tmp["PointCode"]].value = tmp["RealtimeValue"]

    def process_person_position(self, content):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for tmp in content:
            msg = {
                'id': tmp['Id'],
                'name': tmp['PersonName'],
                'type': 'position',
                'x': tmp['CoordinateX'],
                'y': tmp['CoordinateY'],
                'z': tmp['CoordinateZ'],
                'timestamp': ts
            }
            self.publish('vent/position/values', json.dumps(msg))

    def process_command(self, content):
        for c in content:
            print(c)

    def subscribe(self):
        def on_message(client, userdata, msg):
            topic = msg.topic
            content = json.loads(msg.payload.decode())
            if topic == 'mas.iot.realtimedata':
                self.process_point_content(content)
            elif topic == 'mas.iot.PorealTime':
                self.process_person_position(content)
            elif topic == 'vent/device/commands':
                '''
                    todo 处理控制信息，发送到梅安森
                '''
                self.process_command(content)
        self.client.subscribe('mas.iot.realtimedata')
        self.client.subscribe('mas.iot.PorealTime')
        self.client.on_message = on_message
        self.client.loop_start()

    def publish(self, topic, msg):
        self.client.publish(topic, payload=msg)


# meta = Meta()
# devices = meta.load_devices()
# for device in devices:
#     print(device.name)
