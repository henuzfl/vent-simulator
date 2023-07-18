
import configparser
import json
import os
import random
from datetime import datetime

import psycopg2
from paho.mqtt import client as mqtt_client

from mas import control
from vent import callback

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))

point_real_time_topic = 'mas.iot.realtimedata'
position_real_time_topic = 'mas.iot.PorealTime'
device_command_topc = 'vent/device/commands'

'''
风机停机信号 1
风机停机设定 2
风机反风信号 3
风机反风设定 4
风机开控制 5
风机正风信号 6
风机频率反馈 7
风机频率设定 8
风机开设定 20
风窗开度反馈 9
风窗开度设定 10
风门关控制 11
风门开信号 12
风门开控制 13
风速 14
压差 15
温度 16
湿度 17
甲烷 18
广播 99
'''

attrs = {}

attr_device_dict = {}


class Command(object):

    system_code = None
    ctrl_type = None
    point = None
    value = None

    def __init__(self, system_code, ctrl_type, point, value):
        self.system_code = system_code
        self.ctrl_type = ctrl_type
        self.point = point
        self.value = value


class DeviceAttr(object):

    code = None
    use_type = None
    system_code = None
    value = None

    def __init__(self, code, use_type, system_code):
        self.code = code
        self.use_type = use_type
        self.system_code = system_code

    def __str__(self):
        return "code:" + self.code + ",use_type:" + str(self.use_type) + ",value:" + "None" if None == self.value else str(self.value)


class Device(object):
    id = None
    name = None
    type = None
    is_main = True
    attrs = []

    def __init__(self, id, name, type):
        self.id = id
        self.name = name
        self.type = type

    def __str__(self):
        return "id:" + self.id + ",name:" + self.name + ",attrs:\t\n" + "\t\n".join(str(a) for a in self.attrs)

    def to_message(self):
        if not self.is_main:
            return None
        if self.type == 'fan':
            return self.get_fan_message()
        elif self.type == 'windDoor':
            return self.get_wind_door_message()
        elif self.type == 'windWindow':
            return self.get_wind_window_message()
        elif self.type == 'sensor':
            return self.get_sensor_message()
        else:
            pass

    def get_fan_message(self):
        ans = {
            "id": self.id,
            "type": self.type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        freq = self.get_attr_value(7)
        if None != freq:
            ans["freq"] = freq
        forward_signal = self.get_attr_value(6)
        anti_signal = self.get_attr_value(3)
        if forward_signal != None and anti_signal != None:
            ans['is_open'] = 0 if (
                forward_signal == '0' and anti_signal == '0') else 1
        if forward_signal != None:
            ans['is_anti_wind'] = 1 if forward_signal == '0' else 0
        if anti_signal != None:
            ans['is_anti_wind'] = 1 if anti_signal == '1' else 0
        return ans

    def get_wind_window_message(self):
        ans = {
            "id": self.id,
            "type": self.type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        open_angle = self.get_attr_value(9)
        if None != open_angle:
            ans["open_angle"] = open_angle
        return ans

    def get_wind_door_message(self):
        ans = {
            "id": self.id,
            "type": self.type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        is_open = self.get_attr_value(12)
        if None != is_open:
            ans["is_open"] = int(is_open)
        return ans

    def get_sensor_message(self):
        ans = {
            "id": self.id,
            "type": self.type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        speed = self.get_attr_value(14)
        if None != speed:
            ans['speed'] = speed
        pressure = self.get_attr_value(15)
        if None != pressure:
            ans['pressure'] = pressure
        temperature = self.get_attr_value(16)
        if None != temperature:
            ans['temperature'] = temperature
        humidity = self.get_attr_value(17)
        if None != humidity:
            ans['humidity'] = humidity
        gas = self.get_attr_value(18)
        if None != gas:
            ans['gas'] = gas
        return ans

    def get_attr_value(self, type):
        a = self.get_attr(type)
        return None if a == None else a.value

    def get_attr(self, type):
        for a in self.attrs:
            if a.use_type == type:
                return a
        return None


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
        ans += self.load_broadcasts()
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

    def load_broadcasts(self):
        sql = "select * from vent_broadcast where str_lane_id in {}".format(
            tuple(self.lane_ids))
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(sql)
        ans = []
        for broadcast in cursor.fetchall():
            if None != broadcast[8] and len(broadcast[8]) > 0:
                device = Device(broadcast[1], broadcast[2], "broadcast")
                device.attrs = self.get_device_attrs(device, 5, broadcast[8])
                ans.append(device)
        cursor.close()
        return ans

    def load_fans(self):
        sql = "select * from vent_fan where str_lane_id in {}".format(
            tuple(self.lane_ids))
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(sql)
        ans = []
        for fan in cursor.fetchall():
            if None != fan[5] and len(fan[5]) > 0:
                device = Device(fan[1], fan[2], "fan")
                device.attrs = self.get_device_attrs(device, 0, fan[5])
                ans.append(device)
            if None != fan[6] and len(fan[6]) > 0:
                device = Device(fan[1], fan[2], "fan")
                device.is_main = False
                device.attrs = self.get_device_attrs(device, 0, fan[6])
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
            if None != sensor[8] and len(sensor[8]) > 0:
                device = Device(sensor[1], sensor[2], "sensor")
                device.attrs = self.get_device_attrs(device, 3, sensor[8])
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
            if None != wind_door[5] and len(wind_door[5]) > 0:
                device = Device(wind_door[1], wind_door[2], "windDoor")
                device.attrs = self.get_device_attrs(device, 1, wind_door[5])
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
            if None != wind_window[5] and len(wind_window[5]) > 0:
                device = Device(wind_window[1], wind_window[2], "windWindow")
                device.attrs = self.get_device_attrs(device, 2, wind_window[1])
                ans.append(device)
        cursor.close()
        return ans

    def get_device_attrs(self, device, equipment_type, equipment_id):
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute(
            "select * from tbl_element_bind_info where equipment_type = %s and equipment_id = '%s'" % (equipment_type, equipment_id))
        device_info = cursor.fetchone()
        ans = []
        if None != device_info:
            cursor.execute(
                "select * from tbl_element_bind_point where element_bind_info_id = %s" % (device_info[0]))
            points = cursor.fetchall()
            for point in points:
                if point[3] not in attrs:
                    attrs[point[3]] = DeviceAttr(point[3], point[7], point[6])
                attr_device_dict[point[3]] = device
                ans.append(attrs[point[3]])
        cursor.close()
        return ans


meta = Meta()
devices = meta.load_devices()


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
                self.subscribe()
            else:
                print("Failed to connect, return code %d\n", rc)
        self.client = mqtt_client.Client(self.client_id)
        self.client.username_pw_set(
            username=self.username, password=self.password)
        self.client.on_connect = on_connect
        self.client.connect(self.broker, self.port)
        self.client.loop_forever()

    def process_point_content(self, content):
        try:
            for tmp in content:
                if tmp["PointCode"] in attrs:
                    attrs[tmp["PointCode"]].value = tmp["RealtimeValue"]
                    device = attr_device_dict[tmp["PointCode"]]
                    if not device.is_main:
                        device.is_main = True
                        for d in device:
                            if device.id == d.id:
                                d.is_main = False
        except Exception as ex:
            print("处理测点数据内容异常%s" % ex)

    def process_person_position(self, content):
        try:
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for tmp in content:
                msg = {
                    'id': tmp['CardNo'],
                    'name': tmp['PersonName'],
                    'type': 'position',
                    'x': tmp['CoordinateX'],
                    'y': tmp['CoordinateY'],
                    'z': tmp['CoordinateZ'],
                    'timestamp': ts
                }
                self.publish('vent/position/values', json.dumps(msg))
        except Exception as ex:
            print("处理人员定位内容异常%s" % ex)

    def process_app_control(self, content):
        callback_url = content['callbackUrl']
        try:
            c_content = content['commands']
            commands = []
            for c in c_content:
                device = self.find_device(c['id'])
                if None == device or not device.is_main:
                    continue
                commands += self.get_commands(device, c)
            control(commands)
            callback(callback_url, 1, "success")
        except Exception as ex:
            error_msg = "处理应用命令失败%s" % ex
            print(error_msg)
            callback(callback_url, 2, error_msg)

    def find_device(self, id):
        for device in devices:
            if id == device.id:
                return device
        return None

    def get_commands(self, device, c):
        if c['type'] == 'fan':
            return self.get_fan_commands(device, c)
        elif c['type'] == 'wind_door':
            return self.get_wind_door_commands(device, c)
        elif c['type'] == 'wind_window':
            return self.get_wind_window_commands(device, c)
        elif c['type'] == 'broadcast':
            return self.get_broadcast_commands(device, c)
        else:
            pass

    def get_broadcast_commands(self, device, c):
        a = device.get_attr(99)
        if a == None:
            return []
        return [Command(a.system_code, 4, a.code, str(c['value']))]

    def get_wind_door_commands(self, device, c):
        v = c['value']
        a = device.get_attr(11) if v == 0 else device.get_attr(13)
        return [Command(a.system_code, 3, a.code, '1')]

    def get_wind_window_commands(self, device, c):
        a = device.get_attr(10)
        return [Command(a.system_code, 3, a.code, str(c['value']))]

    def get_fan_commands(self, device, c):
        ans = []
        v = c['value']
        if c['attr'] == 'freq':
            a = device.get_attr(8)
            ans.append(Command(a.system_code, 3, a.code, str(v)))
        elif c['attr'] == 'is_anti_wind':
            # 暂停
            a1 = device.get_attr(2)
            ans.append(Command(a1.system_code, 3, a1.code, '1'))
            # 设置反风控制
            a2 = device.get_attr(4)
            ans.append(Command(a2.system_code, 3,
                       a2.code, '2' if v == 0 else '1'))
            # 启动
            a3 = device.get_attr(5)
            ans.append(Command(a3.system_code, 3, a3.code, '1'))
        elif c['attr'] == 'is_open':
            if v == 1:
                a = device.get_attr(5)
                ans.append(Command(a.system_code, 3, a.code, '1'))
            else:
                a = device.get_attr(2)
                ans.append(Command(a.system_code, 3, a.code, '1'))
        else:
            pass
        return ans

    def get_attr(self, device, attr_name):
        for a in device.attrs:
            if a.name == attr_name:
                return a
        return None

    def subscribe(self):
        def on_message(client, userdata, msg):
            topic = msg.topic
            content = json.loads(msg.payload.decode())
            if topic == point_real_time_topic:
                self.process_point_content(content)
            elif topic == position_real_time_topic:
                self.process_person_position(content)
            elif topic == device_command_topc:
                self.process_app_control(content)
        self.client.subscribe(point_real_time_topic)
        self.client.subscribe(position_real_time_topic)
        self.client.subscribe(device_command_topc)
        self.client.on_message = on_message

    def publish(self, topic, msg):
        self.client.publish(topic, payload=msg)
