
import configparser
import json
import os
import random
from datetime import datetime

from paho.mqtt import client as mqtt_client
from commons import Command

from mas import control
from meta import Meta
from vent import callback

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))

point_real_time_topic = 'mas.iot.realtimedata'
position_real_time_topic = 'mas.iot.PorealTime.simulator'
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
        self.meta = Meta()
        self.client.on_connect = on_connect
        self.client.connect(self.broker, self.port)
        self.client.loop_forever()

    def process_point_content(self, content):
        try:
            for tmp in content:
                if tmp["PointCode"] in self.meta.attrs:
                    self.meta.attrs[tmp["PointCode"]
                                    ].value = tmp["RealtimeValue"]
                    device = self.meta.attr_device_dict[tmp["PointCode"]]
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
        for device in self.meta.devices:
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

    last_update_at = None

    def subscribe(self):
        def on_message(client, userdata, msg):
            current_update_at = self.meta.get_recent_update_at()
            if self.last_update_at != None and self.last_update_at != current_update_at:
                self.meta = Meta()
            self.last_update_at = current_update_at
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
