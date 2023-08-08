import configparser
import os

import psycopg2

from commons import Device, DeviceAttr

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), '', 'env.ini'))


class Meta(object):

    pg_host = config['pg']['host']
    pg_port = int(config['pg']['port'])
    pg_database = config['pg']['database']
    pg_user = config['pg']['user']
    pg_password = config['pg']['password']
    lane_ids = None
    devices = None
    attrs = {}
    attr_device_dict = {}

    def __init__(self):
        self.lane_ids = self.get_lanes()
        self.devices = self.load_devices()

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
                device.attrs = self.get_device_attrs(device, 6, fan[6])
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
                device.attrs = self.get_device_attrs(device, 2, wind_window[5])
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
                if point[3] not in self.attrs:
                    self.attrs[point[3]] = DeviceAttr(
                        point[3], point[7], point[6])
                self.attr_device_dict[point[3]] = device
                ans.append(self.attrs[point[3]])
        cursor.close()
        return ans

    def get_recent_update_at(self):
        conn = self.connect_pg()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(updated_at) FROM tbl_element_bind_info;")
        res = cursor.fetchone()
        if None == res:
            return None
        else:
            return res[0]
