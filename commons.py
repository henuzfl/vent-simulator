from datetime import datetime

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