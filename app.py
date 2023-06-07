import json
import threading
from iot import Iot, devices, config
import time
from paho.mqtt import client as mqtt_client


def publish():
    client = mqtt_client.Client("p_0123")
    client.username_pw_set(
        username=config['mqtt']['username'], password=config['mqtt']['password'])
    client.connect(config['mqtt']['broker'], int(config['mqtt']['port']))
    while True:
        for device in devices:
            msg = device.to_message()
            if None != msg:
                client.publish('vent/device/values', payload=json.dumps(msg))
        time.sleep(1)


if __name__ == "__main__":
    t = threading.Thread(target=publish,  args=())
    t.start()
    iot = Iot()
