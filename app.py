import json
import threading
import time

from paho.mqtt import client as mqtt_client

from iot import Iot, config
from meta import Meta

meta = Meta()


def publish():
    global meta
    client = mqtt_client.Client("p_01234567")
    client.username_pw_set(
        username=config['mqtt']['username'], password=config['mqtt']['password'])
    client.connect(config['mqtt']['broker'], int(config['mqtt']['port']))
    last_update_at = None
    while True:
        current_update_at = meta.get_recent_update_at()
        if last_update_at != None and last_update_at != current_update_at:
            meta = Meta()
        last_update_at = current_update_at
        for device in meta.devices:
            try:
                msg = device.to_message()
                if None != msg:
                    client.publish('vent/device/values',
                                   payload=json.dumps(msg))
            except Exception as ex:
                print("发送设备信息异常%s" % ex)
        if not client.is_connected:
            client.connect(config['mqtt']['broker'],
                           int(config['mqtt']['port']))
        time.sleep(1)


if __name__ == "__main__":
    t = threading.Thread(target=publish,  args=())
    t.start()
    iot = Iot()
