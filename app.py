import json
from iot import Iot, devices
from datetime import datetime
import time


if __name__ == "__main__":
    iot = Iot()
    while True:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for device in devices:
            msg = device.to_message()
            if None != msg:
                iot.publish('vent/device/values', json.dumps(msg))
        time.sleep(1)
