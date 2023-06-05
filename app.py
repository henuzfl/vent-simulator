from iot import Iot, devices
from datetime import datetime
import time


if __name__ == "__main__":
    iot = Iot()
    while True:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for device in devices:
            content = {
                "id": device.id,
                "type": device.type,
                "timestamp": ts
            }
            if len(device.attrs) < 1:
                continue
            for attr in device.attrs:
                if attr.value is None or attr.name.startswith('set'):
                    continue
                content[attr.name] = attr.value
            print(content)
        time.sleep(1)
