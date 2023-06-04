from iot import Iot, Meta
from datetime import datetime
import time


if __name__ == "__main__":
    pass
    meta = Meta()
    devices = meta.load_devices()
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
                if attr.value is None:
                    continue
                content[attr.name] = attr.value
            print(content)
        time.sleep(1)
