import datetime
import json
import paho.mqtt.publish as publish

for i in range(10):
    payload = json.dumps({
        "event_time": datetime.datetime.now().isoformat(),
        "value": i,
    })
    print("Publishing:", payload)
    publish.single(
        hostname="test.mosquitto.org",
        port=1883,
        topic="dev.kamu.example.mqtt.temp",
        payload=payload,
        retain=True,
        qos=1,
    )
