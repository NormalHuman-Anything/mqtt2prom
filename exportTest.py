#!/usr/bin/env python3
# subscriber_influxdb.py
import paho.mqtt.client as mqtt
from flask import Flask, render_template_string
import threading
import time
import json

# Flask app
app = Flask(__name__)

# Store latest MQTT message
latest_message = {"topic": "", "payload": ""}
mqtt_connected = {"status": False}

MQTT_BROKER = "*"
MQTT_PORT = *
MQTT_TOPIC = "dev/scooter/+/#"  # Change as needed
MQTT_USERNAME = "*"
MQTT_PASSWORD = "*"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        mqtt_connected["status"] = True
        client.subscribe(MQTT_TOPIC)
    else:
        print("Failed to connect, return code %d\n", rc)
        mqtt_connected["status"] = False

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker")
    mqtt_connected["status"] = False

def on_message(client, userdata, msg):
    latest_message["topic"] = msg.topic
    latest_message["payload"] = msg.payload.decode("utf-8")
    print(f"Received: {msg.topic} -> {latest_message['payload']}")

def mqtt_thread():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)  # Add this line
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(f"MQTT connection error: {e}")
            mqtt_connected["status"] = False
            time.sleep(2)

@app.route("/")
def index():
    return render_template_string("""
        {{ topic }}:{{ payload }}
        
    """, topic=latest_message["topic"], payload=latest_message["payload"], connected=mqtt_connected["status"])

@app.route("/metrics")
def metrics():
    try:
        data = json.loads(latest_message["payload"])
    except Exception:
        return "# Invalid JSON received", 200, {"Content-Type": "text/plain"}

    # Escape label values for Prometheus (e.g., quotes)
    def escape(val):
        return str(val).replace("\\", "\\\\").replace('"', '\\"')

    # Build label string dynamically
    label_parts = [f'{key}="{escape(value)}"' for key, value in data.items()]
    label_string = ",".join(label_parts)

    output = [
        "# HELP gnss GNSS data with dynamic labels",
        "# TYPE gnss gauge",
        f"gnss{{{label_string}}} 1"
    ]

    return "\n".join(output), 200, {"Content-Type": "text/plain"}


if __name__ == "__main__":
    t = threading.Thread(target=mqtt_thread)
    t.daemon = True
    t.start()
    app.run(host="localhost", port=5000, debug=True)