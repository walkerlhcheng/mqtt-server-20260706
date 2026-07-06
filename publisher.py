"""publisher.py - Railway-side MQTT publisher for reference testing (Subtask 2)
Publishes test messages to the local Mosquitto broker on topic 'railway/test'
Runs as a background thread that can be triggered, or loops continuously.
"""
import paho.mqtt.client as mqtt
import time
import os
from datetime import datetime

MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
TOPIC = "railway/test"
INTERVAL = int(os.environ.get("PUBLISH_INTERVAL", 5))  # seconds

def run_publisher():
    client = mqtt.Client(client_id="railway_publisher")

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            print(f"[PUBLISHER] Connected to broker at {MQTT_BROKER}:{MQTT_PORT}")
        else:
            print(f"[PUBLISHER] Connection failed rc={rc}")

    client.on_connect = on_connect

    # Wait for broker to be ready
    time.sleep(3)

    connected = False
    for attempt in range(10):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_start()
            connected = True
            print(f"[PUBLISHER] Connected successfully on attempt {attempt+1}")
            break
        except Exception as e:
            print(f"[PUBLISHER] Attempt {attempt+1} failed: {e}. Retrying in 2s...")
            time.sleep(2)

    if not connected:
        print("[PUBLISHER] Could not connect to broker after 10 attempts")
        return

    count = 0
    while True:
        count += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"Hello from Railway! Message #{count} at {timestamp}"
        result = client.publish(TOPIC, message, qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"[PUBLISHER] Sent: {TOPIC} -> {message}")
        else:
            print(f"[PUBLISHER] Failed to send message rc={result.rc}")
        time.sleep(INTERVAL)

if __name__ == '__main__':
    print("=== Railway MQTT Publisher Starting ===")
    run_publisher()
