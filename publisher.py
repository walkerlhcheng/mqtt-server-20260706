"""publisher.py - Railway-side MQTT publisher for reference testing (Subtask 2)
Publishes test messages to the local Mosquitto broker on topic 'railway/test'
Runs as a background thread that can be triggered, or loops continuously.
Includes auto-reconnect on connection loss.
"""
import paho.mqtt.client as mqtt
import time
import os
from datetime import datetime

MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1885))
TOPIC = "railway/test"
INTERVAL = int(os.environ.get("PUBLISH_INTERVAL", 5))  # seconds

_connected = False


def run_publisher():
    global _connected

    def on_connect(c, userdata, flags, rc):
        global _connected
        if rc == 0:
            _connected = True
            print(f"[PUBLISHER] Connected to broker at {MQTT_BROKER}:{MQTT_PORT}")
        else:
            _connected = False
            print(f"[PUBLISHER] Connection failed rc={rc}")

    def on_disconnect(c, userdata, rc):
        global _connected
        _connected = False
        print(f"[PUBLISHER] Disconnected rc={rc}, will reconnect...")

    client = mqtt.Client(client_id="railway_publisher")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Wait for broker to be ready
    time.sleep(3)

    count = 0
    while True:
        # Reconnect if needed
        if not _connected:
            print("[PUBLISHER] Attempting to connect...")
            for attempt in range(5):
                try:
                    client.connect(MQTT_BROKER, MQTT_PORT, 60)
                    client.loop_start()
                    time.sleep(2)
                    if _connected:
                        print(f"[PUBLISHER] Connected successfully on attempt {attempt+1}")
                        break
                except Exception as e:
                    print(f"[PUBLISHER] Attempt {attempt+1} failed: {e}")
                    time.sleep(3)
            if not _connected:
                print("[PUBLISHER] Could not connect, retrying in 10s...")
                time.sleep(10)
                continue

        count += 1
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = f"Hello from Railway! Message #{count} at {ts}"
        result = client.publish(TOPIC, payload, qos=1)
        if result.rc == 0:
            print(f"[PUBLISHER] Sent: {TOPIC} -> {payload}")
        else:
            print(f"[PUBLISHER] Failed to send message rc={result.rc}")
            _connected = False  # trigger reconnect
        time.sleep(INTERVAL)


if __name__ == "__main__":
    print("=== Railway MQTT Publisher (standalone) ===")
    run_publisher()
